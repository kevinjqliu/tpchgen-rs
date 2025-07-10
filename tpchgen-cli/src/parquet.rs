//! Parquet output format

use crate::statistics::WriteStatistics;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use log::debug;
use parquet::arrow::arrow_writer::{compute_leaves, get_column_writers, ArrowColumnChunk};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescPtr;
use std::io;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tpchgen_arrow::RecordBatchIterator;

pub trait IntoSize {
    /// Convert the object into a size
    fn into_size(self) -> Result<usize, io::Error>;
}

/// Converts a set of RecordBatchIterators into a Parquet file
///
/// Uses num_threads to generate the data in parallel
///
/// Note the input is an iterator of [`RecordBatchIterator`]; The batches
/// produced by each iterator is encoded as its own row group.
pub async fn generate_parquet<W: Write + Send + IntoSize + 'static, I>(
    writer: W,
    iter_iter: I,
    num_threads: usize,
    parquet_compression: Compression,
    row_group_size: usize,
) -> Result<(), io::Error>
where
    I: Iterator<Item: RecordBatchIterator> + 'static,
{
    debug!(
        "Generating Parquet with {num_threads} threads, using {parquet_compression} compression, {row_group_size} rows per group"
    );
    // Based on example in https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowColumnWriter.html
    let mut iter_iter = iter_iter.peekable();

    // get schema from the first iterator
    let Some(first_iter) = iter_iter.peek() else {
        return Ok(()); // no data shrug
    };
    let schema = Arc::clone(first_iter.schema());

    // Compute the parquet schema
    let writer_properties = WriterProperties::builder()
        .set_compression(parquet_compression)
        .build();
    let writer_properties = Arc::new(writer_properties);
    let parquet_schema = Arc::new(
        ArrowSchemaConverter::new()
            .with_coerce_types(writer_properties.coerce_types())
            .convert(&schema)
            .unwrap(),
    );

    // create a stream that computes the data for each row group
    let parquet_schema_for_stream = Arc::clone(&parquet_schema);
    let writer_properties_for_stream = Arc::clone(&writer_properties);
    let schema_for_stream = Arc::clone(&schema);
    let mut row_group_stream = futures::stream::iter(iter_iter)
        .map(move |iter| {
            let parquet_schema = Arc::clone(&parquet_schema_for_stream);
            let writer_properties = Arc::clone(&writer_properties_for_stream);
            let schema = Arc::clone(&schema_for_stream);
            let row_group_size = row_group_size; // capture the value
            // run on a separate thread
            async move {
                tokio::task::spawn(async move {
                    encode_row_group(parquet_schema, writer_properties, schema, iter, row_group_size)
                })
                .await
                .expect("Inner task panicked")
            }
        })
        .buffered(num_threads); // generate row groups in parallel

    let mut statistics = WriteStatistics::new("row groups");

    // A blocking task that writes the row groups to the file
    // done in a blocking task to avoid having a thread waiting on IO
    // Now, read each completed row group and write it to the file
    let root_schema = parquet_schema.root_schema_ptr();
    let writer_properties_captured = Arc::clone(&writer_properties);
    let (tx, mut rx): (
        Sender<Vec<Vec<ArrowColumnChunk>>>,
        Receiver<Vec<Vec<ArrowColumnChunk>>>,
    ) = tokio::sync::mpsc::channel(num_threads);
    let writer_task = tokio::task::spawn_blocking(move || {
        // Create parquet writer
        let mut writer =
            SerializedFileWriter::new(writer, root_schema, writer_properties_captured).unwrap();

        while let Some(row_group) = rx.blocking_recv() {
            for chunks in row_group {
                // Start row group
                let mut row_group_writer = writer.next_row_group().unwrap();

                // Slap the chunks into the row group
                for chunk in chunks {
                    chunk.append_to_row_group(&mut row_group_writer).unwrap();
                }
                row_group_writer.close().unwrap();
                statistics.increment_chunks(1);
            }
        }
        let size = writer.into_inner()?.into_size()?;
        statistics.increment_bytes(size);
        Ok(()) as Result<(), io::Error>
    });

    // now, drive the input stream and send results to the writer task
    while let Some(chunks) = row_group_stream.next().await {
        // send the chunks to the writer task
        if let Err(e) = tx.send(chunks).await {
            debug!("Error sending chunks to writer: {e}");
            break; // stop early
        }
    }
    // signal the writer task that we are done
    drop(tx);

    // Wait for the writer task to finish
    writer_task.await??;

    Ok(())
}

/// Creates the data for some number of row groups
///
/// Note at the moment it does not use multiple tasks/threads but it could
/// potentially encode multiple columns with different threads .
///
/// Returns an array of [`ArrowColumnChunk`] for each row group
fn encode_row_group<I>(
    parquet_schema: SchemaDescPtr,
    writer_properties: Arc<WriterProperties>,
    schema: SchemaRef,
    iter: I,
    row_group_size: usize,
) -> Vec<Vec<ArrowColumnChunk>>
where
    I: RecordBatchIterator,
{
    let mut finished_row_groups = Vec::new();

    let mut iter = iter.peekable();
    loop {
        // No more input
        if iter.peek().is_none() {
            break;
        }
        // Create writers for each of the leaf columns
        let mut col_writers = get_column_writers(&parquet_schema, &writer_properties, &schema).unwrap();

        // otherwise generate a row group with up to row_group_size rows
        let mut num_rows = 0;
        while let Some(batch) = iter.next() {
            // encode the columns in the batch
            let columns = batch.columns().iter();
            let col_writers = col_writers.iter_mut();
            let fields = schema.fields().iter();

            for ((col_writer, field), arr) in col_writers.zip(fields).zip(columns) {
                for leaves in compute_leaves(field.as_ref(), arr).unwrap() {
                    col_writer.write(&leaves).unwrap();
                }
            }

            num_rows += batch.num_rows();
            if num_rows >= row_group_size {
                break; // we have enough rows for this row group
            }
        }
        // finish the writers and create the column chunks for the row group
        let row_group = col_writers
            .into_iter()
            .map(|col_writer| col_writer.close().unwrap())
            .collect();
        finished_row_groups.push(row_group);
        // loop back for the next
    }
    finished_row_groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::basic::Compression;
    use std::io::Cursor;
    use std::sync::Arc;
    use tpchgen_arrow::RecordBatchIterator;

    /// A simple test RecordBatchIterator that generates a specified number of batches
    /// with a specified number of rows per batch
    struct TestIterator {
        schema: Arc<Schema>,
        num_batches: usize,
        rows_per_batch: usize,
        current_batch: usize,
    }

    impl TestIterator {
        fn new(num_batches: usize, rows_per_batch: usize) -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]));

            Self {
                schema,
                num_batches,
                rows_per_batch,
                current_batch: 0,
            }
        }
    }

    impl RecordBatchIterator for TestIterator {
        fn schema(&self) -> &Arc<Schema> {
            &self.schema
        }
    }

    impl Iterator for TestIterator {
        type Item = RecordBatch;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current_batch >= self.num_batches {
                return None;
            }

            let start_id = self.current_batch * self.rows_per_batch;
            let ids: Vec<i32> = (start_id..start_id + self.rows_per_batch)
                .map(|i| i as i32)
                .collect();
            let names: Vec<String> = (start_id..start_id + self.rows_per_batch)
                .map(|i| format!("name_{}", i))
                .collect();

            let id_array = Int32Array::from(ids);
            let name_array = StringArray::from(names);

            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            self.current_batch += 1;
            Some(batch)
        }
    }

    impl IntoSize for Cursor<Vec<u8>> {
        fn into_size(self) -> Result<usize, io::Error> {
            Ok(self.into_inner().len())
        }
    }

    #[tokio::test]
    async fn test_generate_parquet_with_different_row_group_sizes() {
        // Test with multiple row group sizes to ensure the parameter is used correctly
        let test_cases = vec![
            (10, "small row groups"),
            (100, "medium row groups"),
            (1000, "large row groups"),
        ];

        for (row_group_size, description) in test_cases {
            let writer = Cursor::new(Vec::new());
            let test_iter = TestIterator::new(5, 50); // 5 batches of 50 rows = 250 total rows
            let iterators = vec![test_iter];

            let result = generate_parquet(
                writer,
                iterators.into_iter(),
                1, // single thread for deterministic testing
                Compression::UNCOMPRESSED,
                row_group_size,
            )
            .await;

            assert!(
                result.is_ok(),
                "Failed to generate parquet with {}: {:?}",
                description,
                result.err()
            );
        }
    }

    #[tokio::test]
    async fn test_generate_parquet_empty_iterator() {
        // Test edge case: empty iterator
        let writer = Cursor::new(Vec::new());
        let iterators: Vec<TestIterator> = vec![];

        let result = generate_parquet(
            writer,
            iterators.into_iter(),
            1,
            Compression::UNCOMPRESSED,
            1000,
        )
        .await;

        assert!(result.is_ok(), "Failed to handle empty iterator");
    }

    #[tokio::test]
    async fn test_generate_parquet_single_row() {
        // Test edge case: single row
        let writer = Cursor::new(Vec::new());
        let test_iter = TestIterator::new(1, 1); // 1 batch of 1 row
        let iterators = vec![test_iter];

        let result = generate_parquet(
            writer,
            iterators.into_iter(),
            1,
            Compression::UNCOMPRESSED,
            1000,
        )
        .await;

        assert!(result.is_ok(), "Failed to generate parquet with single row");
    }

    #[tokio::test]
    async fn test_generate_parquet_very_small_row_groups() {
        // Test with row group size of 1 (extreme case)
        let writer = Cursor::new(Vec::new());
        let test_iter = TestIterator::new(2, 5); // 2 batches of 5 rows = 10 total rows
        let iterators = vec![test_iter];

        let result = generate_parquet(
            writer,
            iterators.into_iter(),
            1,
            Compression::UNCOMPRESSED,
            1, // 1 row per group
        )
        .await;

        assert!(
            result.is_ok(),
            "Failed to generate parquet with very small row groups"
        );
    }

    #[tokio::test]
    async fn test_encode_row_group_respects_size_limit() {
        // Test that encode_row_group actually respects the row group size
        let test_iter = TestIterator::new(3, 20); // 3 batches of 20 rows = 60 total rows
        let schema = Arc::clone(test_iter.schema());

        let writer_properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .build(),
        );

        let parquet_schema = Arc::new(
            ArrowSchemaConverter::new()
                .with_coerce_types(writer_properties.coerce_types())
                .convert(&schema)
                .unwrap(),
        );

        // Test with row group size of 25 - should create 3 row groups:
        // Group 1: 20 + 5 = 25 rows
        // Group 2: 15 + 10 = 25 rows  
        // Group 3: 20 rows
        let row_groups = encode_row_group(
            parquet_schema,
            writer_properties,
            schema,
            test_iter,
            25, // row group size
        );

        // We should get multiple row groups when total rows exceed row group size
        assert!(
            row_groups.len() >= 2,
            "Expected multiple row groups, got {}",
            row_groups.len()
        );
    }

    #[tokio::test]
    async fn test_different_compression_formats() {
        // Test that different compression formats work with configurable row groups
        let compression_types = vec![
            Compression::UNCOMPRESSED,
            Compression::SNAPPY,
            Compression::GZIP(Default::default()),
            Compression::LZ4,
        ];

        for compression in compression_types {
            let writer = Cursor::new(Vec::new());
            let test_iter = TestIterator::new(2, 10); // 2 batches of 10 rows
            let iterators = vec![test_iter];

            let result = generate_parquet(
                writer,
                iterators.into_iter(),
                1,
                compression,
                15, // row group size
            )
            .await;

            assert!(
                result.is_ok(),
                "Failed to generate parquet with compression {:?}: {:?}",
                compression,
                result.err()
            );
        }
    }

    #[tokio::test]
    async fn test_multiple_threads_with_row_groups() {
        // Test that multiple threads work correctly with configurable row groups
        let writer = Cursor::new(Vec::new());
        let iterators: Vec<TestIterator> = (0..4)
            .map(|_| TestIterator::new(2, 25)) // 4 iterators, each with 2 batches of 25 rows
            .collect();

        let result = generate_parquet(
            writer,
            iterators.into_iter(),
            4, // multiple threads
            Compression::UNCOMPRESSED,
            30, // row group size
        )
        .await;

        assert!(
            result.is_ok(),
            "Failed to generate parquet with multiple threads"
        );
    }

    #[test]
    fn test_encode_row_group_exact_size_boundary() {
        // Test boundary condition where total rows exactly equals row group size
        let test_iter = TestIterator::new(2, 15); // 2 batches of 15 rows = 30 total rows
        let schema = Arc::clone(test_iter.schema());

        let writer_properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .build(),
        );

        let parquet_schema = Arc::new(
            ArrowSchemaConverter::new()
                .with_coerce_types(writer_properties.coerce_types())
                .convert(&schema)
                .unwrap(),
        );

        let row_groups = encode_row_group(
            parquet_schema,
            writer_properties,
            schema,
            test_iter,
            30, // exactly matches total rows
        );

        // Should create exactly 1 row group
        assert_eq!(
            row_groups.len(),
            1,
            "Expected exactly 1 row group, got {}",
            row_groups.len()
        );
    }
}
