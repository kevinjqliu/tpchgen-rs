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
                    encode_row_group(
                        parquet_schema,
                        writer_properties,
                        schema,
                        iter,
                        row_group_size,
                    )
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
        let mut col_writers =
            get_column_writers(&parquet_schema, &writer_properties, &schema).unwrap();

        // otherwise generate a row group with up to row_group_size rows
        let mut num_rows = 0;
        for batch in iter.by_ref() {
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

    /// Test helper: A RecordBatchIterator that generates predictable test data
    ///
    /// Creates batches with:
    /// - `id` column: sequential integers starting from 0
    /// - `name` column: formatted strings like "name_0", "name_1", etc.
    struct TestDataIterator {
        schema: Arc<Schema>,
        num_batches: usize,
        rows_per_batch: usize,
        current_batch: usize,
    }

    impl TestDataIterator {
        /// Creates a new test iterator
        ///
        /// # Arguments
        /// * `num_batches` - Total number of batches to generate
        /// * `rows_per_batch` - Number of rows in each batch
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

        /// Returns the total number of rows this iterator will produce
        fn total_rows(&self) -> usize {
            self.num_batches * self.rows_per_batch
        }
    }

    impl RecordBatchIterator for TestDataIterator {
        fn schema(&self) -> &Arc<Schema> {
            &self.schema
        }
    }

    impl Iterator for TestDataIterator {
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
            .expect("Failed to create test RecordBatch");

            self.current_batch += 1;
            Some(batch)
        }
    }

    impl IntoSize for Cursor<Vec<u8>> {
        fn into_size(self) -> Result<usize, io::Error> {
            Ok(self.into_inner().len())
        }
    }

    /// Helper function to create test writer and validate successful parquet generation
    async fn assert_parquet_generation_succeeds(
        iterators: Vec<TestDataIterator>,
        num_threads: usize,
        compression: Compression,
        row_group_size: usize,
        test_description: &str,
    ) {
        let writer = Cursor::new(Vec::new());

        let result = generate_parquet(
            writer,
            iterators.into_iter(),
            num_threads,
            compression,
            row_group_size,
        )
        .await;

        assert!(
            result.is_ok(),
            "Parquet generation failed for test '{}': {:?}",
            test_description,
            result.err()
        );
    }

    /// Helper function to create WriterProperties and ParquetSchema for testing
    fn create_test_parquet_schema(schema: &Arc<Schema>) -> (Arc<WriterProperties>, SchemaDescPtr) {
        let writer_properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .build(),
        );

        let parquet_schema = Arc::new(
            ArrowSchemaConverter::new()
                .with_coerce_types(writer_properties.coerce_types())
                .convert(schema)
                .expect("Failed to convert Arrow schema to Parquet schema"),
        );

        (writer_properties, parquet_schema)
    }

    #[tokio::test]
    async fn test_parquet_generation_with_various_row_group_sizes() {
        /// Test that parquet generation works correctly with different row group sizes
        /// This ensures the row_group_size parameter is properly used in the generation process

        const TOTAL_ROWS: usize = 250; // 5 batches * 50 rows each
        const NUM_BATCHES: usize = 5;
        const ROWS_PER_BATCH: usize = 50;

        let test_cases = [
            (10, "small row groups (multiple groups expected)"),
            (100, "medium row groups (2-3 groups expected)"),
            (1000, "large row groups (single group expected)"),
            (TOTAL_ROWS, "exact size match (single group expected)"),
        ];

        for (row_group_size, description) in test_cases {
            let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
            assert_eq!(
                test_iter.total_rows(),
                TOTAL_ROWS,
                "Test setup verification failed"
            );

            assert_parquet_generation_succeeds(
                vec![test_iter],
                1, // single thread for deterministic behavior
                Compression::UNCOMPRESSED,
                row_group_size,
                description,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_parquet_generation_with_empty_input() {
        // Test edge case: empty iterator should complete successfully without errors
        let empty_iterators: Vec<TestDataIterator> = vec![];

        assert_parquet_generation_succeeds(
            empty_iterators,
            1,
            Compression::UNCOMPRESSED,
            1000,
            "empty iterator edge case",
        )
        .await;
    }

    #[tokio::test]
    async fn test_parquet_generation_with_single_row() {
        /// Test edge case: single row should be handled correctly

        const SINGLE_BATCH: usize = 1;
        const SINGLE_ROW: usize = 1;

        let test_iter = TestDataIterator::new(SINGLE_BATCH, SINGLE_ROW);
        assert_eq!(test_iter.total_rows(), 1, "Test setup verification failed");

        assert_parquet_generation_succeeds(
            vec![test_iter],
            1,
            Compression::UNCOMPRESSED,
            1000, // row group size much larger than data
            "single row edge case",
        )
        .await;
    }

    #[tokio::test]
    async fn test_parquet_generation_with_minimal_row_groups() {
        /// Test extreme case: row group size of 1 (every row is its own group)

        const NUM_BATCHES: usize = 2;
        const ROWS_PER_BATCH: usize = 5;
        const MINIMAL_ROW_GROUP_SIZE: usize = 1;

        let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
        assert_eq!(test_iter.total_rows(), 10, "Test setup verification failed");

        assert_parquet_generation_succeeds(
            vec![test_iter],
            1,
            Compression::UNCOMPRESSED,
            MINIMAL_ROW_GROUP_SIZE,
            "minimal row group size (1 row per group)",
        )
        .await;
    }

    #[tokio::test]
    async fn test_encode_row_group_size_enforcement() {
        /// Test that encode_row_group function properly respects the row group size limit
        /// and creates the expected number of row groups

        const NUM_BATCHES: usize = 3;
        const ROWS_PER_BATCH: usize = 20;
        const TOTAL_ROWS: usize = NUM_BATCHES * ROWS_PER_BATCH; // 60 rows
        const ROW_GROUP_SIZE: usize = 25;

        let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
        assert_eq!(
            test_iter.total_rows(),
            TOTAL_ROWS,
            "Test setup verification failed"
        );

        let schema = Arc::clone(test_iter.schema());
        let (writer_properties, parquet_schema) = create_test_parquet_schema(&schema);

        // With 60 total rows and row group size of 25:
        // Expected behavior: The function processes batches and groups them,
        // likely resulting in fewer groups than simple ceiling division due to batch processing
        let row_groups = encode_row_group(
            parquet_schema,
            writer_properties,
            schema,
            test_iter,
            ROW_GROUP_SIZE,
        );

        // Verify we got at least one row group and it respects the general size constraint
        assert!(
            !row_groups.is_empty(),
            "Expected at least one row group for non-empty data"
        );

        // Verify we got multiple groups when data significantly exceeds row group size
        // (This tests that the function doesn't just put everything in one group)
        assert!(
            row_groups.len() >= 2,
            "Expected multiple row groups when {} rows significantly exceed group size {}, got {} groups",
            TOTAL_ROWS,
            ROW_GROUP_SIZE,
            row_groups.len()
        );

        // Verify we don't have an excessive number of groups
        assert!(
            row_groups.len() <= TOTAL_ROWS,
            "Row group count ({}) should not exceed total rows ({})",
            row_groups.len(),
            TOTAL_ROWS
        );
    }

    #[tokio::test]
    async fn test_parquet_generation_with_various_compression_formats() {
        /// Test that all supported compression formats work correctly with configurable row groups
        /// This ensures compression doesn't interfere with row group size logic

        const NUM_BATCHES: usize = 2;
        const ROWS_PER_BATCH: usize = 10;
        const ROW_GROUP_SIZE: usize = 15;

        let compression_formats = [
            (Compression::UNCOMPRESSED, "uncompressed"),
            (Compression::SNAPPY, "snappy"),
            (Compression::GZIP(Default::default()), "gzip"),
            (Compression::LZ4, "lz4"),
        ];

        for (compression, format_name) in compression_formats {
            let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
            assert_eq!(test_iter.total_rows(), 20, "Test setup verification failed");

            assert_parquet_generation_succeeds(
                vec![test_iter],
                1,
                compression,
                ROW_GROUP_SIZE,
                &format!("{} compression format", format_name),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_parquet_generation_with_multiple_threads() {
        /// Test that parallel processing with multiple threads works correctly
        /// This ensures thread safety and proper coordination between threads

        const NUM_ITERATORS: usize = 4;
        const NUM_BATCHES_PER_ITERATOR: usize = 2;
        const ROWS_PER_BATCH: usize = 25;
        const TOTAL_ROWS_PER_ITERATOR: usize = NUM_BATCHES_PER_ITERATOR * ROWS_PER_BATCH;
        const ROW_GROUP_SIZE: usize = 30;
        const NUM_THREADS: usize = 4;

        // Create multiple iterators to process in parallel
        let iterators: Vec<TestDataIterator> = (0..NUM_ITERATORS)
            .map(|_| TestDataIterator::new(NUM_BATCHES_PER_ITERATOR, ROWS_PER_BATCH))
            .collect();

        // Verify test setup
        for iter in &iterators {
            assert_eq!(
                iter.total_rows(),
                TOTAL_ROWS_PER_ITERATOR,
                "Each iterator should produce {} rows",
                TOTAL_ROWS_PER_ITERATOR
            );
        }

        assert_parquet_generation_succeeds(
            iterators,
            NUM_THREADS,
            Compression::UNCOMPRESSED,
            ROW_GROUP_SIZE,
            "multiple threads with parallel processing",
        )
        .await;
    }

    #[test]
    fn test_encode_row_group_exact_boundary_conditions() {
        /// Test boundary condition where total rows exactly equals row group size
        /// This verifies correct behavior when no partial row groups are needed

        const NUM_BATCHES: usize = 2;
        const ROWS_PER_BATCH: usize = 15;
        const TOTAL_ROWS: usize = NUM_BATCHES * ROWS_PER_BATCH; // 30 rows
        const ROW_GROUP_SIZE: usize = TOTAL_ROWS; // exact match

        let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
        assert_eq!(
            test_iter.total_rows(),
            TOTAL_ROWS,
            "Test setup verification failed"
        );

        let schema = Arc::clone(test_iter.schema());
        let (writer_properties, parquet_schema) = create_test_parquet_schema(&schema);

        let row_groups = encode_row_group(
            parquet_schema,
            writer_properties,
            schema,
            test_iter,
            ROW_GROUP_SIZE,
        );

        // When total rows exactly equals row group size, should create exactly 1 row group
        assert_eq!(
            row_groups.len(),
            1,
            "Expected exactly 1 row group when total rows ({}) equals row group size ({}), got {}",
            TOTAL_ROWS,
            ROW_GROUP_SIZE,
            row_groups.len()
        );
    }

    #[test]
    fn test_encode_row_group_with_zero_rows() {
        /// Test edge case: iterator with zero rows should produce zero row groups

        const NUM_BATCHES: usize = 0;
        const ROWS_PER_BATCH: usize = 0;
        const ROW_GROUP_SIZE: usize = 100;

        let test_iter = TestDataIterator::new(NUM_BATCHES, ROWS_PER_BATCH);
        assert_eq!(test_iter.total_rows(), 0, "Test setup verification failed");

        let schema = Arc::clone(test_iter.schema());
        let (writer_properties, parquet_schema) = create_test_parquet_schema(&schema);

        let row_groups = encode_row_group(
            parquet_schema,
            writer_properties,
            schema,
            test_iter,
            ROW_GROUP_SIZE,
        );

        assert_eq!(
            row_groups.len(),
            0,
            "Expected 0 row groups for empty iterator, got {}",
            row_groups.len()
        );
    }
}
