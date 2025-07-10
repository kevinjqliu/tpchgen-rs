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
