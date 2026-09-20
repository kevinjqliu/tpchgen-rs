//! Progress registration shared by the TPC-DS row-generator outputs.
//!
//! The DAT and CSV outputs both drive the row generators directly, so they
//! register progress the same way. Keeping that in one place stops the two
//! from drifting.

use crate::progress::{ProgressHandle, ProgressTracker};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tpcdsgen::config::{Session, Table};

/// Number of rows in a source row range, treating an empty chunk as zero.
fn range_len(range: RangeInclusive<u64>) -> u64 {
    if range.is_empty() {
        0
    } else {
        range.end() - range.start() + 1
    }
}

/// Register one progress bar for `table`.
///
/// `sessions` holds one session per requested part. Callers register once, then
/// split the result with [`share_handle_across_parts`].
///
/// Progress is counted in *source* rows (e.g. `store_sales` rather than
/// `store_returns`) rather than output rows, as only the source rows are known
/// exactly up front.
pub(super) fn register_table(
    table: Table,
    sessions: &[Session],
    progress: Arc<dyn ProgressTracker>,
) -> ProgressHandle {
    let requested_source_rows: u64 = sessions
        .iter()
        .map(|session| range_len(session.get_source_row_range(table)))
        .sum();
    progress.register(table.get_name(), requested_source_rows)
}

/// Split one registered handle into `num_parts` clones that all report to the
/// same bar.
///
/// Each part finishes independently and calls [`ProgressHandle::complete`] on
/// its own, so we need to ensure the bar only reaches its "done" style once
/// every part has completed.
pub(super) fn share_handle_across_parts(
    handle: ProgressHandle,
    num_parts: usize,
) -> Vec<ProgressHandle> {
    let remaining = Arc::new(AtomicUsize::new(num_parts.max(1)));
    (0..num_parts.max(1))
        .map(|_| {
            let remaining = remaining.clone();
            let increment_handle = handle.clone();
            let complete_handle = handle.clone();
            ProgressHandle::new_with_complete(
                move |units| increment_handle.increment(units),
                move || {
                    if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                        complete_handle.complete();
                    }
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::Mutex;
    use tpcdsgen::config::SessionBuilder;

    #[derive(Debug, Default)]
    struct RecordingProgress {
        registered: Mutex<Vec<(String, u64)>>,
    }

    impl ProgressTracker for RecordingProgress {
        fn register(self: Arc<Self>, item: &str, total_units: u64) -> ProgressHandle {
            self.registered
                .lock()
                .unwrap()
                .push((item.to_owned(), total_units));
            ProgressHandle::new(|_| {})
        }
    }

    /// A handle plus counters for what it observed, for asserting on
    /// [`share_handle_across_parts`]'s forwarding behavior.
    fn recording_handle() -> (ProgressHandle, Arc<AtomicU64>, Arc<AtomicUsize>) {
        let increments = Arc::new(AtomicU64::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let handle = {
            let increments = increments.clone();
            let completions = completions.clone();
            ProgressHandle::new_with_complete(
                move |units| {
                    increments.fetch_add(units, Ordering::Relaxed);
                },
                move || {
                    completions.fetch_add(1, Ordering::Relaxed);
                },
            )
        };
        (handle, increments, completions)
    }

    /// Sessions for every chunk of a `--parts total_chunks` run.
    fn all_parts(scale_factor: f64, total_chunks: i32) -> Vec<Session> {
        (1..=total_chunks)
            .map(|chunk_number| chunk(scale_factor, chunk_number, total_chunks))
            .collect()
    }

    /// A session for one `--parts total_chunks --part chunk_number` run.
    fn chunk(scale_factor: f64, chunk_number: i32, total_chunks: i32) -> Session {
        SessionBuilder::new()
            .with_scale_factor(scale_factor)
            .with_chunk_number(chunk_number)
            .with_total_chunks(total_chunks)
            .with_partitioned(true)
            .build()
            .unwrap()
    }

    fn whole(scale_factor: f64) -> Session {
        SessionBuilder::new()
            .with_scale_factor(scale_factor)
            .build()
            .unwrap()
    }

    #[test]
    fn register_table_total_covers_every_requested_part() {
        // `--parts 4` runs all four chunks in this process, so the bar is
        // sized to the whole table, exactly as an unpartitioned run is.
        let tracker = Arc::new(RecordingProgress::default());

        register_table(Table::StoreSales, &[whole(5.0)], tracker.clone());
        register_table(Table::StoreSales, &all_parts(5.0, 4), tracker.clone());

        let registered = tracker.registered.lock().unwrap();
        assert_eq!(registered[0], registered[1]);
    }

    #[test]
    fn register_table_total_is_sized_to_a_single_requested_part() {
        // `--parts 4 --part 2` generates only chunk 2, so a bar sized to the
        // whole table would top out at a quarter and then snap to "done".
        let tracker = Arc::new(RecordingProgress::default());
        let whole_session = whole(7.0);
        let one_of_four = chunk(7.0, 2, 4);
        let all_rows = range_len(whole_session.get_source_row_range(Table::CatalogSales));
        let part_rows = range_len(one_of_four.get_source_row_range(Table::CatalogSales));
        assert!(
            part_rows > 0 && part_rows < all_rows,
            "chunk must be a strict subset"
        );

        register_table(Table::CatalogSales, &[whole_session], tracker.clone());
        register_table(Table::CatalogSales, &[one_of_four], tracker.clone());

        let registered = tracker.registered.lock().unwrap();
        assert_eq!(registered[0], ("catalog_sales".to_owned(), all_rows));
        assert_eq!(registered[1], ("catalog_sales".to_owned(), part_rows));
    }

    /// A returns table progress is sized to its sales table's source rows
    #[test]
    fn a_returns_table_is_sized_to_its_sales_source_rows() {
        let tracker = Arc::new(RecordingProgress::default());
        let session = whole(1.0);

        register_table(
            Table::StoreSales,
            std::slice::from_ref(&session),
            tracker.clone(),
        );
        register_table(Table::StoreReturns, &[session], tracker.clone());

        let registered = tracker.registered.lock().unwrap();
        assert_eq!(registered[0].1, 240_000);
        assert_eq!(registered[1], ("store_returns".to_owned(), 240_000));
    }

    #[test]
    fn register_table_total_is_zero_for_an_empty_chunk() {
        // `reason` is far below the 1M threshold, so every chunk past the
        // first is empty and its bar has nothing to report.
        let tracker = Arc::new(RecordingProgress::default());

        register_table(Table::Reason, &[chunk(1.0, 2, 4)], tracker.clone());

        let registered = tracker.registered.lock().unwrap();
        assert_eq!(registered[0].1, 0);
    }

    #[test]
    fn share_handle_across_parts_forwards_every_increment() {
        let (handle, increments, _completions) = recording_handle();
        let parts = share_handle_across_parts(handle, 4);
        assert_eq!(parts.len(), 4);

        for part in &parts {
            part.increment(1);
        }
        assert_eq!(increments.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn share_handle_across_parts_completes_only_after_every_part_completes() {
        let (handle, _increments, completions) = recording_handle();
        let parts = share_handle_across_parts(handle, 3);

        parts[0].complete();
        parts[1].complete();
        assert_eq!(
            completions.load(Ordering::Relaxed),
            0,
            "must not finish the bar before the last part reports completion"
        );

        parts[2].complete();
        assert_eq!(completions.load(Ordering::Relaxed), 1);
    }
}
