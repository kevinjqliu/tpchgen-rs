//! Progress tracking for table generation.
//!
//! # Overview
//!
//! [`ProgressTracker`] is a small, dyn-compatible trait that receives
//! generation events from [`crate::runner::PlanRunner`]. The runner calls:
//!
//! 1. [`ProgressTracker::register`] once per table, before any worker
//!    starts, with the total number of chunks the table will produce.
//! 2. [`ProgressTracker::increment`] from each worker as chunks are
//!    written. This is the hot path: multiple worker threads call it
//!    concurrently, so impls must be `Send + Sync` and `increment`
//!    itself should be non-blocking.
//! 3. [`ProgressTracker::finish`] once on the success path when the
//!    runner exits. Implementations needing cleanup on the error or
//!    panic path should put it in their `Drop` impl.
//!
//! `register` and `finish` are invoked serially by the runner and may
//! do bookkeeping or I/O; only `increment` is on the worker hot path.
//!
//! Implementations must not panic and must not propagate I/O errors —
//! progress reporting is best-effort and must never affect the data
//! path.
//!
//! # Default implementation
//!
//! When the `indicatif-progress` feature is enabled (on by default), the
//! crate provides [`IndicatifProgress`], which renders one progress bar
//! per table on stderr using the [`indicatif`] crate. Library users who
//! do not want to pull in `indicatif` can disable the default features
//! and supply their own [`ProgressTracker`] implementation.
//!
//! # Example: a custom logging tracker
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use tpchgen_cli::progress::ProgressTracker;
//! use tpchgen_cli::Table;
//!
//! #[derive(Debug)]
//! struct LoggingTracker {
//!     written: AtomicU64,
//! }
//!
//! impl ProgressTracker for LoggingTracker {
//!     fn register(&self, table: Table, total: u64) {
//!         eprintln!("plan: {table:?} -> {total} chunks");
//!     }
//!     fn increment(&self, _table: Table, chunks: u64) {
//!         self.written.fetch_add(chunks, Ordering::Relaxed);
//!     }
//!     fn finish(&self) {
//!         eprintln!("done: {} chunks", self.written.load(Ordering::Relaxed));
//!     }
//! }
//! ```

use crate::Table;
use std::fmt;

/// Receives generation-progress events for one
/// [`PlanRunner`](crate::runner::PlanRunner) invocation.
///
/// See the [module-level documentation](self) for the call-order
/// contract. Implementations are wrapped in an [`std::sync::Arc`] by the
/// runner and shared across worker tasks, so they must be `Send + Sync`.
/// They must also be `Debug` so containing types can derive `Debug`.
pub trait ProgressTracker: Send + Sync + fmt::Debug {
    /// Pre-register a table with its total expected chunk count.
    ///
    /// Called once per table before any worker starts. Implementations
    /// that need to know totals up front (e.g. to render a progress bar
    /// or compute an ETA) should override this; the default does
    /// nothing.
    fn register(&self, _table: Table, _total_chunks: u64) {}

    /// Advance the counter for `table` by `chunks` units.
    ///
    /// Called once per generated chunk on the worker hot path from
    /// multiple threads concurrently. Must be non-blocking and must
    /// never panic.
    fn increment(&self, table: Table, chunks: u64);

    /// Called once after the last [`Self::increment`] on the success
    /// path. Implementations needing cleanup on the error or panic path
    /// should put it in their `Drop` impl. The default does nothing.
    fn finish(&self) {}
}

#[cfg(feature = "indicatif-progress")]
pub use indicatif_impl::IndicatifProgress;

#[cfg(feature = "indicatif-progress")]
mod indicatif_impl {
    use super::ProgressTracker;
    use crate::Table;
    use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};
    use std::collections::BTreeMap;
    use std::sync::{OnceLock, RwLock};

    /// Default [`ProgressTracker`] implementation backed by
    /// [`indicatif::MultiProgress`].
    ///
    /// Renders one bar per table on stderr. Bars are pre-allocated in
    /// [`ProgressTracker::register`] and are looked up by [`Table`] on
    /// each [`ProgressTracker::increment`] call. Lookup uses a `RwLock`
    /// read on the hot path; this is uncontended after the serial
    /// `register` phase completes.
    #[derive(Debug)]
    pub struct IndicatifProgress {
        multi: MultiProgress,
        tables: RwLock<BTreeMap<Table, ProgressBar>>,
    }

    impl IndicatifProgress {
        /// Construct an empty tracker. Tables are added via
        /// [`ProgressTracker::register`].
        pub fn new() -> Self {
            Self {
                multi: MultiProgress::new(),
                tables: RwLock::new(BTreeMap::new()),
            }
        }
    }

    impl Default for IndicatifProgress {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ProgressTracker for IndicatifProgress {
        fn register(&self, table: Table, total_chunks: u64) {
            let pb = self.multi.add(ProgressBar::new(total_chunks));
            pb.set_style(bar_style().clone());
            pb.set_message(table.to_string());
            let pb = pb.with_finish(ProgressFinish::AndLeave);
            // Write-lock is only contended during the register phase, which
            // happens serially before any worker task starts.
            self.tables
                .write()
                .expect("progress tables RwLock poisoned")
                .insert(table, pb);
        }

        fn increment(&self, table: Table, chunks: u64) {
            // Minimize the read-lock scope so concurrent `increment` callers
            // don't serialize on it. Cloning the bar is a cheap `Arc` bump,
            // and `ProgressBar::inc` is internally thread-safe.
            let bar = {
                let tables = self.tables.read().expect("progress tables RwLock poisoned");
                tables.get(&table).cloned()
            };
            if let Some(bar) = bar {
                bar.inc(chunks);
            }
        }

        // `finish` falls through to the trait default no-op: bars are
        // registered with `ProgressFinish::AndLeave`, so each one is
        // finalized when the `tables` map is dropped along with `self`.
    }

    fn bar_style() -> &'static ProgressStyle {
        static STYLE: OnceLock<ProgressStyle> = OnceLock::new();
        STYLE.get_or_init(|| {
            ProgressStyle::default_bar()
                .template("{msg:10} [{bar:28}]   Progress: {percent:>3}%")
                .expect("static progress bar template is valid")
                .progress_chars("█▓░")
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_and_increments() {
            let t = IndicatifProgress::new();
            t.register(Table::Lineitem, 60);
            t.register(Table::Orders, 15);
            t.increment(Table::Lineitem, 1);
            t.increment(Table::Orders, 5);

            let tables = t.tables.read().unwrap();
            assert_eq!(tables[&Table::Lineitem].position(), 1);
            assert_eq!(tables[&Table::Orders].position(), 5);
        }

        #[test]
        fn reaches_total() {
            let t = IndicatifProgress::new();
            t.register(Table::Orders, 5);
            for _ in 0..5 {
                t.increment(Table::Orders, 1);
            }
            assert_eq!(t.tables.read().unwrap()[&Table::Orders].position(), 5);
        }

        #[test]
        fn unknown_table_is_no_op() {
            // Incrementing a table not registered must not panic.
            let t = IndicatifProgress::new();
            t.register(Table::Orders, 1);
            t.increment(Table::Lineitem, 1);
            assert_eq!(t.tables.read().unwrap()[&Table::Orders].position(), 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Table;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    };

    /// Mock implementation that records every event. Demonstrates that
    /// the trait is dyn-compatible and usable from external code without
    /// pulling in `indicatif`.
    #[derive(Debug, Default)]
    struct MockTracker {
        registered: Mutex<Vec<(Table, u64)>>,
        total_increments: AtomicU64,
        finished: AtomicU64,
    }

    impl ProgressTracker for MockTracker {
        fn register(&self, table: Table, total_chunks: u64) {
            self.registered.lock().unwrap().push((table, total_chunks));
        }
        fn increment(&self, _table: Table, chunks: u64) {
            self.total_increments.fetch_add(chunks, Ordering::Relaxed);
        }
        fn finish(&self) {
            self.finished.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn mock_tracker_works_through_arc_dyn() {
        let mock = Arc::new(MockTracker::default());
        let dynamic: Arc<dyn ProgressTracker> = mock.clone();
        dynamic.register(Table::Lineitem, 10);
        dynamic.register(Table::Orders, 4);
        dynamic.increment(Table::Lineitem, 3);
        dynamic.increment(Table::Orders, 1);
        dynamic.finish();

        assert_eq!(
            *mock.registered.lock().unwrap(),
            vec![(Table::Lineitem, 10), (Table::Orders, 4)]
        );
        assert_eq!(mock.total_increments.load(Ordering::Relaxed), 4);
        assert_eq!(mock.finished.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn default_register_and_finish_are_noops() {
        // An impl that only overrides `increment` should compile and run.
        #[derive(Debug)]
        struct Minimal(AtomicU64);
        impl ProgressTracker for Minimal {
            fn increment(&self, _t: Table, c: u64) {
                self.0.fetch_add(c, Ordering::Relaxed);
            }
        }
        let m = Minimal(AtomicU64::new(0));
        m.register(Table::Region, 99); // no-op default
        m.increment(Table::Region, 7);
        m.finish(); // no-op default
        assert_eq!(m.0.load(Ordering::Relaxed), 7);
    }
}
