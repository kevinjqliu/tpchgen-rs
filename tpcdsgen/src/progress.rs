//! Progress tracking for TPC-DS data generation.

use crate::config::Table;
#[cfg(feature = "progress")]
use std::fmt;
#[cfg(feature = "progress")]
use std::sync::Arc;

/// Receives generation-progress events for one DAT generation invocation.
///
/// See the [module-level documentation](self) for the call-order contract.
/// Trackers are passed as an [`std::sync::Arc`] so they must be `Send + Sync`.
/// They must also be `Debug` so containing types can derive `Debug`.
#[cfg(feature = "progress")]
pub trait ProgressTracker: Send + Sync + fmt::Debug {
    /// Pre-register a table with its total expected output-unit count.
    ///
    /// Called once per table before generation starts. Implementations that
    /// need to know totals up front, such as progress bars or ETA trackers,
    /// should override this; the default does nothing.
    fn register(&self, _table: Table, _total_units: u64) {}

    /// Advance the counter for `table` by `units` output units.
    ///
    /// Called after generated output units are written.
    fn increment(&self, table: Table, units: u64);

    /// Called once after the last [`Self::increment`] on the success path.
    fn finish(&self) {}
}

/// Progress handle for one DAT generation invocation.
///
/// Owns run-level progress lifecycle: registering totals and finishing the
/// tracker.
#[derive(Debug, Clone, Default)]
#[cfg(feature = "progress")]
pub(crate) struct RunProgress {
    tracker: Option<Arc<dyn ProgressTracker>>,
}

#[cfg(feature = "progress")]
impl RunProgress {
    pub(crate) fn with_tracker(tracker: Arc<dyn ProgressTracker>) -> Self {
        Self {
            tracker: Some(tracker),
        }
    }

    pub(crate) fn register_totals(&self, totals: &[(Table, u64)]) {
        if let Some(tracker) = self.tracker.as_ref() {
            for (table, total_units) in totals {
                tracker.register(*table, *total_units);
            }
        }
    }

    pub(crate) fn for_table(&self, table: Table) -> TableProgress {
        TableProgress::for_table(self.tracker.clone(), table)
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.tracker.is_some()
    }

    pub(crate) fn finish(self) {
        if let Some(tracker) = self.tracker {
            tracker.finish();
        }
    }
}

/// No-op run progress handle used when progress support is disabled.
#[derive(Debug, Clone, Default)]
#[cfg(not(feature = "progress"))]
pub(crate) struct RunProgress;

#[cfg(not(feature = "progress"))]
impl RunProgress {
    pub(crate) fn register_totals(&self, _totals: &[(Table, u64)]) {}

    pub(crate) fn for_table(&self, _table: Table) -> TableProgress {
        TableProgress
    }

    pub(crate) fn is_enabled(&self) -> bool {
        false
    }

    pub(crate) fn finish(self) {}
}

/// Progress handle for one table output stream.
///
/// Used by DAT output loops to report each successfully written output unit
/// without knowing whether progress tracking is enabled.
#[derive(Clone, Default)]
#[cfg(feature = "progress")]
pub(crate) struct TableProgress {
    tracker: Option<(Arc<dyn ProgressTracker>, Table)>,
}

#[cfg(feature = "progress")]
impl TableProgress {
    pub(crate) fn for_table(progress: Option<Arc<dyn ProgressTracker>>, table: Table) -> Self {
        Self {
            tracker: progress.map(|progress| (progress, table)),
        }
    }

    pub(crate) fn increment_output_unit(&self) {
        if let Some((progress, table)) = self.tracker.as_ref() {
            progress.increment(*table, 1);
        }
    }
}

/// No-op table progress handle used when progress support is disabled.
#[derive(Clone, Default)]
#[cfg(not(feature = "progress"))]
pub(crate) struct TableProgress;

#[cfg(not(feature = "progress"))]
impl TableProgress {
    pub(crate) fn increment_output_unit(&self) {}
}

#[cfg(feature = "indicatif-progress")]
pub use indicatif_impl::IndicatifProgress;

#[cfg(feature = "indicatif-progress")]
mod indicatif_impl {
    use super::ProgressTracker;
    use crate::config::Table;
    use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};
    use std::collections::HashMap;
    use std::io::{self, Write};
    use std::sync::{OnceLock, RwLock};

    // Pad table names to the longest TPC-DS main table, `household_demographics`,
    // so every bar starts in the same column. The test below catches future drift.
    const TABLE_NAME_WIDTH: usize = 22;
    const PROGRESS_BAR_WIDTH: usize = 28;

    /// Default [`ProgressTracker`] implementation backed by
    /// [`indicatif::MultiProgress`].
    ///
    /// Renders one bar per table on stderr. Bars are pre-allocated in
    /// [`ProgressTracker::register`] and are looked up by [`Table`] on each
    /// [`ProgressTracker::increment`] call.
    #[derive(Debug)]
    pub struct IndicatifProgress {
        multi: MultiProgress,
        tables: RwLock<HashMap<Table, ProgressBar>>,
    }

    impl IndicatifProgress {
        /// Construct an empty tracker. Tables are added via
        /// [`ProgressTracker::register`].
        pub fn new() -> Self {
            Self {
                multi: MultiProgress::new(),
                tables: RwLock::new(HashMap::new()),
            }
        }

        /// Return a writer that coordinates stderr log writes with progress
        /// bar redraws.
        pub fn log_writer(&self) -> Box<dyn io::Write + Send + 'static> {
            Box::new(IndicatifLogWriter {
                multi: self.multi.clone(),
            })
        }
    }

    impl Default for IndicatifProgress {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ProgressTracker for IndicatifProgress {
        fn register(&self, table: Table, total_units: u64) {
            let Ok(mut tables) = self.tables.write() else {
                return;
            };

            let pb = self.multi.add(ProgressBar::new(total_units));
            pb.set_style(bar_style().clone());
            pb.set_message(table.to_string());
            let pb = pb.with_finish(ProgressFinish::AndLeave);
            tables.insert(table, pb);
        }

        fn increment(&self, table: Table, units: u64) {
            let bar = {
                let Ok(tables) = self.tables.read() else {
                    return;
                };
                tables.get(&table).cloned()
            };
            if let Some(bar) = bar {
                bar.inc(units);
                if bar.length().is_some_and(|length| bar.position() >= length) {
                    bar.finish_using_style();
                }
            }
        }

        fn finish(&self) {
            let bars = {
                let Ok(tables) = self.tables.read() else {
                    return;
                };
                tables.values().cloned().collect::<Vec<_>>()
            };
            for bar in bars {
                bar.finish_using_style();
            }
        }
    }

    fn bar_style() -> &'static ProgressStyle {
        static STYLE: OnceLock<ProgressStyle> = OnceLock::new();
        STYLE.get_or_init(|| {
            let template = format!(
                "{{msg:{TABLE_NAME_WIDTH}}} [{{bar:{PROGRESS_BAR_WIDTH}}}]   Progress: {{percent:>3}}%"
            );

            ProgressStyle::default_bar()
                .template(&template)
                .expect("static progress bar template is valid")
                .progress_chars("█▓░")
        })
    }

    struct IndicatifLogWriter {
        multi: MultiProgress,
    }

    impl Write for IndicatifLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.multi.suspend(|| io::stderr().write(buf))
        }

        fn flush(&mut self) -> io::Result<()> {
            self.multi.suspend(|| io::stderr().flush())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn table_name_width_covers_main_tables() {
            let max_table_name_width = Table::main_tables()
                .into_iter()
                .map(|table| table.to_string().len())
                .max()
                .expect("TPC-DS has main tables");

            assert_eq!(max_table_name_width, TABLE_NAME_WIDTH);
        }
    }
}
