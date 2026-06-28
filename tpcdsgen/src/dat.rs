/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! TPC-DS DAT output generation.

use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use crate::config::{Session, Table};
use crate::output::CompatWriter;
#[cfg(feature = "progress")]
use crate::progress::ProgressTracker;
use crate::progress::RunProgress;
use crate::row::*;
#[cfg(feature = "progress")]
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Generate TPC-DS data in DAT format.
pub fn generate(session: &Session) -> Result<()> {
    let tables = tables_for_session(session);
    generate_tables_inner(session, tables, RunProgress::default())
}

/// Generate the specified TPC-DS tables in DAT format.
pub fn generate_tables(session: &Session, tables: &[Table]) -> Result<()> {
    generate_tables_inner(session, tables.to_vec(), RunProgress::default())
}

/// Generate TPC-DS data in DAT format with progress tracking.
#[cfg(feature = "progress")]
pub fn generate_with_progress(session: &Session, progress: Arc<dyn ProgressTracker>) -> Result<()> {
    let tables = tables_for_session(session);
    generate_tables_inner(session, tables, RunProgress::with_tracker(progress))
}

/// Generate the specified TPC-DS tables in DAT format with progress tracking.
#[cfg(feature = "progress")]
pub fn generate_tables_with_progress(
    session: &Session,
    tables: &[Table],
    progress: Arc<dyn ProgressTracker>,
) -> Result<()> {
    generate_tables_inner(
        session,
        tables.to_vec(),
        RunProgress::with_tracker(progress),
    )
}

fn generate_tables_inner(
    session: &Session,
    tables: Vec<Table>,
    progress: RunProgress,
) -> Result<()> {
    println!("TPC-DS Data Generator (Rust)");
    println!("Scale factor: {}", session.get_scaling().get_scale());
    println!("Output directory: {}", session.get_target_directory());

    create_dir_all(session.get_target_directory())?;

    let tables = dat_output_tables(tables);
    let start = Instant::now();
    let totals = progress_totals(&tables, session);
    progress.register_totals(&totals);

    for table in tables {
        generate_table(table, session, &progress)?;
    }

    progress.finish();

    let elapsed = start.elapsed();
    println!("\nCompleted in {:.2}s", elapsed.as_secs_f64());

    Ok(())
}

fn dat_output_tables(tables: Vec<Table>) -> Vec<Table> {
    tables
        .into_iter()
        .filter(|table| table.is_main_table())
        .collect()
}

fn tables_for_session(session: &Session) -> Vec<Table> {
    if session.generate_only_one_table() {
        vec![session.get_only_table_to_generate()]
    } else {
        Table::main_tables()
    }
}

fn progress_totals(tables: &[Table], session: &Session) -> Vec<(Table, u64)> {
    tables
        .iter()
        .filter_map(|&table| progress_total_for_table(table, session))
        .collect()
}

fn progress_total_for_table(table: Table, session: &Session) -> Option<(Table, u64)> {
    match table {
        // The *_returns totals are not known up front because those rows are
        // randomly emitted during generation of their respective parent tables.
        Table::StoreReturns | Table::CatalogReturns | Table::WebReturns => None,
        table => Some((table, session.get_scaling().get_row_count(table) as u64)),
    }
}

fn generate_table(table: Table, session: &Session, progress: &RunProgress) -> Result<()> {
    match table {
        // Simple dimension tables
        Table::CallCenter => generate_simple::<CallCenterRowGenerator>(table, session, progress),
        Table::CatalogPage => generate_simple::<CatalogPageRowGenerator>(table, session, progress),
        Table::Customer => generate_simple::<CustomerRowGenerator>(table, session, progress),
        Table::CustomerAddress => {
            generate_simple::<CustomerAddressRowGenerator>(table, session, progress)
        }
        Table::CustomerDemographics => {
            generate_simple::<CustomerDemographicsRowGenerator>(table, session, progress)
        }
        Table::DateDim => generate_simple::<DateDimRowGenerator>(table, session, progress),
        Table::DbgenVersion => {
            generate_simple::<DbgenVersionRowGenerator>(table, session, progress)
        }
        Table::HouseholdDemographics => {
            generate_simple::<HouseholdDemographicsRowGenerator>(table, session, progress)
        }
        Table::IncomeBand => generate_simple::<IncomeBandRowGenerator>(table, session, progress),
        Table::Item => generate_simple::<ItemRowGenerator>(table, session, progress),
        Table::Promotion => generate_simple::<PromotionRowGenerator>(table, session, progress),
        Table::Reason => generate_simple::<ReasonRowGenerator>(table, session, progress),
        Table::ShipMode => generate_simple::<ShipModeRowGenerator>(table, session, progress),
        Table::Store => generate_simple::<StoreRowGenerator>(table, session, progress),
        Table::TimeDim => generate_simple::<TimeDimRowGenerator>(table, session, progress),
        Table::Warehouse => generate_simple::<WarehouseRowGenerator>(table, session, progress),
        Table::WebPage => generate_simple::<WebPageRowGenerator>(table, session, progress),
        Table::WebSite => generate_simple::<WebSiteRowGenerator>(table, session, progress),

        // Sales + Returns pairs
        Table::StoreSales => generate_store_sales(session, progress),
        Table::StoreReturns => Ok(()), // Generated with StoreSales
        Table::CatalogSales => generate_catalog_sales(session, progress),
        Table::CatalogReturns => Ok(()), // Generated with CatalogSales
        Table::WebSales => generate_web_sales(session, progress),
        Table::WebReturns => Ok(()), // Generated with WebSales

        // Special tables
        Table::Inventory => generate_inventory(session, progress),

        // Source tables - skip
        _ => Ok(()),
    }
}

/// Trait for creating row generators
trait RowGeneratorFactory: RowGenerator + Sized {
    fn create() -> Self;
}

// Implement factory for all simple generators
macro_rules! impl_factory {
    ($($gen:ty),*) => {
        $(
            impl RowGeneratorFactory for $gen {
                fn create() -> Self { Self::new() }
            }
        )*
    };
}

impl_factory!(
    CallCenterRowGenerator,
    CatalogPageRowGenerator,
    CustomerRowGenerator,
    CustomerAddressRowGenerator,
    CustomerDemographicsRowGenerator,
    DateDimRowGenerator,
    DbgenVersionRowGenerator,
    HouseholdDemographicsRowGenerator,
    IncomeBandRowGenerator,
    ItemRowGenerator,
    PromotionRowGenerator,
    ReasonRowGenerator,
    ShipModeRowGenerator,
    StoreRowGenerator,
    TimeDimRowGenerator,
    WarehouseRowGenerator,
    WebPageRowGenerator,
    WebSiteRowGenerator
);

/// Generate a simple table (one row per row_number, no child tables)
fn generate_simple<G: RowGeneratorFactory>(
    table: Table,
    session: &Session,
    progress: &RunProgress,
) -> Result<()> {
    let mut generator = G::create();
    let row_count = session.get_scaling().get_row_count(table);
    let show_status = !progress.is_enabled();
    let progress = progress.for_table(table);

    let path = get_output_path(table, session);
    let file = File::create(&path)?;
    let mut writer = CompatWriter::new(BufWriter::new(file), session.get_compat_mode());

    if show_status {
        print!("Generating {}... ", table.get_name());
        std::io::stdout().flush()?;
    }

    for row_number in 1..=row_count {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;

        for row in result.get_rows() {
            row.write_to(&mut writer, session.get_separator())?;
        }

        generator.consume_remaining_seeds_for_row();
        progress.increment_output_unit();
    }

    writer.flush()?;
    if show_status {
        println!("{} rows -> {}", row_count, path.display());
    }

    Ok(())
}

/// Generate store_sales and store_returns together
fn generate_store_sales(session: &Session, progress: &RunProgress) -> Result<()> {
    let mut generator = StoreSalesRowGenerator::new();
    let num_orders = session.get_scaling().get_row_count(Table::StoreSales);
    let show_status = !progress.is_enabled();
    let progress = progress.for_table(Table::StoreSales);

    let sales_path = get_output_path(Table::StoreSales, session);
    let returns_path = get_output_path(Table::StoreReturns, session);

    let compat_mode = session.get_compat_mode();
    let mut sales_writer =
        CompatWriter::new(BufWriter::new(File::create(&sales_path)?), compat_mode);
    let mut returns_writer =
        CompatWriter::new(BufWriter::new(File::create(&returns_path)?), compat_mode);

    if show_status {
        print!("Generating store_sales + store_returns... ");
        std::io::stdout().flush()?;
    }

    let mut sales_count = 0i64;
    let mut returns_count = 0i64;
    let mut row_number = 1i64;

    while row_number <= num_orders {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        let rows = result.get_rows();

        if !rows.is_empty() {
            rows[0].write_to(&mut sales_writer, session.get_separator())?;
            sales_count += 1;
        }

        if rows.len() > 1 {
            rows[1].write_to(&mut returns_writer, session.get_separator())?;
            returns_count += 1;
        }

        if result.should_end_row() {
            generator.consume_remaining_seeds_for_row();
            row_number += 1;
            progress.increment_output_unit();
        }
    }

    sales_writer.flush()?;
    returns_writer.flush()?;

    if show_status {
        println!(
            "{} sales, {} returns -> {}, {}",
            sales_count,
            returns_count,
            sales_path.display(),
            returns_path.display()
        );
    }

    Ok(())
}

/// Generate catalog_sales and catalog_returns together
fn generate_catalog_sales(session: &Session, progress: &RunProgress) -> Result<()> {
    let mut generator = CatalogSalesRowGenerator::new();
    let num_orders = session.get_scaling().get_row_count(Table::CatalogSales);
    let show_status = !progress.is_enabled();
    let progress = progress.for_table(Table::CatalogSales);

    let sales_path = get_output_path(Table::CatalogSales, session);
    let returns_path = get_output_path(Table::CatalogReturns, session);

    let compat_mode = session.get_compat_mode();
    let mut sales_writer =
        CompatWriter::new(BufWriter::new(File::create(&sales_path)?), compat_mode);
    let mut returns_writer =
        CompatWriter::new(BufWriter::new(File::create(&returns_path)?), compat_mode);

    if show_status {
        print!("Generating catalog_sales + catalog_returns... ");
        std::io::stdout().flush()?;
    }

    let mut sales_count = 0i64;
    let mut returns_count = 0i64;
    let mut row_number = 1i64;

    while row_number <= num_orders {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        let rows = result.get_rows();

        if !rows.is_empty() {
            rows[0].write_to(&mut sales_writer, session.get_separator())?;
            sales_count += 1;
        }

        if rows.len() > 1 {
            rows[1].write_to(&mut returns_writer, session.get_separator())?;
            returns_count += 1;
        }

        if result.should_end_row() {
            generator.consume_remaining_seeds_for_row();
            row_number += 1;
            progress.increment_output_unit();
        }
    }

    sales_writer.flush()?;
    returns_writer.flush()?;

    if show_status {
        println!(
            "{} sales, {} returns -> {}, {}",
            sales_count,
            returns_count,
            sales_path.display(),
            returns_path.display()
        );
    }

    Ok(())
}

/// Generate web_sales and web_returns together
fn generate_web_sales(session: &Session, progress: &RunProgress) -> Result<()> {
    let mut generator = WebSalesRowGenerator::new();
    let num_orders = session.get_scaling().get_row_count(Table::WebSales);
    let show_status = !progress.is_enabled();
    let progress = progress.for_table(Table::WebSales);

    let sales_path = get_output_path(Table::WebSales, session);
    let returns_path = get_output_path(Table::WebReturns, session);

    let compat_mode = session.get_compat_mode();
    let mut sales_writer =
        CompatWriter::new(BufWriter::new(File::create(&sales_path)?), compat_mode);
    let mut returns_writer =
        CompatWriter::new(BufWriter::new(File::create(&returns_path)?), compat_mode);

    if show_status {
        print!("Generating web_sales + web_returns... ");
        std::io::stdout().flush()?;
    }

    let mut sales_count = 0i64;
    let mut returns_count = 0i64;
    let mut row_number = 1i64;

    while row_number <= num_orders {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;
        let rows = result.get_rows();

        if !rows.is_empty() {
            rows[0].write_to(&mut sales_writer, session.get_separator())?;
            sales_count += 1;
        }

        if rows.len() > 1 {
            rows[1].write_to(&mut returns_writer, session.get_separator())?;
            returns_count += 1;
        }

        if result.should_end_row() {
            generator.consume_remaining_seeds_for_row();
            row_number += 1;
            progress.increment_output_unit();
        }
    }

    sales_writer.flush()?;
    returns_writer.flush()?;

    if show_status {
        println!(
            "{} sales, {} returns -> {}, {}",
            sales_count,
            returns_count,
            sales_path.display(),
            returns_path.display()
        );
    }

    Ok(())
}

/// Generate inventory table.
fn generate_inventory(session: &Session, progress: &RunProgress) -> Result<()> {
    let mut generator = InventoryRowGenerator::new();
    let num_rows = session.get_scaling().get_row_count(Table::Inventory);
    let show_status = !progress.is_enabled();
    let progress = progress.for_table(Table::Inventory);

    let path = get_output_path(Table::Inventory, session);
    let mut writer = CompatWriter::new(
        BufWriter::new(File::create(&path)?),
        session.get_compat_mode(),
    );

    if show_status {
        print!("Generating inventory... ");
        std::io::stdout().flush()?;
    }

    for row_number in 1..=num_rows {
        let result = generator.generate_row_and_child_rows(row_number, session, None, None)?;

        for row in result.get_rows() {
            row.write_to(&mut writer, session.get_separator())?;
        }

        generator.consume_remaining_seeds_for_row();
        progress.increment_output_unit();
    }

    writer.flush()?;
    if show_status {
        println!("{} rows -> {}", num_rows, path.display());
    }

    Ok(())
}

/// Get output file path for a table
fn get_output_path(table: Table, session: &Session) -> std::path::PathBuf {
    Path::new(session.get_target_directory()).join(format!(
        "{}{}",
        table.get_name(),
        session.get_suffix()
    ))
}

#[cfg(all(test, feature = "progress"))]
mod tests {
    use super::*;
    use crate::config::Options;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ProgressEvent {
        Register(Table, u64),
        Increment(Table, u64),
        Finish,
    }

    #[derive(Debug, Default)]
    struct CountingProgress {
        registered: Mutex<Vec<(Table, u64)>>,
        increments: Mutex<HashMap<Table, u64>>,
        events: Mutex<Vec<ProgressEvent>>,
        finishes: AtomicUsize,
    }

    impl ProgressTracker for CountingProgress {
        fn register(&self, table: Table, total_units: u64) {
            self.events
                .lock()
                .expect("events lock")
                .push(ProgressEvent::Register(table, total_units));
            self.registered
                .lock()
                .expect("registered lock")
                .push((table, total_units));
        }

        fn increment(&self, table: Table, units: u64) {
            self.events
                .lock()
                .expect("events lock")
                .push(ProgressEvent::Increment(table, units));
            *self
                .increments
                .lock()
                .expect("increments lock")
                .entry(table)
                .or_default() += units;
        }

        fn finish(&self) {
            self.events
                .lock()
                .expect("events lock")
                .push(ProgressEvent::Finish);
            self.finishes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn generate_with_progress_reports_reason_rows() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let mut options = Options::new();
        options.directory = temp_dir.path().to_string_lossy().into_owned();
        options.table = Some("reason".to_string());
        let session = options.to_session().expect("session");

        let tracker = Arc::new(CountingProgress::default());
        let progress: Arc<dyn ProgressTracker> = tracker.clone();
        generate_with_progress(&session, progress).expect("generate");

        assert_eq!(
            tracker
                .registered
                .lock()
                .expect("registered lock")
                .as_slice(),
            &[(Table::Reason, 35)]
        );
        assert_eq!(
            tracker
                .increments
                .lock()
                .expect("increments lock")
                .get(&Table::Reason),
            Some(&35)
        );
        assert_eq!(tracker.finishes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn generate_tables_with_progress_registers_all_tables_before_incrementing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let mut options = Options::new();
        options.directory = temp_dir.path().to_string_lossy().into_owned();
        let session = options.to_session().expect("session");

        let tracker = Arc::new(CountingProgress::default());
        let progress: Arc<dyn ProgressTracker> = tracker.clone();
        generate_tables_with_progress(&session, &[Table::Reason, Table::ShipMode], progress)
            .expect("generate");

        let events = tracker.events.lock().expect("events lock");
        assert_eq!(
            &events[..2],
            &[
                ProgressEvent::Register(Table::Reason, 35),
                ProgressEvent::Register(Table::ShipMode, 20)
            ]
        );
        assert!(matches!(
            events[2],
            ProgressEvent::Increment(Table::Reason, 1)
        ));
    }

    #[test]
    fn generate_tables_with_progress_does_not_register_source_tables() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let mut options = Options::new();
        options.directory = temp_dir.path().to_string_lossy().into_owned();
        let session = options.to_session().expect("session");

        let tracker = Arc::new(CountingProgress::default());
        let progress: Arc<dyn ProgressTracker> = tracker.clone();
        generate_tables_with_progress(&session, &[Table::SBrand], progress).expect("generate");

        assert!(tracker
            .registered
            .lock()
            .expect("registered lock")
            .is_empty());
        assert!(tracker
            .increments
            .lock()
            .expect("increments lock")
            .is_empty());
        assert_eq!(tracker.finishes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn generate_tables_with_progress_matches_registered_totals() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let mut options = Options::new();
        options.directory = temp_dir.path().to_string_lossy().into_owned();
        options.scale = 0.001;
        let session = options.to_session().expect("session");

        let tables = [
            Table::Reason,
            Table::ShipMode,
            Table::StoreSales,
            Table::CatalogSales,
            Table::WebSales,
            Table::Inventory,
        ];
        let tracker = Arc::new(CountingProgress::default());
        let progress: Arc<dyn ProgressTracker> = tracker.clone();
        generate_tables_with_progress(&session, &tables, progress).expect("generate");

        let registered = tracker.registered.lock().expect("registered lock");
        let increments = tracker.increments.lock().expect("increments lock");
        assert!(
            !registered.is_empty(),
            "expected representative tables to register progress totals"
        );
        for (table, total) in registered.iter().copied() {
            assert_eq!(
                increments.get(&table).copied(),
                Some(total),
                "expected {table} increments to match the registered total"
            );
        }
    }
}
