//! TPCH data generation CLI with a dbgen compatible API.
//!
//! This crate provides a CLI for generating TPCH data and tries to remain close
//! API wise to the original dbgen tool, as in we use the same command line flags
//! and arguments.
//!
//! -h, --help       Prints help information
//! -V, --version    Prints version information
//! -s, --scale      Scale factor for the data generation
//! -T, --tables     Tables to generate data for
//! -F, --format     Output format for the data (CSV or Parquet)
//! -O, --output     Output directory for the generated data
//
// The main function is the entry point for the CLI and it uses the `clap` crate
// to parse the command line arguments and then generate the data.

// tpchgen-cli/src/main.rs
use clap::{Parser, ValueEnum};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSupplierGenerator, RegionGenerator, SupplierGenerator,
};

#[derive(Parser)]
#[command(name = "tpchgen")]
#[command(about = "TPC-H Data Generator", long_about = None)]
struct Cli {
    /// Scale factor to address (default: 1)
    #[arg(short, long, default_value_t = 1.)]
    scale_factor: f64,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Which tables to generate (default: all)
    #[arg(short, long)]
    tables: Option<Vec<Table>>,

    /// Number of parts to generate (for parallel generation)
    #[arg(short, long, default_value_t = 1)]
    parts: i32,

    /// Which part to generate (1-based, only relevant if parts > 1)
    #[arg(long, default_value_t = 1)]
    part: i32,

    /// Output format: tbl, csv, parquet (default: tbl)
    #[arg(short, long, default_value = "tbl")]
    format: OutputFormat,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Table {
    Nation,
    Region,
    Part,
    Supplier,
    PartSupp,
    Customer,
    Orders,
    LineItem,
}

impl Table {
    fn name(&self) -> &'static str {
        match self {
            Table::Nation => "nation",
            Table::Region => "region",
            Table::Part => "part",
            Table::Supplier => "supplier",
            Table::PartSupp => "partsupp",
            Table::Customer => "customer",
            Table::Orders => "orders",
            Table::LineItem => "lineitem",
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Tbl,
    Csv,
    Parquet,
}

fn main() -> io::Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();
    cli.main()
}

impl Cli {
    fn main(self) -> io::Result<()> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(&self.output_dir)?;

        // Determine which tables to generate
        let tables: Vec<Table> = if let Some(tables) = self.tables.as_ref() {
            tables.clone()
        } else {
            vec![
                Table::Nation,
                Table::Region,
                Table::Part,
                Table::Supplier,
                Table::PartSupp,
                Table::Customer,
                Table::Orders,
                Table::LineItem,
            ]
        };

        // Generate each table
        for table in tables {
            match table {
                Table::Nation => self.generate_nation()?,
                Table::Region => self.generate_region()?,
                Table::Part => self.generate_part()?,
                Table::Supplier => self.generate_supplier()?,
                Table::PartSupp => self.generate_partsupp()?,
                Table::Customer => self.generate_customer()?,
                Table::Orders => self.generate_orders()?,
                Table::LineItem => self.generate_lineitem()?,
            }
        }

        println!("Generation complete!");
        Ok(())
    }

    /// return the output filename for the given table
    fn output_filename(&self, table: Table) -> String {
        let extension = match self.format {
            OutputFormat::Tbl => "tbl",
            OutputFormat::Csv => "csv",
            OutputFormat::Parquet => "parquet",
        };
        format!("{}.{extension}", table.name())
    }

    /// return a buffered file for writing the given filename in the output directory
    fn new_output_writer(&self, filename: &str) -> io::Result<BufWriter<File>> {
        let path = self.output_dir.join(filename);
        let file = File::create(path)?;
        Ok(BufWriter::with_capacity(32 * 1024, file))
    }

    fn generate_nation(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Nation);
        let writer = self.new_output_writer(&filename)?;

        let generator = NationGenerator::new();
        match self.format {
            OutputFormat::Tbl => self.nation_tbl(writer, generator),
            OutputFormat::Csv => self.nation_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_region(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Region);
        let writer = self.new_output_writer(&filename)?;

        let generator = RegionGenerator::new();
        match self.format {
            OutputFormat::Tbl => self.region_tbl(writer, generator),
            OutputFormat::Csv => self.region_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_part(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Part);
        let writer = self.new_output_writer(&filename)?;

        let generator = PartGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.part_tbl(writer, generator),
            OutputFormat::Csv => self.part_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_supplier(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Supplier);
        let writer = self.new_output_writer(&filename)?;

        let generator = SupplierGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.supplier_tbl(writer, generator),
            OutputFormat::Csv => self.supplier_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_partsupp(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::PartSupp);
        let writer = self.new_output_writer(&filename)?;

        let generator = PartSupplierGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.partsupp_tbl(writer, generator),
            OutputFormat::Csv => self.partsupp_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_customer(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Customer);
        let writer = self.new_output_writer(&filename)?;

        let generator = CustomerGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.customer_tbl(writer, generator),
            OutputFormat::Csv => self.customer_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_orders(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::Orders);
        let writer = self.new_output_writer(&filename)?;

        let generator = OrderGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.orders_tbl(writer, generator),
            OutputFormat::Csv => self.orders_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    fn generate_lineitem(&self) -> io::Result<()> {
        let filename = self.output_filename(Table::LineItem);
        let writer = self.new_output_writer(&filename)?;

        let generator = LineItemGenerator::new(self.scale_factor, self.part, self.parts);
        match self.format {
            OutputFormat::Tbl => self.lineitem_tbl(writer, generator),
            OutputFormat::Csv => self.lineitem_csv(writer, generator),
            OutputFormat::Parquet => {
                unimplemented!("Parquet output not yet implemented");
            }
        }
    }

    // Separate functions for each table/output format combination
    // to ensure they are inlined / a single function doesn't get out of hand
    // TODO: make these via macros

    fn nation_tbl<W: Write>(&self, mut w: W, gen: NationGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn nation_csv<W: Write>(&self, mut w: W, gen: NationGenerator) -> io::Result<()> {
        writeln!(w, "{}", NationCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", NationCsv::new(item))?;
        }
        w.flush()
    }

    fn region_tbl<W: Write>(&self, mut w: W, gen: RegionGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn region_csv<W: Write>(&self, mut w: W, gen: RegionGenerator) -> io::Result<()> {
        writeln!(w, "{}", RegionCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", RegionCsv::new(item))?;
        }
        w.flush()
    }

    fn part_tbl<W: Write>(&self, mut w: W, gen: PartGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn part_csv<W: Write>(&self, mut w: W, gen: PartGenerator) -> io::Result<()> {
        writeln!(w, "{}", PartCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", PartCsv::new(item))?;
        }
        w.flush()
    }

    fn supplier_tbl<W: Write>(&self, mut w: W, gen: SupplierGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn supplier_csv<W: Write>(&self, mut w: W, generator: SupplierGenerator) -> io::Result<()> {
        writeln!(w, "{}", SupplierCsv::header())?;
        for item in generator.iter() {
            writeln!(&mut w, "{}", SupplierCsv::new(item))?;
        }
        w.flush()
    }

    fn partsupp_tbl<W: Write>(&self, mut w: W, gen: PartSupplierGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn partsupp_csv<W: Write>(&self, mut w: W, gen: PartSupplierGenerator) -> io::Result<()> {
        writeln!(w, "{}", PartSuppCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", PartSuppCsv::new(item))?;
        }
        w.flush()
    }

    fn customer_tbl<W: Write>(&self, mut w: W, gen: CustomerGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn customer_csv<W: Write>(&self, mut w: W, gen: CustomerGenerator) -> io::Result<()> {
        writeln!(w, "{}", CustomerCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", CustomerCsv::new(item))?;
        }
        w.flush()
    }

    fn orders_tbl<W: Write>(&self, mut w: W, gen: OrderGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn orders_csv<W: Write>(&self, mut w: W, gen: OrderGenerator) -> io::Result<()> {
        writeln!(w, "{}", OrderCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", OrderCsv::new(item))?;
        }
        w.flush()
    }

    fn lineitem_tbl<W: Write>(&self, mut w: W, gen: LineItemGenerator) -> io::Result<()> {
        for item in gen.iter() {
            writeln!(&mut w, "{item}")?;
        }
        w.flush()
    }

    fn lineitem_csv<W: Write>(&self, mut w: W, gen: LineItemGenerator) -> io::Result<()> {
        writeln!(w, "{}", LineItemCsv::header())?;
        for item in gen.iter() {
            writeln!(&mut w, "{}", LineItemCsv::new(item))?;
        }
        w.flush()
    }
}
