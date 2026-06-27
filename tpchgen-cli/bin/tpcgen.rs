use clap::{Parser, Subcommand};

mod tpcds_cli;
mod tpch_cli;

use tpcds_cli::Cli as TpcdsCli;
use tpch_cli::Cli as TpchCli;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(name = "tpcgen")]
#[command(version)]
#[command(
    about = "TPC data generator",
    long_about = r#"
TPC data generator (https://github.com/clflushopt/tpchgen-rs)

Examples

# TPC-H TBL data:

tpcgen tpch -s 1 --output-dir=/tmp/tpch

# TPC-H CSV data:

tpcgen tpch csv -s 1 --output-dir=/tmp/tpch

# TPC-H Apache Parquet data:

tpcgen tpch parquet -s 100 --tables=lineitem --parts=10 --output-dir=/tmp/tpch

# TPC-DS DAT data:

tpcgen tpcds -s 1 --output-dir=/tmp/tpcds

# TPC-DS Apache Parquet data:

tpcgen tpcds parquet -s 100 --tables=store_sales --output-dir=/tmp/tpcds
"#
)]
struct TpcgenCli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// TPC-H data
    Tpch(TpchCli),
    /// TPC-DS data
    Tpcds(TpcdsCli),
}

#[tokio::main]
async fn main() -> Result<()> {
    TpcgenCli::parse().run().await
}

impl TpcgenCli {
    async fn run(self) -> Result<()> {
        match self.command {
            Command::Tpch(args) => args.main().await?,
            Command::Tpcds(args) => args.run()?,
        }

        Ok(())
    }
}
