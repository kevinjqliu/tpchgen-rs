use clap::Parser;
use std::io;

mod tpch_cli;

use tpch_cli::Cli;

#[tokio::main]
async fn main() -> io::Result<()> {
    Cli::parse().main().await
}
