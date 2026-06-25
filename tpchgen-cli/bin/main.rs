use clap::Parser;
use std::io;

mod cli;

use cli::Cli;

#[tokio::main]
async fn main() -> io::Result<()> {
    Cli::parse().main().await
}
