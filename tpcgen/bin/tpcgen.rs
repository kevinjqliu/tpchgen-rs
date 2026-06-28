use clap::Parser;
use tpcgen::tpcgen_cli::Cli;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    Cli::parse().run().await
}
