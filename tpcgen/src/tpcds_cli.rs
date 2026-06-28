use clap::{ArgAction, Args, Subcommand};
use log::{info, LevelFilter};
use std::fmt;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::sync::Arc;
use tpcdsgen::config::{CompatMode, Options as TpcdsOptions, Table as TpcdsTable};
use tpcdsgen::progress::{IndicatifProgress, ProgressTracker};
use tpchgen_cli::{Compression, DEFAULT_PARQUET_ROW_GROUP_BYTES};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const NOT_IMPLEMENTED: &str = "TPC-DS data generation is not yet implemented";

#[derive(Args)]
#[command(version)]
#[command(args_conflicts_with_subcommands = true)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    args: DatArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate DAT (pipe-delimited) output
    Dat(DatArgs),
    /// Generate CSV output with CSV-specific options
    Csv(CsvArgs),
    /// Generate Apache Parquet output with Parquet-specific options
    Parquet(ParquetArgs),
}

#[derive(Args)]
struct DatArgs {
    #[command(flatten)]
    common: CommonArgs,
}

#[derive(Args)]
struct CsvArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// CSV delimiter character (default: ',')
    ///
    /// Specifies the delimiter character to use when generating CSV files.
    ///
    /// Supports escape sequences: \t (tab), \n (newline), \r (carriage return), \\ (backslash)
    /// Common delimiters: ',' (comma), '|' (pipe), '\t' (tab), ';' (semicolon)
    #[arg(long, default_value = ",", value_parser = parse_delimiter)]
    delimiter: char,
}

#[derive(Args)]
struct ParquetArgs {
    #[command(flatten)]
    common: CommonArgs,

    /// Parquet block compression format.
    ///
    /// Supported values: UNCOMPRESSED, ZSTD(N), SNAPPY, GZIP, LZO, BROTLI, LZ4
    ///
    /// Note to use zstd you must supply the "compression" level (1-22)
    /// as a number in parentheses, e.g. `ZSTD(1)` for level 1 compression.
    ///
    /// Using `ZSTD` results in the best compression, but is about 2x slower than
    /// UNCOMPRESSED. For example, for the lineitem table at SF=10
    ///
    ///   ZSTD(1):      1.9G  (0.52 GB/sec)
    ///   SNAPPY:       2.4G  (0.75 GB/sec)
    ///   UNCOMPRESSED: 3.8G  (1.41 GB/sec)
    #[arg(short = 'c', long, default_value = "SNAPPY")]
    compression: Compression,

    /// Target size in row group bytes in Parquet files
    ///
    /// Row groups are the typical unit of parallel processing and compression
    /// with many query engines. Therefore, smaller row groups enable better
    /// parallelism and lower peak memory use but may reduce compression
    /// efficiency.
    ///
    /// Note: Parquet files are limited to 32k row groups, so at high scale
    /// factors, the row group size may be increased to keep the number of row
    /// groups under this limit.
    ///
    /// Typical values range from 10MB to 100MB.
    #[arg(long, default_value_t = DEFAULT_PARQUET_ROW_GROUP_BYTES)]
    row_group_bytes: i64,
}

#[derive(Args)]
struct CommonArgs {
    /// Scale factor to create
    #[arg(short, long, default_value_t = 1.)]
    scale_factor: f64,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Which tables to generate (default: all)
    #[arg(short = 'T', long = "tables", value_delimiter = ',')]
    tables: Option<Vec<String>>,

    /// Suffix for generated data files
    #[arg(long = "suffix", default_value = TpcdsOptions::DEFAULT_SUFFIX)]
    suffix: String,

    /// String representation for null values
    #[arg(long = "null", default_value = TpcdsOptions::DEFAULT_NULL_STRING)]
    null_string: String,

    /// Separator between columns
    #[arg(long = "separator", default_value = "|")]
    separator: String,

    /// Do not terminate each row with a separator
    #[arg(long = "do-not-terminate")]
    do_not_terminate: bool,

    /// Use gender-neutral manager names
    #[arg(long = "no-sexism")]
    no_sexism: bool,

    /// Build data in `n` separate chunks
    #[arg(long = "parallelism", default_value_t = TpcdsOptions::DEFAULT_PARALLELISM)]
    parallelism: i32,

    /// Overwrite existing data files for tables
    #[arg(long = "overwrite")]
    overwrite: bool,

    /// Reference implementation to match
    #[arg(long = "compat", default_value = "trino")]
    compat: CompatMode,

    /// Verbose output
    ///
    /// When specified, sets the log level to `info` and ignores the `RUST_LOG`
    /// environment variable. When not specified, uses `RUST_LOG`
    #[arg(short, long, default_value_t = false, conflicts_with = "quiet")]
    verbose: bool,

    /// Quiet mode - only show error-level logs
    #[arg(short, long, default_value_t = false, conflicts_with = "verbose")]
    quiet: bool,

    /// Disable progress bars during data generation.
    ///
    /// Bars are also auto-suppressed by `--quiet` or when stderr is not a terminal.
    #[arg(long = "no-progress", action = ArgAction::SetFalse, default_value_t = true)]
    progress_bars_enabled: bool,
}

impl Cli {
    pub(crate) fn run(self) -> Result<()> {
        match self.command {
            Some(Commands::Dat(args)) => args.run(),
            Some(Commands::Csv(args)) => args.run(),
            Some(Commands::Parquet(args)) => args.run(),
            None => self.args.run(),
        }
    }
}

impl DatArgs {
    fn run(self) -> Result<()> {
        self.common.run_dat()
    }
}

impl CsvArgs {
    fn run(self) -> Result<()> {
        let _ = self.delimiter;
        self.common.run_not_implemented()
    }
}

impl ParquetArgs {
    fn run(self) -> Result<()> {
        let _ = (self.compression, self.row_group_bytes);
        self.common.run_not_implemented()
    }
}

impl CommonArgs {
    fn run_dat(self) -> Result<()> {
        let progress = self.progress_tracker();
        configure_logging(
            self.verbose,
            self.quiet,
            progress.as_ref().map(|progress| progress.log_writer()),
        );
        let progress = progress.map(|progress| progress as Arc<dyn ProgressTracker>);
        let options = self.to_tpcds_options(None);
        let session = options.to_session()?;

        if let Some(tables) = &self.tables {
            let tables = parse_tables(tables)?;
            self.run_dat_for_tables(&session, &tables, progress)
        } else {
            self.run_dat_for_session(&session, progress)
        }
    }

    fn run_dat_for_session(
        &self,
        session: &tpcdsgen::config::Session,
        progress: Option<Arc<dyn ProgressTracker>>,
    ) -> Result<()> {
        if let Some(progress) = progress {
            tpcdsgen::dat::generate_with_progress(session, progress)
        } else {
            tpcdsgen::dat::generate(session)
        }
    }

    fn run_dat_for_tables(
        &self,
        session: &tpcdsgen::config::Session,
        tables: &[TpcdsTable],
        progress: Option<Arc<dyn ProgressTracker>>,
    ) -> Result<()> {
        if let Some(progress) = progress {
            tpcdsgen::dat::generate_tables_with_progress(session, tables, progress)
        } else {
            tpcdsgen::dat::generate_tables(session, tables)
        }
    }

    fn progress_tracker(&self) -> Option<Arc<IndicatifProgress>> {
        self.should_show_progress_bars()
            .then(|| Arc::new(IndicatifProgress::new()))
    }

    fn should_show_progress_bars(&self) -> bool {
        self.progress_bars_enabled && !self.quiet && io::stderr().is_terminal()
    }

    fn to_tpcds_options(&self, table: Option<String>) -> TpcdsOptions {
        let mut options = TpcdsOptions::new();
        options.scale = self.scale_factor;
        options.directory = self.output_dir.to_string_lossy().into_owned();
        options.suffix = self.suffix.clone();
        options.table = table;
        options.null_string = self.null_string.clone();
        options.separator = self.separator.clone();
        options.do_not_terminate = self.do_not_terminate;
        options.no_sexism = self.no_sexism;
        options.parallelism = self.parallelism;
        options.overwrite = self.overwrite;
        options.compat = self.compat;
        options
    }

    fn run_not_implemented(self) -> Result<()> {
        let _ = self;
        Err(Box::new(NotImplemented))
    }
}

fn parse_tables(tables: &[String]) -> Result<Vec<TpcdsTable>> {
    tables
        .iter()
        .map(|table| table.parse::<TpcdsTable>().map_err(Into::into))
        .collect()
}

fn configure_logging(
    verbose: bool,
    quiet: bool,
    log_writer: Option<Box<dyn io::Write + Send + 'static>>,
) {
    let mut builder = env_logger::builder();
    if quiet {
        builder.filter_level(LevelFilter::Error);
    } else if verbose {
        builder.filter_level(LevelFilter::Info);
    } else {
        builder.filter_level(LevelFilter::Warn).parse_default_env();
    }
    if let Some(log_writer) = log_writer {
        builder.target(env_logger::Target::Pipe(log_writer));
    }

    let _ = builder.try_init();

    if verbose {
        info!("Verbose output enabled (ignoring RUST_LOG environment variable)");
    }
}

struct NotImplemented;

impl fmt::Display for NotImplemented {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(NOT_IMPLEMENTED)
    }
}

impl fmt::Debug for NotImplemented {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for NotImplemented {}

fn parse_delimiter(s: &str) -> std::result::Result<char, String> {
    let parsed = match s {
        "\\t" => '\t',
        "\\n" => '\n',
        "\\r" => '\r',
        "\\\\" => '\\',
        _ => {
            let chars: Vec<char> = s.chars().collect();
            if chars.len() != 1 {
                return Err(format!(
                    "Delimiter must be a single character or escape sequence (\\t, \\n, \\r, \\\\), got: '{}'",
                    s
                ));
            }
            chars[0]
        }
    };
    if !parsed.is_ascii() {
        return Err(format!(
            "Delimiter must be an ASCII character, got: '{}'",
            parsed
        ));
    }
    Ok(parsed)
}
