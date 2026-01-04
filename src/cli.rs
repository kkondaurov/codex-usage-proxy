use clap::Parser;
use std::path::PathBuf;

/// Command-line interface for configuring the dashboard.
#[derive(Debug, Parser)]
#[command(author, version, about = "Local Codex dashboard and TUI", long_about = None)]
pub struct Cli {
    /// Path to a TOML configuration file (defaults to ./codex-usage.toml if present).
    #[arg(long, value_name = "FILE")]
    pub config_path: Option<PathBuf>,
    /// Rebuild usage data by truncating all non-pricing tables before ingesting.
    #[arg(long)]
    pub rebuild: bool,
}
