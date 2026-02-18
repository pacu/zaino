//! Command-line interface for Zaino.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Default path for the configuration file.
pub const DEFAULT_CONFIG_PATH: &str = "./zainod/zindexer.toml";

/// The Zcash Indexing Service.
#[derive(Parser, Debug)]
#[command(
    name = "zainod",
    version,
    about = "Zaino - The Zcash Indexing Service",
    long_about = None
)]
pub struct Cli {
    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Available subcommands.
#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Run the Zaino indexer service.
    Run {
        /// Path to the configuration file.
        #[arg(short, long, value_name = "FILE", default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    /// Generate an example configuration file with default values.
    GenerateConfig {
        /// Output path for the generated config file (defaults to stdout).
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,
    },
}

impl Command {
    /// Generate a default configuration file and write to output.
    pub fn generate_config(output: Option<PathBuf>) {
        let content = match crate::generate_default_config() {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error generating config: {}", e);
                std::process::exit(1);
            }
        };

        match output {
            Some(path) => {
                if let Err(e) = std::fs::write(&path, &content) {
                    eprintln!("Error writing to {}: {}", path.display(), e);
                    std::process::exit(1);
                }
                eprintln!("Generated config file: {}", path.display());
            }
            None => {
                print!("{}", content);
            }
        }
    }
}
