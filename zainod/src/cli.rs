//! Command-line interface for Zaino.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::ZainodConfig;

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
    /// Generate a default configuration file.
    pub fn generate_config(output: Option<PathBuf>) {
        let config = ZainodConfig::default();

        let header = r#"# Zaino Configuration
#
# Generated with `zainod generate-config`
#
# Configuration sources are layered (highest priority first):
#   1. Environment variables (prefix: ZAINO_)
#   2. TOML configuration file
#   3. Built-in defaults
#
# For detailed documentation, see:
#   https://github.com/zingolabs/zaino

"#;

        let toml_content = match toml::to_string_pretty(&config) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error serializing config: {}", e);
                std::process::exit(1);
            }
        };

        let output_content = format!("{}{}", header, toml_content);

        match output {
            Some(path) => {
                if let Err(e) = std::fs::write(&path, &output_content) {
                    eprintln!("Error writing to {}: {}", path.display(), e);
                    std::process::exit(1);
                }
                eprintln!("Generated config file: {}", path.display());
            }
            None => {
                print!("{}", output_content);
            }
        }
    }
}
