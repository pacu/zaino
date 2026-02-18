//! Command-line interface for Zaino.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Returns the default config path following XDG Base Directory spec.
///
/// Uses `$XDG_CONFIG_HOME/zaino/zainod.toml` if set,
/// otherwise falls back to `$HOME/.config/zaino/zainod.toml`.
pub fn default_config_path() -> PathBuf {
    let config_dir = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").expect("HOME not set");
            PathBuf::from(home).join(".config")
        });

    config_dir.join("zaino").join("zainod.toml")
}

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
        /// Path to the configuration file. Defaults to $XDG_CONFIG_HOME/zaino/zainod.toml
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
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
        let content = match crate::config::generate_default_config() {
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
