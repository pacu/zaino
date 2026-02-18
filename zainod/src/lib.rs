//! Zaino Indexer service.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

use std::path::PathBuf;

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::{load_config, ZainodConfig};
use crate::error::IndexerError;
use crate::indexer::start_indexer;

pub mod cli;
pub mod config;
pub mod error;
pub mod indexer;

/// Header for generated configuration files.
pub const GENERATED_CONFIG_HEADER: &str = r#"# Zaino Configuration
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

/// Generate default configuration file content.
///
/// Returns the full config file content including header and TOML-serialized defaults.
pub fn generate_default_config() -> Result<String, IndexerError> {
    let config = ZainodConfig::default();

    let toml_content = toml::to_string_pretty(&config)
        .map_err(|e| IndexerError::ConfigError(format!("Failed to serialize config: {}", e)))?;

    Ok(format!("{}{}", GENERATED_CONFIG_HEADER, toml_content))
}

/// Run the Zaino indexer.
///
/// Initializes logging and runs the main indexer loop with restart support.
pub async fn run(config_path: PathBuf) {
    init_logging();

    loop {
        match start_indexer(load_config(&config_path).unwrap()).await {
            Ok(joinhandle_result) => {
                info!("Zaino Indexer started successfully.");
                match joinhandle_result.await {
                    Ok(indexer_result) => match indexer_result {
                        Ok(()) => {
                            info!("Exiting Zaino successfully.");
                            break;
                        }
                        Err(IndexerError::Restart) => {
                            error!("Zaino encountered critical error, restarting.");
                            continue;
                        }
                        Err(e) => {
                            error!("Exiting Zaino with error: {}", e);
                            break;
                        }
                    },
                    Err(e) => {
                        error!("Zaino exited early with error: {}", e);
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Zaino failed to start with error: {}", e);
                break;
            }
        }
    }
}

/// Initialize the tracing subscriber for logging.
fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_target(true)
        .init();
}
