//! Zaino Indexer service.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

use std::path::PathBuf;

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::load_config;
use crate::error::IndexerError;
use crate::indexer::start_indexer;

pub mod cli;
pub mod config;
pub mod error;
pub mod indexer;

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
