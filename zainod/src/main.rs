//! Zaino Indexer daemon.

use clap::Parser;

use zainodlib::cli::{Cli, Command};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Run { config } => zainodlib::run(config).await,
        Command::GenerateConfig { output } => Command::generate_config(output),
    }
}
