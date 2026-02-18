//! Zaino Indexer daemon.

use clap::Parser;

use zainodlib::cli::{default_config_path, Cli, Command};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Run { config } => {
            let config_path = config.unwrap_or_else(default_config_path);
            zainodlib::run(config_path).await
        }
        Command::GenerateConfig { output } => Command::generate_config(output),
    }
}
