//! The command-line interface for the Sinex database migrator.
//!
//! This binary entry point uses the `sea-orm-cli` to apply, revert, and
//! manage the database schema migrations defined in this crate.

use color_eyre::eyre::Result;
use sea_orm_migration::prelude::*;
use sinex_schema::Migrator; // Import our canonical Migrator

#[tokio::main]
async fn main() -> Result<()> {
    // Standard setup for eyre error reporting and tokio runtime.
    color_eyre::install()?;

    // Run the SeaORM migration CLI, passing it our Migrator struct.
    // This command handles all subcommand parsing (up, down, status, etc.).
    cli::run_cli(Migrator).await;

    Ok(())
}
