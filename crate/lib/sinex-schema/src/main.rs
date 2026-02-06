//! The command-line interface for the Sinex database migrator and schema manager.
//!
//! This binary entry point provides two main functionalities:
//! 1. Database migrations via `sea-orm-cli` (up, down, status, etc.)
//! 2. Event payload schema management (generate, sync) - not yet implemented

use color_eyre::eyre::{bail, Result};
use sea_orm_migration::prelude::*;
use sinex_schema::Migrator;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Check if first argument is an unimplemented schema management command
    let args: Vec<String> = env::args().collect();
    if let Some(cmd) = args.get(1) {
        match cmd.as_str() {
            "generate" => {
                bail!(
                    "The 'generate' command for event payload schemas is not yet implemented.\n\
                     This feature will generate JSON schemas from Rust event payload types.\n\
                     For now, schemas must be created manually in the schemas/v1 directory."
                );
            }
            "sync" => {
                bail!(
                    "The 'sync' command for event payload schemas is not yet implemented.\n\
                     This feature will deploy JSON schemas to the sinex_schemas.event_payload_schemas table.\n\
                     For now, use direct SQL inserts or the schema_cache repository methods."
                );
            }
            _ => {}
        }
    }

    // For all other commands, delegate to SeaORM migration CLI
    // This handles: up, down, status, fresh, reset, etc.
    cli::run_cli(Migrator).await;

    Ok(())
}
