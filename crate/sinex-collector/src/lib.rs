pub mod agent;
pub mod collector;
pub mod config;
pub mod observability;
pub mod recovery;

pub use agent::*;
pub use collector::*;
pub use config::*;
pub use observability::*;
pub use recovery::*;

use anyhow::Result;
use sinex_db::{models::RawEvent, validation::EventValidator};
use sqlx::PgPool;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info};

/// Output configuration for collector
#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub to_database: bool,
    pub to_stdout: bool, 
    pub to_file: Option<String>,
    pub dry_run: bool,
}

impl OutputConfig {
    pub fn new(to_database: bool, to_stdout: bool, to_file: Option<String>, dry_run: bool) -> Self {
        Self {
            to_database,
            to_stdout,
            to_file,
            dry_run,
        }
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            to_database: true,
            to_stdout: false,
            to_file: None,
            dry_run: false,
        }
    }
}

/// Output an event to the configured destinations
pub async fn output_event(
    event: &RawEvent,
    output_config: &OutputConfig,
    db_pool: Option<&PgPool>,
    validator: Option<&EventValidator>,
    file_handle: &mut Option<File>,
) -> Result<()> {
    if output_config.dry_run {
        info!("DRY RUN - Event: {} {} {}", 
              event.source, event.event_type, event.id);
        return Ok(());
    }

    // Output to stdout
    if output_config.to_stdout {
        let json = serde_json::to_string_pretty(&event)?;
        println!("{}", json);
    }

    // Output to file
    if let Some(file_path) = &output_config.to_file {
        if file_handle.is_none() {
            *file_handle = Some(File::create(file_path).await?);
        }
        
        if let Some(file) = file_handle {
            let json = serde_json::to_string(&event)?;
            file.write_all(json.as_bytes()).await?;
            file.write_all(b"\n").await?;
            file.flush().await?;
        }
    }

    // Output to database
    if output_config.to_database {
        if let Some(pool) = db_pool {
            // Validate event if validator is available
            if let Some(validator) = validator {
                if let Err(e) = validator.validate(event) {
                    error!("Event validation failed: {}", e);
                    return Err(e.into());
                }
            }
            
            // Insert into database
            match sinex_db::queries::insert_event_with_validator(pool, event, None).await {
                Ok(_) => {
                    debug!("Inserted event: {} {}", event.source, event.event_type);
                }
                Err(e) => {
                    error!("Failed to insert event: {}", e);
                    return Err(e);
                }
            }
        } else {
            error!("Database output requested but no pool provided");
        }
    }

    Ok(())
}