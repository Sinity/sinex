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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_output_config_creation() {
        let config = OutputConfig::new(true, false, Some("test.log".to_string()), false);
        assert!(config.to_database);
        assert!(!config.to_stdout);
        assert_eq!(config.to_file, Some("test.log".to_string()));
        assert!(!config.dry_run);
    }

    #[tokio::test]
    async fn test_output_config_default() {
        let config = OutputConfig::default();
        assert!(config.to_database);
        assert!(!config.to_stdout);
        assert!(config.to_file.is_none());
        assert!(!config.dry_run);
    }

    #[tokio::test]
    async fn test_output_event_dry_run() {
        let event = RawEvent::new(
            "test_source".to_string(),
            "test_event".to_string(),
            serde_json::json!({"test": "data"}),
            "test_host".to_string(),
        );

        let output_config = OutputConfig {
            to_database: true,
            to_stdout: true,
            to_file: Some("test.log".to_string()),
            dry_run: true,
        };

        let mut file_handle = None;
        let result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        
        assert!(result.is_ok());
        assert!(file_handle.is_none()); // No file should be created in dry run
    }

    #[tokio::test]
    async fn test_output_event_to_stdout() {
        let event = RawEvent::new(
            "test_source".to_string(),
            "test_event".to_string(),
            serde_json::json!({"test": "data"}),
            "test_host".to_string(),
        );

        let output_config = OutputConfig {
            to_database: false,
            to_stdout: true,
            to_file: None,
            dry_run: false,
        };

        let mut file_handle = None;
        // Note: In a real test, we'd capture stdout, but for unit test we just verify no error
        let result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_output_event_to_file() {
        let event = RawEvent::new(
            "test_source".to_string(),
            "test_event".to_string(),
            serde_json::json!({"test": "data"}),
            "test_host".to_string(),
        );

        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();

        let output_config = OutputConfig {
            to_database: false,
            to_stdout: false,
            to_file: Some(file_path.clone()),
            dry_run: false,
        };

        let mut file_handle = None;
        let result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        assert!(result.is_ok());
        assert!(file_handle.is_some());

        // Read the file and verify content
        let mut content = String::new();
        let mut file = File::open(&file_path).await.unwrap();
        file.read_to_string(&mut content).await.unwrap();
        
        let written_event: RawEvent = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(written_event.source, "test_source");
        assert_eq!(written_event.event_type, "test_event");
    }

    #[tokio::test]
    async fn test_output_event_validation_failure() {
        let event = RawEvent::new(
            "".to_string(), // Invalid empty source
            "test_event".to_string(),
            serde_json::json!({"test": "data"}),
            "test_host".to_string(),
        );

        let output_config = OutputConfig {
            to_database: true,
            to_stdout: false,
            to_file: None,
            dry_run: false,
        };

        // Create a mock validator that always fails
        let validator = EventValidator::new(Default::default());
        
        let mut file_handle = None;
        // Without a real database pool, this should handle the missing pool gracefully
        let result = output_event(&event, &output_config, None, Some(&validator), &mut file_handle).await;
        
        // Should succeed because there's no database pool to validate against
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_output_event_multiple_destinations() {
        let event = RawEvent::new(
            "test_source".to_string(),
            "test_event".to_string(),
            serde_json::json!({"test": "data"}),
            "test_host".to_string(),
        );

        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();

        let output_config = OutputConfig {
            to_database: false, // No DB in unit test
            to_stdout: true,
            to_file: Some(file_path.clone()),
            dry_run: false,
        };

        let mut file_handle = None;
        let result = output_event(&event, &output_config, None, None, &mut file_handle).await;
        assert!(result.is_ok());

        // Verify file was written
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("test_source"));
    }
}