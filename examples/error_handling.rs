//! Error Handling Examples
//! 
//! This file demonstrates proper usage of CoreError from sinex-error
//! instead of anyhow or raw error handling.

use sinex_error::{CoreError, ErrorContext, Result};
use sinex_events::{RawEvent, EventFactory};
use std::fs;
use std::path::Path;
use sinex_ulid::Ulid;
use serde_json::Value;

/// Example 1: Basic error creation and context
fn process_event(event: &RawEvent) -> Result<()> {
    // ❌ WRONG: Using anyhow
    // if event.payload.is_null() {
    //     return Err(anyhow!("Event payload is null"));
    // }

    // ✅ CORRECT: Using specific CoreError variant
    if event.payload.is_null() {
        return Err(CoreError::Validation {
            field: "payload".to_string(),
            reason: "Event payload cannot be null".to_string(),
        });
    }

    // Process the event...
    Ok(())
}

/// Example 2: Converting from other error types with context
async fn read_config_file(path: &Path) -> Result<String> {
    // ❌ WRONG: Using unwrap or expect
    // let content = fs::read_to_string(path).expect("Failed to read config");

    // ❌ WRONG: Using anyhow with context
    // let content = fs::read_to_string(path)
    //     .with_context(|| format!("Failed to read config from {:?}", path))?;

    // ✅ CORRECT: Using ErrorContext trait
    let content = fs::read_to_string(path)
        .context(CoreError::Configuration {
            message: format!("Failed to read config from {:?}", path),
        })?;

    Ok(content)
}

/// Example 3: Creating rich error context
fn validate_event_payload(event_id: Ulid, payload: &Value) -> Result<()> {
    // ❌ WRONG: Generic error message
    // if !payload.is_object() {
    //     return Err(anyhow!("Invalid payload"));
    // }

    // ✅ CORRECT: Specific error with full context
    if !payload.is_object() {
        return Err(CoreError::Validation {
            field: "payload".to_string(),
            reason: format!(
                "Event {} payload must be an object, got {}",
                event_id,
                payload_type_name(payload)
            ),
        });
    }

    // Validate required fields
    let obj = payload.as_object().unwrap();
    
    for required_field in &["timestamp", "source", "data"] {
        if !obj.contains_key(*required_field) {
            return Err(CoreError::Validation {
                field: format!("payload.{}", required_field),
                reason: format!("Required field '{}' is missing", required_field),
            });
        }
    }

    Ok(())
}

/// Example 4: Error propagation with additional context
async fn process_event_pipeline(event: Event) -> Result<ProcessedEvent> {
    // Step 1: Validate
    validate_event(&event)
        .context(CoreError::Processing {
            event_id: event.id,
            reason: "Validation failed".to_string(),
        })?;

    // Step 2: Transform
    let transformed = transform_event(&event)
        .context(CoreError::Processing {
            event_id: event.id,
            reason: "Transformation failed".to_string(),
        })?;

    // Step 3: Persist
    persist_event(&transformed)
        .await
        .context(CoreError::Database {
            operation: format!("persist_event({})", event.id),
        })?;

    Ok(ProcessedEvent {
        original_id: event.id,
        processed_at: chrono::Utc::now(),
    })
}

/// Example 5: Custom error handling for specific domains
mod satellite_errors {
    use super::*;

    pub fn connection_failed(satellite_name: &str, addr: &str) -> CoreError {
        // ❌ WRONG: Generic error
        // anyhow!("Connection failed")

        // ✅ CORRECT: Domain-specific error
        CoreError::Connection {
            service: satellite_name.to_string(),
            reason: format!("Failed to connect to {}", addr),
        }
    }

    pub fn invalid_checkpoint(automaton: &str, checkpoint_id: Option<Ulid>) -> CoreError {
        // ✅ CORRECT: Rich error information
        match checkpoint_id {
            Some(id) => CoreError::InvalidState {
                entity: "checkpoint".to_string(),
                state: format!("Checkpoint {} for automaton {} is corrupted", id, automaton),
            },
            None => CoreError::NotFound {
                entity: format!("checkpoint for automaton {}", automaton),
            },
        }
    }
}

/// Example 6: Error recovery and fallback
async fn get_event_with_fallback(
    primary_pool: &sqlx::PgPool,
    fallback_pool: &sqlx::PgPool,
    event_id: Ulid,
) -> Result<Event> {
    // Try primary first
    match get_event_from_pool(primary_pool, event_id).await {
        Ok(event) => Ok(event),
        Err(e) => {
            // Log the primary failure
            tracing::warn!("Primary pool failed: {:?}", e);

            // Try fallback
            get_event_from_pool(fallback_pool, event_id)
                .await
                .context(CoreError::Database {
                    operation: format!(
                        "get_event({}) failed on both primary and fallback",
                        event_id
                    ),
                })
        }
    }
}

/// Example 7: Batch error handling
fn process_events_batch(events: Vec<Event>) -> Result<BatchResult> {
    let mut successes = Vec::new();
    let mut failures = Vec::new();

    for event in events {
        match process_single_event(&event) {
            Ok(result) => successes.push((event.id, result)),
            Err(e) => {
                // ✅ CORRECT: Collect errors with context
                failures.push((
                    event.id,
                    CoreError::Processing {
                        event_id: event.id,
                        reason: format!("Batch processing failed: {:?}", e),
                    },
                ));
            }
        }
    }

    // Return partial success
    if failures.is_empty() {
        Ok(BatchResult {
            processed: successes.len(),
            failed: 0,
            results: successes,
            errors: vec![],
        })
    } else if successes.is_empty() {
        // All failed
        Err(CoreError::BatchOperation {
            succeeded: 0,
            failed: failures.len(),
            first_error: Box::new(failures[0].1.clone()),
        })
    } else {
        // Partial success
        Ok(BatchResult {
            processed: successes.len(),
            failed: failures.len(),
            results: successes,
            errors: failures,
        })
    }
}

/// Example 8: Validation chain with proper errors
use sinex_validation::ValidationChain;

fn validate_configuration(config: &ConfigData) -> Result<()> {
    // ❌ WRONG: Manual validation with generic errors
    // if config.port < 1024 {
    //     return Err(anyhow!("Port must be >= 1024"));
    // }
    // if config.name.is_empty() {
    //     return Err(anyhow!("Name cannot be empty"));
    // }

    // ✅ CORRECT: Using ValidationChain
    ValidationChain::validate(&config.name, "name")
        .not_empty()
        .min_length(3)
        .max_length(50)
        .into_result()?;

    ValidationChain::validate(&config.port, "port")
        .min(1024)
        .max(65535)
        .into_result()?;

    ValidationChain::validate(&config.endpoint, "endpoint")
        .not_empty()
        .matches_regex(r"^https?://")
        .into_result()?;

    Ok(())
}

// Helper types for examples
struct ProcessedEvent {
    original_id: Ulid,
    processed_at: chrono::DateTime<chrono::Utc>,
}

struct BatchResult {
    processed: usize,
    failed: usize,
    results: Vec<(Ulid, ProcessedEvent)>,
    errors: Vec<(Ulid, CoreError)>,
}

struct ConfigData {
    name: String,
    port: u16,
    endpoint: String,
}

// Helper functions (stubs for example)
fn validate_event(_event: &Event) -> Result<()> {
    Ok(())
}

fn transform_event(_event: &Event) -> Result<Event> {
    Ok(_event.clone())
}

async fn persist_event(_event: &Event) -> Result<()> {
    Ok(())
}

fn process_single_event(_event: &Event) -> Result<ProcessedEvent> {
    Ok(ProcessedEvent {
        original_id: _event.id,
        processed_at: chrono::Utc::now(),
    })
}

async fn get_event_from_pool(_pool: &sqlx::PgPool, _id: Ulid) -> Result<Event> {
    Err(CoreError::NotFound {
        entity: "event".to_string(),
    })
}

fn payload_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn main() {
    println!("This is an example file demonstrating error handling patterns.");
    println!("See the individual functions for usage examples.");
}
