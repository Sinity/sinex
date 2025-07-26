//! Error Handling Examples
//!
//! This file demonstrates proper usage of CoreError from sinex-error
//! instead of anyhow or raw error handling.

use serde_json::Value;
use sinex_error::{CoreError, ErrorContext, Result, ResultExt};
use sinex_events::{EventFactory, RawEvent};
use sinex_ulid::Ulid;
use std::fs;
use std::path::Path;

/// Example 1: Basic error creation and context
fn process_event(event: &RawEvent) -> Result<()> {
    // ✅ CORRECT: Using specific CoreError variant
    if event.payload.is_null() {
        return Err(CoreError::Validation(
            "payload: Event payload cannot be null".to_string(),
        ));
    }

    // Process the event...
    Ok(())
}

/// Example 2: Converting from other error types with context
async fn read_config_file(path: &Path) -> Result<String> {
    // ✅ CORRECT: Using ResultExt trait for context
    let content = fs::read_to_string(path)
        .map_err(|e| CoreError::Io(format!("Failed to read config from {:?}: {}", path, e)))?;

    Ok(content)
}

/// Example 3: Creating rich error context
fn validate_event_payload(event_id: Ulid, payload: &Value) -> Result<()> {
    // ✅ CORRECT: Specific error with full context
    if !payload.is_object() {
        return Err(CoreError::Validation(format!(
            "Event {} payload must be an object, got {}",
            event_id,
            payload_type_name(payload)
        )));
    }

    // Validate required fields
    let obj = payload.as_object().unwrap();

    for required_field in &["timestamp", "source", "data"] {
        if !obj.contains_key(*required_field) {
            return Err(CoreError::Validation(format!(
                "payload.{}: Required field '{}' is missing",
                required_field, required_field
            )));
        }
    }

    Ok(())
}

/// Example 4: Error propagation with additional context
async fn process_event_pipeline(event: RawEvent) -> Result<ProcessedEvent> {
    // Step 1: Validate
    validate_event(&event).map_err(|e| {
        CoreError::Unknown(format!(
            "Processing event {}: validation failed: {:?}",
            event.id, e
        ))
    })?;

    // Step 2: Transform
    let transformed = transform_event(&event).map_err(|e| {
        CoreError::Unknown(format!(
            "Processing event {}: transformation failed: {:?}",
            event.id, e
        ))
    })?;

    // Step 3: Persist
    persist_event(&transformed)
        .await
        .map_err(|e| CoreError::Database(format!("persist_event({}): {:?}", event.id, e)))?;

    Ok(ProcessedEvent {
        original_id: event.id,
        processed_at: chrono::Utc::now(),
    })
}

/// Example 5: Custom error handling for specific domains
mod satellite_errors {
    use super::*;

    pub fn connection_failed(satellite_name: &str, addr: &str) -> CoreError {
        // ✅ CORRECT: Domain-specific error
        CoreError::Network(format!(
            "Service {} failed to connect to {}",
            satellite_name, addr
        ))
    }

    pub fn invalid_checkpoint(automaton: &str, checkpoint_id: Option<Ulid>) -> CoreError {
        // ✅ CORRECT: Rich error information
        match checkpoint_id {
            Some(id) => CoreError::InvalidState(format!(
                "Checkpoint {} for automaton {} is corrupted",
                id, automaton
            )),
            None => CoreError::NotFound(format!("checkpoint for automaton {}", automaton)),
        }
    }
}

/// Example 6: Error recovery and fallback
async fn get_event_with_fallback(
    primary_pool: &sqlx::PgPool,
    fallback_pool: &sqlx::PgPool,
    event_id: Ulid,
) -> Result<RawEvent> {
    // Try primary first
    match get_event_from_pool(primary_pool, event_id).await {
        Ok(event) => Ok(event),
        Err(e) => {
            // Log the primary failure
            tracing::warn!("Primary pool failed: {:?}", e);

            // Try fallback
            get_event_from_pool(fallback_pool, event_id)
                .await
                .map_err(|_| {
                    CoreError::Database(format!(
                        "get_event({}) failed on both primary and fallback",
                        event_id
                    ))
                })
        }
    }
}

/// Example 7: Batch error handling
fn process_events_batch(events: Vec<RawEvent>) -> Result<BatchResult> {
    let mut successes = Vec::new();
    let mut failures = Vec::new();

    for event in events {
        match process_single_event(&event) {
            Ok(result) => successes.push((event.id, result)),
            Err(e) => {
                // ✅ CORRECT: Collect errors with context
                failures.push((
                    event.id,
                    CoreError::Unknown(format!(
                        "Processing event {}: Batch processing failed: {:?}",
                        event.id, e
                    )),
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
        Err(CoreError::Unknown(format!(
            "Batch operation failed: 0/{} succeeded",
            failures.len()
        )))
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
    // ✅ CORRECT: Using ValidationChain
    ValidationChain::validate(config.name.clone(), "name")
        .not_empty()
        .min_length(3)
        .max_length(50)
        .into_result()
        .map_err(|e| CoreError::Validation(format!("Invalid name: {:?}", e)))?;

    ValidationChain::validate(&config.port, "port")
        .min(&1024)
        .max(&65535)
        .into_result()
        .map_err(|e| CoreError::Validation(format!("Invalid port: {:?}", e)))?;

    ValidationChain::validate(config.endpoint.clone(), "endpoint")
        .not_empty()
        .into_result()
        .map_err(|e| CoreError::Validation(format!("Invalid endpoint: {:?}", e)))?;

    // Additional validation for URL format
    if !config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://") {
        return Err(CoreError::Validation(
            "Endpoint must start with http:// or https://".to_string(),
        ));
    }

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
fn validate_event(_event: &RawEvent) -> Result<()> {
    Ok(())
}

fn transform_event(_event: &RawEvent) -> Result<RawEvent> {
    Ok(_event.clone())
}

async fn persist_event(_event: &RawEvent) -> Result<()> {
    Ok(())
}

fn process_single_event(_event: &RawEvent) -> Result<ProcessedEvent> {
    Ok(ProcessedEvent {
        original_id: _event.id,
        processed_at: chrono::Utc::now(),
    })
}

async fn get_event_from_pool(_pool: &sqlx::PgPool, _id: Ulid) -> Result<RawEvent> {
    Err(CoreError::NotFound("event".to_string()))
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
