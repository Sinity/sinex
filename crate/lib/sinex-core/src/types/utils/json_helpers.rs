//! JSON parsing helpers for consistent error handling
//!
//! This module provides utilities for parsing JSON with consistent
//! error context and validation.

use crate::error::{Result, SinexError};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Parse JSON from a string with error context and validation
pub fn parse_json<T: DeserializeOwned>(
    json_str: &str,
    context_type: &str,
    operation: &str,
) -> Result<T> {
    // First validate the JSON structure
    let validated_value = crate::validate_json(json_str).map_err(|e| {
        SinexError::validation(format!(
            "Invalid JSON structure for {context_type} (operation: {operation}): {e}"
        ))
    })?;

    // Then deserialize with error context
    serde_json::from_value(validated_value).map_err(|e| {
        SinexError::serialization(format!(
            "Failed to parse {} (operation: {}, json_length: {}): {}",
            context_type,
            operation,
            json_str.len(),
            e
        ))
    })
}

/// Parse JSON from a string with file path context and validation
pub fn parse_json_file<T: DeserializeOwned>(
    json_str: &str,
    file_path: impl AsRef<camino::Utf8Path>,
    operation: &str,
) -> Result<T> {
    // First validate the JSON structure
    let validated_value = crate::validate_json(json_str).map_err(|e| {
        SinexError::validation(format!(
            "Invalid JSON structure in file {} (operation: {}): {}",
            file_path.as_ref().as_str(),
            operation,
            e
        ))
    })?;

    // Then deserialize with error context
    serde_json::from_value(validated_value).map_err(|e| {
        SinexError::serialization(format!(
            "Failed to parse JSON file {} (operation: {}, json_length: {}): {}",
            file_path.as_ref().as_str(),
            operation,
            json_str.len(),
            e
        ))
    })
}

/// Parse JSON Value from a string with error context and validation
pub fn parse_json_value(json_str: &str, context_type: &str, operation: &str) -> Result<Value> {
    // Use sinex_types to parse and validate in one step
    crate::validate_json(json_str).map_err(|e| {
        SinexError::validation(format!(
            "Invalid JSON structure for {context_type} (operation: {operation}): {e}"
        ))
    })
}

/// Safely extract a field from a JSON Value
pub fn extract_field<T: DeserializeOwned>(
    value: &Value,
    field_name: &str,
    operation: &str,
) -> Result<T> {
    let field_value = value.get(field_name).ok_or_else(|| {
        SinexError::validation(format!(
            "Missing field: {} (operation: {}, available_fields: {:?})",
            field_name,
            operation,
            value
                .as_object()
                .map(|o| o.keys().collect::<Vec<_>>())
                .unwrap_or_default()
        ))
    })?;

    serde_json::from_value(field_value.clone()).map_err(|e| {
        SinexError::serialization(format!(
            "Failed to deserialize field: {field_name} (operation: {operation}): {e}"
        ))
    })
}

/// Convert a value to JSON with error context
pub fn to_json_value<T: serde::Serialize>(
    value: &T,
    context_type: &str,
    operation: &str,
) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| {
        SinexError::serialization(format!(
            "Failed to serialize {context_type} (operation: {operation}): {e}"
        ))
    })
}
