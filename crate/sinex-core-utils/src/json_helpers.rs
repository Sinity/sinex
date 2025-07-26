//! JSON parsing helpers for consistent error handling
//!
//! This module provides utilities for parsing JSON with consistent
//! error context and validation.

use serde::de::DeserializeOwned;
use serde_json::Value;
use sinex_core_types::{CoreError, Result};

/// Parse JSON from a string with error context
pub fn parse_json<T: DeserializeOwned>(
    json_str: &str,
    context_type: &str,
    operation: &str,
) -> Result<T> {
    serde_json::from_str(json_str).map_err(|e| {
        CoreError::Serialization(format!(
            "Failed to parse {} (operation: {}, json_length: {}): {}",
            context_type,
            operation,
            json_str.len(),
            e
        ))
    })
}

/// Parse JSON from a string with file path context
pub fn parse_json_file<T: DeserializeOwned>(
    json_str: &str,
    file_path: impl AsRef<std::path::Path>,
    operation: &str,
) -> Result<T> {
    serde_json::from_str(json_str).map_err(|e| {
        CoreError::Serialization(format!(
            "Failed to parse JSON file {} (operation: {}, json_length: {}): {}",
            file_path.as_ref().display(),
            operation,
            json_str.len(),
            e
        ))
    })
}

/// Parse JSON Value from a string with error context
pub fn parse_json_value(json_str: &str, context_type: &str, operation: &str) -> Result<Value> {
    serde_json::from_str(json_str).map_err(|e| {
        CoreError::Serialization(format!(
            "Failed to parse {} as JSON (operation: {}, preview: {}): {}",
            context_type,
            operation,
            json_str.chars().take(100).collect::<String>(),
            e
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
        CoreError::Validation(format!(
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
        CoreError::Serialization(format!(
            "Failed to deserialize field: {} (operation: {}): {}",
            field_name, operation, e
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
        CoreError::Serialization(format!(
            "Failed to serialize {} (operation: {}): {}",
            context_type, operation, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        value: i32,
    }

    #[test]
    fn test_parse_json() {
        let json = r#"{"name": "test", "value": 42}"#;
        let result: TestStruct = parse_json(json, "test struct", "test_operation").unwrap();
        assert_eq!(result.name, "test");
        assert_eq!(result.value, 42);

        // Test error case
        let bad_json = r#"{"invalid": json}"#;
        let result: Result<TestStruct> = parse_json(bad_json, "test struct", "test_operation");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_field() {
        let json_value = serde_json::json!({
            "name": "test",
            "value": 42,
            "nested": {
                "field": "data"
            }
        });

        let name: String = extract_field(&json_value, "name", "test_op").unwrap();
        assert_eq!(name, "test");

        let value: i32 = extract_field(&json_value, "value", "test_op").unwrap();
        assert_eq!(value, 42);

        // Test missing field
        let result: Result<String> = extract_field(&json_value, "missing", "test_op");
        assert!(result.is_err());
    }
}
