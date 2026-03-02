//! JSON parsing and accessor helpers
//!
//! This module provides utilities for working with JSON:
//! - **Parsing functions**: Type-safe parsing with error context and validation
//! - **Accessor functions**: Simple field accessors with fallback values for display
//!
//! # Accessor Functions
//!
//! Use these for safely extracting values from JSON for display purposes:
//!
//! ```rust
//! use serde_json::json;
//! use sinex_primitives::utils::json_helpers::{get_str, get_i64, get_bool};
//!
//! let obj = json!({"name": "test", "count": 42, "enabled": true});
//! assert_eq!(get_str(&obj, "name"), "test");
//! assert_eq!(get_str(&obj, "missing"), "N/A");  // Safe fallback
//! assert_eq!(get_i64(&obj, "count"), 42);
//! assert_eq!(get_i64(&obj, "missing"), 0);      // Safe fallback
//! ```
//!
//! # Parsing Functions
//!
//! Use these for strict parsing with proper error handling:
//!
//! ```rust,ignore
//! use sinex_primitives::utils::json_helpers::parse_json;
//!
//! let config: MyConfig = parse_json(json_str, "config", "load")?;
//! ```

use crate::error::{Result, SinexError};
use serde::de::DeserializeOwned;
use serde_json::Value;

// =============================================================================
// Simple Accessor Functions (for display with safe fallbacks)
// =============================================================================

/// Get a string value from a JSON object, returning "N/A" if not found or not a string.
///
/// Use this for display purposes where missing values should show a placeholder.
#[must_use]
pub fn get_str<'a>(obj: &'a Value, key: &str) -> &'a str {
    obj.get(key).and_then(|v| v.as_str()).unwrap_or("N/A")
}

/// Get an owned string value from a JSON object.
///
/// Convenience wrapper around `get_str` that returns an owned String.
#[must_use]
pub fn get_string(obj: &Value, key: &str) -> String {
    get_str(obj, key).to_string()
}

/// Get an optional string value from a JSON object.
///
/// Returns `None` if the key doesn't exist or the value isn't a string.
#[must_use]
pub fn get_optional_str<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

/// Get an i64 value from a JSON object, returning 0 if not found or not a number.
#[must_use]
pub fn get_i64(obj: &Value, key: &str) -> i64 {
    obj.get(key)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
}

/// Get a u64 value from a JSON object, returning 0 if not found or not a number.
#[must_use]
pub fn get_u64(obj: &Value, key: &str) -> u64 {
    obj.get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

/// Get a boolean value from a JSON object, returning false if not found or not a boolean.
#[must_use]
pub fn get_bool(obj: &Value, key: &str) -> bool {
    obj.get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Get a nested object from a JSON value, returning None if not found or not an object.
#[must_use]
pub fn get_object<'a>(obj: &'a Value, key: &str) -> Option<&'a Value> {
    obj.get(key).filter(|v| v.is_object())
}

/// Get an array from a JSON value, returning None if not found or not an array.
#[must_use]
pub fn get_array<'a>(obj: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    obj.get(key).and_then(|v| v.as_array())
}

// =============================================================================
// Type-Safe Parsing Functions (with error context and validation)
// =============================================================================

/// Parse JSON from a string with error context and validation.
///
/// Validates the JSON structure first, then deserializes to the target type.
/// Returns errors with full context including the JSON length and operation details.
pub fn parse_json<T: DeserializeOwned>(
    json_str: &str,
    context_type: &str,
    operation: &str,
) -> Result<T> {
    // First validate the JSON structure
    let validated_value = crate::validation::validate_json(json_str).map_err(|e| {
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

/// Parse JSON from a string with file path context and validation.
///
/// Similar to `parse_json()` but includes the file path in error messages for better diagnostics.
pub fn parse_json_file<T: DeserializeOwned>(
    json_str: &str,
    file_path: impl AsRef<camino::Utf8Path>,
    operation: &str,
) -> Result<T> {
    // First validate the JSON structure
    let validated_value = crate::validation::validate_json(json_str).map_err(|e| {
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

/// Parse JSON Value from a string with error context and validation.
///
/// Validates and parses a JSON string, returning a `serde_json::Value`.
pub fn parse_json_value(json_str: &str, context_type: &str, operation: &str) -> Result<Value> {
    // Use sinex_types to parse and validate in one step
    crate::validation::validate_json(json_str).map_err(|e| {
        SinexError::validation(format!(
            "Invalid JSON structure for {context_type} (operation: {operation}): {e}"
        ))
    })
}

/// Safely extract and deserialize a field from a JSON Value.
///
/// Extracts the named field from the JSON object and deserializes it to the target type.
/// Returns detailed error context if the field is missing or deserialization fails.
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

/// Convert a Rust value to a JSON Value with error context.
///
/// Serializes the value to JSON. Returns detailed error context if serialization fails.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_get_str() -> TestResult<()> {
        let obj = json!({
            "name": "test",
            "number": 42,
            "null": null
        });

        assert_eq!(get_str(&obj, "name"), "test");
        assert_eq!(get_str(&obj, "missing"), "N/A");
        assert_eq!(get_str(&obj, "number"), "N/A"); // Not a string
        assert_eq!(get_str(&obj, "null"), "N/A"); // Null value
        Ok(())
    }

    #[sinex_test]
    async fn test_get_string() -> TestResult<()> {
        let obj = json!({
            "name": "test"
        });

        assert_eq!(get_string(&obj, "name"), "test");
        assert_eq!(get_string(&obj, "missing"), "N/A");
        Ok(())
    }

    #[sinex_test]
    async fn test_get_optional_str() -> TestResult<()> {
        let obj = json!({
            "name": "test",
            "number": 42
        });

        assert_eq!(get_optional_str(&obj, "name"), Some("test"));
        assert_eq!(get_optional_str(&obj, "missing"), None);
        assert_eq!(get_optional_str(&obj, "number"), None);
        Ok(())
    }

    #[sinex_test]
    async fn test_get_i64() -> TestResult<()> {
        let obj = json!({
            "count": 42,
            "string": "not a number",
            "float": 1.23
        });

        assert_eq!(get_i64(&obj, "count"), 42);
        assert_eq!(get_i64(&obj, "missing"), 0);
        assert_eq!(get_i64(&obj, "string"), 0);
        assert_eq!(get_i64(&obj, "float"), 0); // f64 not convertible to i64
        Ok(())
    }

    #[sinex_test]
    async fn test_get_u64() -> TestResult<()> {
        let obj = json!({
            "count": 42,
            "negative": -5
        });

        assert_eq!(get_u64(&obj, "count"), 42);
        assert_eq!(get_u64(&obj, "missing"), 0);
        assert_eq!(get_u64(&obj, "negative"), 0); // Can't convert negative to u64
        Ok(())
    }

    #[sinex_test]
    async fn test_get_bool() -> TestResult<()> {
        let obj = json!({
            "enabled": true,
            "disabled": false,
            "string": "true"
        });

        assert!(get_bool(&obj, "enabled"));
        assert!(!get_bool(&obj, "disabled"));
        assert!(!get_bool(&obj, "missing"));
        assert!(!get_bool(&obj, "string")); // Not a bool
        Ok(())
    }

    #[sinex_test]
    async fn test_get_object() -> TestResult<()> {
        let obj = json!({
            "nested": {
                "key": "value"
            },
            "array": [],
            "string": "not an object"
        });

        assert!(get_object(&obj, "nested").is_some());
        assert!(get_object(&obj, "missing").is_none());
        assert!(get_object(&obj, "array").is_none());
        assert!(get_object(&obj, "string").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_get_array() -> TestResult<()> {
        let obj = json!({
            "items": [1, 2, 3],
            "object": {},
            "string": "not an array"
        });

        assert_eq!(get_array(&obj, "items").map(std::vec::Vec::len), Some(3));
        assert!(get_array(&obj, "missing").is_none());
        assert!(get_array(&obj, "object").is_none());
        assert!(get_array(&obj, "string").is_none());
        Ok(())
    }
}
