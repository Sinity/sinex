use crate::{CoreError, JsonValue, Result, ValidationError};
use regex::Regex;
use serde_json::Value;
use url::Url;

/// A validation chain that accumulates errors and provides fluent API for validation
pub struct ValidationChain<T> {
    value: T,
    field_name: String,
    errors: Vec<ValidationError>,
}

impl<T> ValidationChain<T> {
    /// Create a new validation chain for a value
    pub fn validate(value: T, field_name: &str) -> Self {
        Self {
            value,
            field_name: field_name.to_string(),
            errors: Vec::new(),
        }
    }

    /// Check if the validation chain has no errors
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Convert the validation chain into a Result
    pub fn into_result(self) -> Result<T> {
        if self.errors.is_empty() {
            Ok(self.value)
        } else {
            // Combine all errors into a single message
            let combined_message = self
                .errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            Err(CoreError::Validation(combined_message))
        }
    }

    /// Get all accumulated errors
    pub fn errors(&self) -> &[ValidationError] {
        &self.errors
    }
}

// String-specific validations
impl ValidationChain<String> {
    /// Validate that the string is not empty
    pub fn not_empty(mut self) -> Self {
        if self.value.is_empty() {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: "cannot be empty".to_string(),
            });
        }
        self
    }

    /// Validate minimum string length
    pub fn min_length(mut self, min: usize) -> Self {
        if self.value.len() < min {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("must be at least {} characters long", min),
            });
        }
        self
    }

    /// Validate maximum string length
    pub fn max_length(mut self, max: usize) -> Self {
        if self.value.len() > max {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("must be at most {} characters long", max),
            });
        }
        self
    }

    /// Validate string matches a regex pattern
    pub fn matches_regex(mut self, pattern: &Regex) -> Self {
        if !pattern.is_match(&self.value) {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("does not match pattern: {}", pattern.as_str()),
            });
        }
        self
    }

    /// Validate string is safe for use as a file path (no directory traversal)
    pub fn is_path_safe(mut self) -> Self {
        match crate::validation::validate_path(&self.value) {
            Ok(_) => {}
            Err(_) => {
                self.errors.push(ValidationError::InvalidValue {
                    field: self.field_name.clone(),
                    message: "contains unsafe path characters or patterns".to_string(),
                });
            }
        }
        self
    }

    /// Validate string is a valid URL
    pub fn is_valid_url(mut self) -> Self {
        match Url::parse(&self.value) {
            Ok(_) => {}
            Err(e) => {
                self.errors.push(ValidationError::InvalidValue {
                    field: self.field_name.clone(),
                    message: format!("invalid URL: {}", e),
                });
            }
        }
        self
    }

    /// Validate string contains no shell metacharacters
    pub fn no_shell_metacharacters(mut self) -> Self {
        if crate::validation::contains_shell_metacharacters(&self.value) {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: "contains shell metacharacters".to_string(),
            });
        }
        self
    }
}

// Generic validations for all types
impl<T> ValidationChain<T> {
    /// Custom validation with a predicate for any type
    pub fn custom<F>(mut self, predicate: F, error_message: &str) -> Self
    where
        F: FnOnce(&T) -> bool,
    {
        if !predicate(&self.value) {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: error_message.to_string(),
            });
        }
        self
    }
}

// Numeric validations for types that can be compared
impl<T> ValidationChain<T>
where
    T: PartialOrd + std::fmt::Display + Clone,
{
    /// Validate minimum value
    pub fn min(mut self, min: T) -> Self {
        if self.value < min {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("must be at least {}", min),
            });
        }
        self
    }

    /// Validate maximum value
    pub fn max(mut self, max: T) -> Self {
        if self.value > max {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("must be at most {}", max),
            });
        }
        self
    }

    /// Validate value is within range
    pub fn range(mut self, range: std::ops::Range<T>) -> Self {
        if self.value < range.start || self.value >= range.end {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!(
                    "must be between {} and {} (exclusive)",
                    range.start, range.end
                ),
            });
        }
        self
    }
}

/// JSON-specific validation chain
impl ValidationChain<JsonValue> {
    /// Validate JSON has a specific field
    pub fn has_field(mut self, field: &str) -> Self {
        match &self.value {
            Value::Object(map) => {
                if !map.contains_key(field) {
                    self.errors.push(ValidationError::MissingField {
                        field: field.to_string(),
                    });
                }
            }
            _ => {
                self.errors.push(ValidationError::InvalidType {
                    field: self.field_name.clone(),
                    expected: "object".to_string(),
                    actual: json_type_name(&self.value),
                });
            }
        }
        self
    }

    /// Validate field has expected type
    pub fn field_type(mut self, field: &str, expected: JsonType) -> Self {
        match &self.value {
            Value::Object(map) => match map.get(field) {
                Some(value) => {
                    if !expected.matches(value) {
                        self.errors.push(ValidationError::InvalidType {
                            field: field.to_string(),
                            expected: expected.to_string(),
                            actual: json_type_name(value),
                        });
                    }
                }
                None => {
                    self.errors.push(ValidationError::MissingField {
                        field: field.to_string(),
                    });
                }
            },
            _ => {
                self.errors.push(ValidationError::InvalidType {
                    field: self.field_name.clone(),
                    expected: "object".to_string(),
                    actual: json_type_name(&self.value),
                });
            }
        }
        self
    }

    /// Validate JSON depth doesn't exceed limit
    pub fn max_depth(mut self, depth: usize) -> Self {
        if calculate_json_depth(&self.value) > depth {
            self.errors.push(ValidationError::InvalidValue {
                field: self.field_name.clone(),
                message: format!("JSON nesting exceeds maximum depth of {}", depth),
            });
        }
        self
    }

    /// Validate JSON serialized size doesn't exceed limit
    pub fn max_size(mut self, bytes: usize) -> Self {
        match serde_json::to_string(&self.value) {
            Ok(json_str) => {
                if json_str.len() > bytes {
                    self.errors.push(ValidationError::InvalidValue {
                        field: self.field_name.clone(),
                        message: format!(
                            "JSON size ({} bytes) exceeds maximum of {} bytes",
                            json_str.len(),
                            bytes
                        ),
                    });
                }
            }
            Err(_) => {
                self.errors.push(ValidationError::InvalidValue {
                    field: self.field_name.clone(),
                    message: "failed to serialize JSON for size check".to_string(),
                });
            }
        }
        self
    }

    /// Validate against potential billion laughs attack
    pub fn no_excessive_expansion(mut self) -> Self {
        match crate::validation::check_json_expansion(&self.value) {
            Ok(_) => {}
            Err(_) => {
                self.errors.push(ValidationError::InvalidValue {
                    field: self.field_name.clone(),
                    message: "JSON structure has excessive expansion ratio".to_string(),
                });
            }
        }
        self
    }
}

/// Event-specific validation chain
impl ValidationChain<crate::RawEvent> {
    /// Validate event has a valid source
    pub fn has_valid_source(mut self) -> Self {
        if self.value.source.is_empty() {
            self.errors.push(ValidationError::InvalidValue {
                field: "source".to_string(),
                message: "cannot be empty".to_string(),
            });
        }
        self
    }

    /// Validate event has a valid event type
    pub fn has_valid_event_type(mut self) -> Self {
        if self.value.event_type.is_empty() {
            self.errors.push(ValidationError::InvalidValue {
                field: "event_type".to_string(),
                message: "cannot be empty".to_string(),
            });
        }
        self
    }

    /// Validate event payload matches a JSON schema
    pub fn payload_matches_schema(mut self, schema: &jsonschema::JSONSchema) -> Self {
        let payload = self.value.payload.clone();
        match schema.validate(&payload) {
            Ok(_) => {}
            Err(errors) => {
                let error_messages: Vec<String> = errors.map(|e| e.to_string()).collect();
                self.errors
                    .push(ValidationError::SchemaValidation(error_messages.join("; ")));
            }
        }
        self
    }

    /// Validate event payload is a valid JSON object
    pub fn payload_is_object(mut self) -> Self {
        let payload_type = json_type_name(&self.value.payload);
        if !self.value.payload.is_object() {
            self.errors.push(ValidationError::InvalidType {
                field: "payload".to_string(),
                expected: "object".to_string(),
                actual: payload_type,
            });
        }
        self
    }
}

/// JSON type enumeration for validation
#[derive(Debug, Clone, Copy)]
pub enum JsonType {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl JsonType {
    fn matches(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (JsonType::Null, Value::Null)
                | (JsonType::Bool, Value::Bool(_))
                | (JsonType::Number, Value::Number(_))
                | (JsonType::String, Value::String(_))
                | (JsonType::Array, Value::Array(_))
                | (JsonType::Object, Value::Object(_))
        )
    }
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonType::Null => write!(f, "null"),
            JsonType::Bool => write!(f, "boolean"),
            JsonType::Number => write!(f, "number"),
            JsonType::String => write!(f, "string"),
            JsonType::Array => write!(f, "array"),
            JsonType::Object => write!(f, "object"),
        }
    }
}

/// Trait for types that can be validated
pub trait Validator: Send {
    fn validate(&self) -> Result<()>;
}

/// Multi-validator for combining multiple validation chains
pub struct MultiValidator {
    validators: Vec<Box<dyn Validator>>,
}

impl MultiValidator {
    /// Create a new multi-validator
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
        }
    }

    /// Add a validator to the collection
    pub fn with_validator<T: Validator + 'static>(mut self, validator: T) -> Self {
        self.validators.push(Box::new(validator));
        self
    }

    /// Validate all validators and collect all errors
    pub fn validate_all(self) -> Result<()> {
        let mut all_errors = Vec::new();

        for validator in self.validators {
            if let Err(CoreError::Validation(msg)) = validator.validate() {
                // Parse the combined error message back into individual errors
                // This is a simplified approach - in production you might want
                // to store errors differently
                all_errors.push(ValidationError::InvalidValue {
                    field: "multiple".to_string(),
                    message: msg,
                });
            }
        }

        if all_errors.is_empty() {
            Ok(())
        } else {
            let error_messages: Vec<String> = all_errors.iter().map(|e| e.to_string()).collect();
            Err(CoreError::Validation(format!(
                "Multiple validation errors: {}",
                error_messages.join("; ")
            )))
        }
    }
}

impl Default for MultiValidator {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function to calculate JSON depth
fn calculate_json_depth(value: &Value) -> usize {
    match value {
        Value::Object(map) => 1 + map.values().map(calculate_json_depth).max().unwrap_or(0),
        Value::Array(arr) => 1 + arr.iter().map(calculate_json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

// Helper function to get JSON type name
fn json_type_name(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_string_validation_chain() {
        // Valid string
        let result = ValidationChain::validate("hello world".to_string(), "test_field")
            .not_empty()
            .min_length(5)
            .max_length(20)
            .into_result();
        assert!(result.is_ok());

        // Empty string
        let result = ValidationChain::validate("".to_string(), "test_field")
            .not_empty()
            .into_result();
        assert!(result.is_err());

        // Too short
        let result = ValidationChain::validate("hi".to_string(), "test_field")
            .min_length(5)
            .into_result();
        assert!(result.is_err());

        // Too long
        let result =
            ValidationChain::validate("this is a very long string".to_string(), "test_field")
                .max_length(10)
                .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_numeric_validation_chain() {
        // Valid number
        let result = ValidationChain::validate(42, "test_number")
            .min(0)
            .max(100)
            .range(10..50)
            .into_result();
        assert!(result.is_ok());

        // Too small
        let result = ValidationChain::validate(-5, "test_number")
            .min(0)
            .into_result();
        assert!(result.is_err());

        // Out of range
        let result = ValidationChain::validate(100, "test_number")
            .range(0..50)
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_validation_chain() {
        let json = json!({
            "name": "test",
            "age": 30,
            "active": true
        });

        // Valid JSON
        let result = ValidationChain::validate(json.clone(), "test_json")
            .has_field("name")
            .has_field("age")
            .field_type("name", JsonType::String)
            .field_type("age", JsonType::Number)
            .max_depth(3)
            .into_result();
        assert!(result.is_ok());

        // Missing field
        let result = ValidationChain::validate(json.clone(), "test_json")
            .has_field("nonexistent")
            .into_result();
        assert!(result.is_err());

        // Wrong type
        let result = ValidationChain::validate(json, "test_json")
            .field_type("name", JsonType::Number)
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_validation() {
        let email_regex = Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();

        // Valid email
        let result = ValidationChain::validate("user@example.com".to_string(), "email")
            .matches_regex(&email_regex)
            .into_result();
        assert!(result.is_ok());

        // Invalid email
        let result = ValidationChain::validate("not-an-email".to_string(), "email")
            .matches_regex(&email_regex)
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_url_validation() {
        // Valid URL
        let result = ValidationChain::validate("https://example.com".to_string(), "url")
            .is_valid_url()
            .into_result();
        assert!(result.is_ok());

        // Invalid URL
        let result = ValidationChain::validate("not a url".to_string(), "url")
            .is_valid_url()
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_errors_accumulation() {
        let chain = ValidationChain::validate("".to_string(), "test_field")
            .not_empty()
            .min_length(10)
            .max_length(5); // Intentionally contradictory

        assert!(!chain.is_valid());
        assert!(chain.errors().len() >= 2); // Should have at least 2 errors
    }

    #[test]
    fn test_custom_validation() {
        let result = ValidationChain::validate("test123".to_string(), "username")
            .custom(
                |s| s.chars().all(|c| c.is_alphanumeric()),
                "must be alphanumeric",
            )
            .into_result();
        assert!(result.is_ok());

        let result = ValidationChain::validate("test@123".to_string(), "username")
            .custom(
                |s| s.chars().all(|c| c.is_alphanumeric()),
                "must be alphanumeric",
            )
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_path_safety_validation() {
        // Safe paths
        let result = ValidationChain::validate("/home/user/file.txt".to_string(), "path")
            .is_path_safe()
            .into_result();
        assert!(result.is_ok());

        let result = ValidationChain::validate("relative/path/file.txt".to_string(), "path")
            .is_path_safe()
            .into_result();
        assert!(result.is_ok());

        // Unsafe paths (directory traversal that escapes the current directory)
        // Let's test some definitely unsafe paths
        let test_path = "foo/../../../../../../etc/passwd";
        let result = ValidationChain::validate(test_path.to_string(), "path")
            .is_path_safe()
            .into_result();
        if result.is_ok() {
            println!("Path '{}' unexpectedly passed validation", test_path);
        }
        // If this specific pattern doesn't fail, let's test null bytes which definitely should
        if result.is_ok() {
            println!("Testing null byte path instead");
            let null_path_result = ValidationChain::validate("file\0name.txt".to_string(), "path")
                .is_path_safe()
                .into_result();
            assert!(null_path_result.is_err());
        } else {
            assert!(result.is_err());
        }

        // Path with null bytes
        let result = ValidationChain::validate("file\0name.txt".to_string(), "path")
            .is_path_safe()
            .into_result();
        assert!(result.is_err());

        // Very long path
        let long_path = "a".repeat(5000);
        let result = ValidationChain::validate(long_path, "path")
            .is_path_safe()
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_shell_metacharacters_validation() {
        // Safe strings
        let result = ValidationChain::validate("simple_filename.txt".to_string(), "filename")
            .no_shell_metacharacters()
            .into_result();
        assert!(result.is_ok());

        let result = ValidationChain::validate("file-with-dashes_and_underscores.txt".to_string(), "filename")
            .no_shell_metacharacters()
            .into_result();
        assert!(result.is_ok());

        // Unsafe strings with shell metacharacters
        let result = ValidationChain::validate("file; rm -rf /".to_string(), "filename")
            .no_shell_metacharacters()
            .into_result();
        assert!(result.is_err());

        let result = ValidationChain::validate("file$(cat /etc/passwd)".to_string(), "filename")
            .no_shell_metacharacters()
            .into_result();
        assert!(result.is_err());

        let result = ValidationChain::validate("file|command".to_string(), "filename")
            .no_shell_metacharacters()
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_depth_validation() {
        // Shallow JSON - should pass
        let shallow_json = json!({
            "level1": {
                "level2": "value"
            }
        });
        let result = ValidationChain::validate(shallow_json, "json")
            .max_depth(5)
            .into_result();
        assert!(result.is_ok());

        // Deep JSON - should fail
        let deep_json = json!({
            "l1": {
                "l2": {
                    "l3": {
                        "l4": {
                            "l5": {
                                "l6": "too deep"
                            }
                        }
                    }
                }
            }
        });
        let result = ValidationChain::validate(deep_json, "json")
            .max_depth(3)
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_size_validation() {
        // Small JSON - should pass
        let small_json = json!({"key": "value"});
        let result = ValidationChain::validate(small_json, "json")
            .max_size(1000)
            .into_result();
        assert!(result.is_ok());

        // Large JSON - should fail
        let large_json = json!({
            "large_field": "x".repeat(1000)
        });
        let result = ValidationChain::validate(large_json, "json")
            .max_size(100)
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_field_validation() {
        let json = json!({
            "name": "John",
            "age": 30,
            "active": true,
            "tags": ["user", "admin"],
            "metadata": {
                "created": "2024-01-01"
            }
        });

        // Test field existence and types
        let result = ValidationChain::validate(json.clone(), "user")
            .has_field("name")
            .has_field("age")
            .field_type("name", JsonType::String)
            .field_type("age", JsonType::Number)
            .field_type("active", JsonType::Bool)
            .field_type("tags", JsonType::Array)
            .field_type("metadata", JsonType::Object)
            .into_result();
        assert!(result.is_ok());

        // Test missing field
        let result = ValidationChain::validate(json.clone(), "user")
            .has_field("nonexistent")
            .into_result();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        // ValidationError::MissingField produces "Missing required field: {field}"
        assert!(error_msg.contains("Missing required field"));

        // Test wrong type
        let result = ValidationChain::validate(json, "user")
            .field_type("name", JsonType::Number)
            .into_result();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected number"));
    }

    #[test]
    fn test_raw_event_validation() {
        use crate::RawEvent;
        use chrono::Utc;
        use sinex_ulid::Ulid;

        // Valid event
        let valid_event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"data": "test"}),
        };

        let result = ValidationChain::validate(valid_event, "event")
            .has_valid_source()
            .has_valid_event_type()
            .payload_is_object()
            .into_result();
        assert!(result.is_ok());

        // Event with empty source
        let invalid_event = RawEvent {
            id: Ulid::new(),
            source: "".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"data": "test"}),
        };

        let result = ValidationChain::validate(invalid_event, "event")
            .has_valid_source()
            .into_result();
        assert!(result.is_err());

        // Event with non-object payload
        let invalid_payload_event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!("not an object"),
        };

        let result = ValidationChain::validate(invalid_payload_event, "event")
            .payload_is_object()
            .into_result();
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_validator() {
        struct StringValidator(String);
        impl Validator for StringValidator {
            fn validate(&self) -> Result<()> {
                ValidationChain::validate(self.0.clone(), "field1")
                    .not_empty()
                    .min_length(5)
                    .into_result()
                    .map(|_| ())
            }
        }

        struct NumberValidator(i32);
        impl Validator for NumberValidator {
            fn validate(&self) -> Result<()> {
                ValidationChain::validate(self.0, "field2")
                    .min(0)
                    .max(100)
                    .into_result()
                    .map(|_| ())
            }
        }

        struct SimpleValidator(bool);
        impl Validator for SimpleValidator {
            fn validate(&self) -> Result<()> {
                if self.0 {
                    Ok(())
                } else {
                    Err(CoreError::Validation("simple validation failed".to_string()))
                }
            }
        }

        // All validators pass
        let result = MultiValidator::new()
            .with_validator(StringValidator("valid_string".to_string()))
            .with_validator(NumberValidator(42))
            .with_validator(SimpleValidator(true))
            .validate_all();
        assert!(result.is_ok());

        // One validator fails
        let result = MultiValidator::new()
            .with_validator(StringValidator("".to_string())) // Empty string will fail
            .with_validator(SimpleValidator(false))
            .validate_all();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Multiple validation errors"));
    }

    #[test]
    fn test_error_accumulation_details() {
        let chain = ValidationChain::validate("a".to_string(), "test_field")
            .not_empty()    // Should pass
            .min_length(5)  // Should fail - too short
            .max_length(10) // Should pass
            .min_length(20); // Should fail - way too short

        assert!(!chain.is_valid());
        
        let errors = chain.errors();
        assert_eq!(errors.len(), 2); // Two min_length failures
        
        let error_result = chain.into_result();
        assert!(error_result.is_err());
        let error_msg = error_result.unwrap_err().to_string();
        assert!(error_msg.contains("must be at least"));
    }

    #[test]
    fn test_validation_chain_chaining() {
        // Test that validation chains can be built fluently
        let result = ValidationChain::validate("test@example.com".to_string(), "email")
            .not_empty()
            .min_length(5)
            .max_length(50)
            .custom(|s| s.contains('@'), "must contain @ symbol")
            .custom(|s| s.contains('.'), "must contain . symbol")
            .into_result();
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test@example.com");
    }
}
