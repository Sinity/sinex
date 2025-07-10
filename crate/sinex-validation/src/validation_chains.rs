use crate::validation::{ValidationError, Result};
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
            Err(ValidationError::General(combined_message))
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
            self.errors.push(ValidationError::General(format!(
                "{} cannot be empty", self.field_name
            )));
        }
        self
    }

    /// Validate minimum string length
    pub fn min_length(mut self, min: usize) -> Self {
        if self.value.len() < min {
            self.errors.push(ValidationError::General(format!(
                "{} must be at least {} characters long", self.field_name, min
            )));
        }
        self
    }

    /// Validate maximum string length
    pub fn max_length(mut self, max: usize) -> Self {
        if self.value.len() > max {
            self.errors.push(ValidationError::General(format!(
                "{} must be at most {} characters long", self.field_name, max
            )));
        }
        self
    }

    /// Validate string matches a regex pattern
    pub fn matches_regex(mut self, pattern: &Regex) -> Self {
        if !pattern.is_match(&self.value) {
            self.errors.push(ValidationError::General(format!(
                "{} does not match pattern: {}", self.field_name, pattern.as_str()
            )));
        }
        self
    }

    /// Validate string is safe for use as a file path (no directory traversal)
    pub fn is_path_safe(mut self) -> Self {
        match crate::validation::validate_path(&self.value) {
            Ok(_) => {}
            Err(_) => {
                self.errors.push(ValidationError::General(format!(
                    "{} contains unsafe path characters or patterns", self.field_name
                )));
            }
        }
        self
    }

    /// Validate string is a valid URL
    pub fn is_valid_url(mut self) -> Self {
        match Url::parse(&self.value) {
            Ok(_) => {}
            Err(e) => {
                self.errors.push(ValidationError::General(format!(
                    "{} invalid URL: {}", self.field_name, e
                )));
            }
        }
        self
    }

    /// Validate string contains no shell metacharacters
    pub fn no_shell_metacharacters(mut self) -> Self {
        if crate::validation::contains_shell_metacharacters(&self.value) {
            self.errors.push(ValidationError::General(format!(
                "{} contains shell metacharacters", self.field_name
            )));
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
            self.errors.push(ValidationError::General(format!(
                "{} {}", self.field_name, error_message
            )));
        }
        self
    }
}

// String slice (&str) validations - mirrors String implementation for compatibility
impl ValidationChain<&str> {
    /// Validate that the string is not empty
    pub fn not_empty(mut self) -> Self {
        if self.value.is_empty() {
            self.errors.push(ValidationError::General(format!(
                "{} cannot be empty", self.field_name
            )));
        }
        self
    }

    /// Validate minimum string length
    pub fn min_length(mut self, min: usize) -> Self {
        if self.value.len() < min {
            self.errors.push(ValidationError::General(format!(
                "{} must be at least {} characters long", self.field_name, min
            )));
        }
        self
    }

    /// Validate maximum string length
    pub fn max_length(mut self, max: usize) -> Self {
        if self.value.len() > max {
            self.errors.push(ValidationError::General(format!(
                "{} must be at most {} characters long", self.field_name, max
            )));
        }
        self
    }

    /// Validate string matches a regex pattern
    pub fn matches_regex(mut self, pattern: &Regex) -> Self {
        if !pattern.is_match(self.value) {
            self.errors.push(ValidationError::General(format!(
                "{} does not match pattern: {}", self.field_name, pattern.as_str()
            )));
        }
        self
    }

    /// Validate string is safe for use as a file path (no directory traversal)
    pub fn is_path_safe(mut self) -> Self {
        match crate::validation::validate_path(self.value) {
            Ok(_) => {}
            Err(e) => {
                self.errors.push(ValidationError::General(format!(
                    "{} path validation failed: {}", self.field_name, e
                )));
            }
        }
        self
    }

    /// Validate that string is a valid URL
    pub fn is_valid_url(mut self) -> Self {
        match Url::parse(self.value) {
            Ok(_) => {}
            Err(e) => {
                self.errors.push(ValidationError::General(format!(
                    "{} invalid URL: {}", self.field_name, e
                )));
            }
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
            self.errors.push(ValidationError::General(format!(
                "{} must be at least {}", self.field_name, min
            )));
        }
        self
    }

    /// Validate maximum value
    pub fn max(mut self, max: T) -> Self {
        if self.value > max {
            self.errors.push(ValidationError::General(format!(
                "{} must be at most {}", self.field_name, max
            )));
        }
        self
    }

    /// Validate value is within range
    pub fn range(mut self, range: std::ops::Range<T>) -> Self {
        if self.value < range.start || self.value >= range.end {
            self.errors.push(ValidationError::General(format!(
                "{} must be between {} and {} (exclusive)", self.field_name, range.start, range.end
            )));
        }
        self
    }

    /// Alias for min() for compatibility
    pub fn min_value(self, min: T) -> Self {
        self.min(min)
    }
}

/// JSON-specific validation chain
impl ValidationChain<Value> {
    /// Validate JSON has a specific field
    pub fn has_field(mut self, field: &str) -> Self {
        match &self.value {
            Value::Object(map) => {
                if !map.contains_key(field) {
                    self.errors.push(ValidationError::General(format!(
                        "Missing required field: {}", field
                    )));
                }
            }
            _ => {
                self.errors.push(ValidationError::General(format!(
                    "{} expected object, got {}", self.field_name, json_type_name(&self.value)
                )));
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
                        self.errors.push(ValidationError::General(format!(
                            "{} field expected {}, got {}", field, expected, json_type_name(value)
                        )));
                    }
                }
                None => {
                    self.errors.push(ValidationError::General(format!(
                        "Missing required field: {}", field
                    )));
                }
            },
            _ => {
                self.errors.push(ValidationError::General(format!(
                    "{} expected object, got {}", self.field_name, json_type_name(&self.value)
                )));
            }
        }
        self
    }

    /// Validate JSON depth doesn't exceed limit
    pub fn max_depth(mut self, depth: usize) -> Self {
        if calculate_json_depth(&self.value) > depth {
            self.errors.push(ValidationError::General(format!(
                "{} JSON nesting exceeds maximum depth of {}", self.field_name, depth
            )));
        }
        self
    }

    /// Validate JSON serialized size doesn't exceed limit
    pub fn max_size(mut self, bytes: usize) -> Self {
        match serde_json::to_string(&self.value) {
            Ok(json_str) => {
                if json_str.len() > bytes {
                    self.errors.push(ValidationError::General(format!(
                        "{} JSON size ({} bytes) exceeds maximum of {} bytes",
                        self.field_name, json_str.len(), bytes
                    )));
                }
            }
            Err(_) => {
                self.errors.push(ValidationError::General(format!(
                    "{} failed to serialize JSON for size check", self.field_name
                )));
            }
        }
        self
    }

    /// Validate against potential billion laughs attack
    pub fn no_excessive_expansion(mut self) -> Self {
        match crate::validation::check_json_expansion(&self.value) {
            Ok(_) => {}
            Err(_) => {
                self.errors.push(ValidationError::General(format!(
                    "{} JSON structure has excessive expansion ratio", self.field_name
                )));
            }
        }
        self
    }

    /// Validate JSON has a specific type
    pub fn json_type(mut self, expected: JsonType) -> Self {
        if !expected.matches(&self.value) {
            self.errors.push(ValidationError::General(format!(
                "{} expected {}, got {}", self.field_name, expected, json_type_name(&self.value)
            )));
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
            if let Err(e) = validator.validate() {
                all_errors.push(e.to_string());
            }
        }

        if all_errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationError::General(format!(
                "Multiple validation errors: {}",
                all_errors.join("; ")
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
}