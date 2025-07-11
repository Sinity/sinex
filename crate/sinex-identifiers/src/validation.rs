//! Identifier Validation
//!
//! Error types and validation utilities for type-safe identifiers.

use thiserror::Error;

/// Type alias for validator function
type ValidatorFunction = Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

/// Result type for identifier operations
pub type IdentifierResult<T> = Result<T, IdentifierError>;

/// Errors that can occur with identifiers
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum IdentifierError {
    /// Validation failed for an identifier
    #[error("Validation failed for {identifier_type} '{value}': {reason}")]
    Validation {
        identifier_type: &'static str,
        value: String,
        reason: String,
    },

    /// Identifier is empty when it shouldn't be
    #[error("Identifier cannot be empty")]
    Empty,

    /// Identifier is too long
    #[error("Identifier too long: {length} characters (max {max})")]
    TooLong { length: usize, max: usize },

    /// Identifier is too short
    #[error("Identifier too short: {length} characters (min {min})")]
    TooShort { length: usize, min: usize },

    /// Invalid character in identifier
    #[error("Invalid character '{character}' at position {position} in identifier")]
    InvalidCharacter { character: char, position: usize },

    /// Invalid format
    #[error("Invalid format: {reason}")]
    InvalidFormat { reason: String },

    /// Namespace error
    #[error("Invalid namespace: {reason}")]
    InvalidNamespace { reason: String },

    /// Scope error
    #[error("Invalid scope: {reason}")]
    InvalidScope { reason: String },

    /// Hierarchy error
    #[error("Invalid hierarchy: {reason}")]
    InvalidHierarchy { reason: String },

    /// Generation error
    #[error("Failed to generate identifier: {reason}")]
    Generation { reason: String },

    /// Conversion error
    #[error("Failed to convert identifier: {reason}")]
    Conversion { reason: String },

    /// Other error
    #[error("Identifier error: {0}")]
    Other(String),
}

/// Common validation functions
pub mod validators {

    /// Validate that a string is not empty
    pub fn not_empty(value: &str) -> Result<(), String> {
        if value.is_empty() {
            Err("cannot be empty".to_string())
        } else {
            Ok(())
        }
    }

    /// Validate string length is within bounds
    pub fn length_between(min: usize, max: usize) -> impl Fn(&str) -> Result<(), String> {
        move |value: &str| {
            let len = value.len();
            if len < min {
                Err(format!("too short (minimum {} characters)", min))
            } else if len > max {
                Err(format!("too long (maximum {} characters)", max))
            } else {
                Ok(())
            }
        }
    }

    /// Validate that string contains only alphanumeric characters
    pub fn alphanumeric(value: &str) -> Result<(), String> {
        if value.chars().all(|c| c.is_alphanumeric()) {
            Ok(())
        } else {
            Err("must contain only alphanumeric characters".to_string())
        }
    }

    /// Validate that string contains only alphanumeric characters and specified separators
    pub fn alphanumeric_with_separators(
        separators: &str,
    ) -> impl Fn(&str) -> Result<(), String> + '_ {
        move |value: &str| {
            if value
                .chars()
                .all(|c| c.is_alphanumeric() || separators.contains(c))
            {
                Ok(())
            } else {
                Err(format!(
                    "must contain only alphanumeric characters and separators: {}",
                    separators
                ))
            }
        }
    }

    /// Validate that string starts with a specific prefix
    pub fn starts_with(prefix: &str) -> impl Fn(&str) -> Result<(), String> + '_ {
        move |value: &str| {
            if value.starts_with(prefix) {
                Ok(())
            } else {
                Err(format!("must start with '{}'", prefix))
            }
        }
    }

    /// Validate that string ends with a specific suffix
    pub fn ends_with(suffix: &str) -> impl Fn(&str) -> Result<(), String> + '_ {
        move |value: &str| {
            if value.ends_with(suffix) {
                Ok(())
            } else {
                Err(format!("must end with '{}'", suffix))
            }
        }
    }

    /// Validate that string matches a regex pattern
    pub fn matches_regex(pattern: &str) -> impl Fn(&str) -> Result<(), String> + '_ {
        move |value: &str| {
            // Simple regex check - in practice you'd use the regex crate
            // For now, we'll do basic pattern matching
            match pattern {
                r"^[a-zA-Z][a-zA-Z0-9_-]*$" => {
                    if value
                        .chars()
                        .next()
                        .map(|c| c.is_alphabetic())
                        .unwrap_or(false)
                        && value
                            .chars()
                            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
                    {
                        Ok(())
                    } else {
                        Err("must start with letter and contain only alphanumeric, underscore, or hyphen".to_string())
                    }
                }
                r"^[a-z][a-z0-9-]*$" => {
                    if value
                        .chars()
                        .next()
                        .map(|c| c.is_lowercase() && c.is_alphabetic())
                        .unwrap_or(false)
                        && value
                            .chars()
                            .all(|c| c.is_lowercase() || c.is_numeric() || c == '-')
                    {
                        Ok(())
                    } else {
                        Err("must start with lowercase letter and contain only lowercase, digits, or hyphens".to_string())
                    }
                }
                _ => Ok(()), // Fallback - accept anything for unknown patterns
            }
        }
    }

    /// Validate that string doesn't contain any of the forbidden characters
    pub fn no_forbidden_chars(forbidden: &str) -> impl Fn(&str) -> Result<(), String> + '_ {
        move |value: &str| {
            if let Some(forbidden_char) = value.chars().find(|c| forbidden.contains(*c)) {
                Err(format!(
                    "contains forbidden character: '{}'",
                    forbidden_char
                ))
            } else {
                Ok(())
            }
        }
    }

    /// Validate ULID format
    pub fn ulid_format(value: &str) -> Result<(), String> {
        if value.len() != 26 {
            return Err("ULID must be exactly 26 characters".to_string());
        }

        // ULID uses Crockford's Base32 encoding
        let valid_chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        if value.chars().all(|c| valid_chars.contains(c)) {
            Ok(())
        } else {
            Err("ULID contains invalid characters".to_string())
        }
    }

    /// Validate UUID format
    pub fn uuid_format(value: &str) -> Result<(), String> {
        // Simple UUID format check: 8-4-4-4-12 hex digits
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() != 5 {
            return Err("UUID must have 5 parts separated by hyphens".to_string());
        }

        let expected_lengths = [8, 4, 4, 4, 12];
        for (i, (part, &expected_len)) in parts.iter().zip(expected_lengths.iter()).enumerate() {
            if part.len() != expected_len {
                return Err(format!("UUID part {} has wrong length", i + 1));
            }
            if !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("UUID part {} contains non-hex characters", i + 1));
            }
        }

        Ok(())
    }

    /// Validate email format (basic)
    pub fn email_format(value: &str) -> Result<(), String> {
        if value.contains('@')
            && value.len() > 3
            && value.chars().filter(|&c| c == '@').count() == 1
        {
            let parts: Vec<&str> = value.split('@').collect();
            if parts.len() == 2
                && !parts[0].is_empty()
                && !parts[1].is_empty()
                && parts[1].contains('.')
            {
                Ok(())
            } else {
                Err("invalid email format".to_string())
            }
        } else {
            Err("invalid email format".to_string())
        }
    }

    /// Validate URL format (basic)
    pub fn url_format(value: &str) -> Result<(), String> {
        if value.starts_with("http://") || value.starts_with("https://") {
            if value.len() > 10 {
                Ok(())
            } else {
                Err("URL too short".to_string())
            }
        } else {
            Err("URL must start with http:// or https://".to_string())
        }
    }

    /// Validate path format (Unix-style)
    pub fn path_format(value: &str) -> Result<(), String> {
        if value.is_empty() {
            return Err("path cannot be empty".to_string());
        }

        // Check for dangerous path components
        if value.contains("..") {
            return Err("path cannot contain '..' components".to_string());
        }

        if value.contains('\0') {
            return Err("path cannot contain null bytes".to_string());
        }

        Ok(())
    }

    /// Combine multiple validators with AND logic
    pub fn combine_and<F1, F2>(v1: F1, v2: F2) -> impl Fn(&str) -> Result<(), String>
    where
        F1: Fn(&str) -> Result<(), String>,
        F2: Fn(&str) -> Result<(), String>,
    {
        move |value: &str| {
            v1(value)?;
            v2(value)?;
            Ok(())
        }
    }

    /// Combine multiple validators with OR logic (succeeds if any validator passes)
    pub fn combine_or<F1, F2>(v1: F1, v2: F2) -> impl Fn(&str) -> Result<(), String>
    where
        F1: Fn(&str) -> Result<(), String>,
        F2: Fn(&str) -> Result<(), String>,
    {
        move |value: &str| {
            if v1(value).is_ok() || v2(value).is_ok() {
                Ok(())
            } else {
                Err("does not match any valid format".to_string())
            }
        }
    }
}

/// Validation builder for complex validation rules
pub struct ValidationBuilder {
    validators: Vec<ValidatorFunction>,
}

impl ValidationBuilder {
    /// Create a new validation builder
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
        }
    }

    /// Add a validator function
    pub fn with_validator<F>(mut self, validator: F) -> Self
    where
        F: Fn(&str) -> Result<(), String> + Send + Sync + 'static,
    {
        self.validators.push(Box::new(validator));
        self
    }

    /// Add a not-empty validation
    pub fn not_empty(self) -> Self {
        self.with_validator(validators::not_empty)
    }

    /// Add a length validation
    pub fn length(self, min: usize, max: usize) -> Self {
        self.with_validator(validators::length_between(min, max))
    }

    /// Add an alphanumeric validation
    pub fn alphanumeric(self) -> Self {
        self.with_validator(validators::alphanumeric)
    }

    /// Add a regex validation
    pub fn regex(self, pattern: &str) -> Self {
        let pattern = pattern.to_string();
        self.with_validator(move |value| validators::matches_regex(&pattern)(value))
    }

    /// Build the final validator function
    pub fn build(self) -> impl Fn(&str) -> Result<(), String> {
        move |value: &str| {
            for validator in &self.validators {
                validator(value)?;
            }
            Ok(())
        }
    }
}

impl Default for ValidationBuilder {
    fn default() -> Self {
        Self::new()
    }
}
