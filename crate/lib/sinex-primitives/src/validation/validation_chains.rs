//! Modern validation using the validator crate
//!
//! This module provides derive-based validation for structs using the validator crate,
//! replacing the custom validation chains with a more standard approach.

use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError, ValidationErrors};

/// Example configuration struct with validation rules
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct DatabaseConfig {
    #[validate(url)]
    pub connection_url: String,

    #[validate(range(min = 1, max = 1000))]
    pub max_connections: u32,

    #[validate(range(min = 0))]
    pub connection_timeout_ms: u64,

    #[validate(length(min = 1))]
    pub database_name: String,
}

/// Example event validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct EventValidation {
    #[validate(length(min = 1, max = 100))]
    pub event_type: String,

    #[validate(length(min = 1, max = 50))]
    pub source: String,

    #[validate(custom(function = "validate_host"))]
    pub host: String,

    #[validate(email)]
    pub contact_email: Option<String>,
}

/// Custom validator for host names
fn validate_host(host: &str) -> Result<(), ValidationError> {
    if host.is_empty() {
        return Err(ValidationError::new("host_empty"));
    }

    // Basic hostname validation - could be more sophisticated
    if host.contains("..") || host.starts_with('.') || host.ends_with('.') {
        return Err(ValidationError::new("invalid_hostname"));
    }

    Ok(())
}

// Regex for safe relative paths (no directory traversal)
lazy_static::lazy_static! {
    static ref SAFE_PATH_REGEX: regex::Regex = regex::Regex::new(r"^[a-zA-Z0-9_\-/]+$").unwrap();
}

/// File path validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct FilePathConfig {
    #[validate(custom(function = "validate_path"))]
    pub base_path: String,

    #[validate(custom(function = "validate_relative_path"))]
    pub relative_path: String,
}

fn validate_relative_path(path: &str) -> Result<(), ValidationError> {
    if SAFE_PATH_REGEX.is_match(path) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_relative_path"))
    }
}

fn validate_path(path: &str) -> Result<(), ValidationError> {
    if path.contains("..") {
        return Err(ValidationError::new("path_traversal"));
    }

    if !path.starts_with('/') {
        return Err(ValidationError::new("absolute_path_required"));
    }

    Ok(())
}

/// Network configuration with nested validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct NetworkConfig {
    #[validate(ip)]
    pub bind_address: String,

    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    #[validate(nested)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TlsConfig {
    #[validate(custom(function = "validate_path"))]
    pub cert_path: String,

    #[validate(custom(function = "validate_path"))]
    pub key_path: String,

    #[validate(length(min = 1))]
    pub ca_cert_path: Option<String>,
}

/// Helper functions for validation
pub trait ValidateExt {
    /// Validate and return a user-friendly error message
    fn validate_friendly(&self) -> Result<(), String>;

    /// Validate with field context
    fn validate_with_context(&self, context: &str) -> Result<(), String>;
}

impl<T: Validate> ValidateExt for T {
    fn validate_friendly(&self) -> Result<(), String> {
        match self.validate() {
            Ok(_) => Ok(()),
            Err(errors) => Err(format_validation_errors(&errors)),
        }
    }

    fn validate_with_context(&self, context: &str) -> Result<(), String> {
        match self.validate() {
            Ok(_) => Ok(()),
            Err(errors) => Err(format_validation_errors_with_context(&errors, context)),
        }
    }
}

/// Format validation errors into a user-friendly message
pub fn format_validation_errors(errors: &ValidationErrors) -> String {
    let mut messages = Vec::new();

    for (field, field_errors) in errors.field_errors() {
        for error in field_errors {
            let msg = match &error.code {
                std::borrow::Cow::Borrowed("email") => format!("{field}: invalid email format"),
                std::borrow::Cow::Borrowed("url") => format!("{field}: invalid URL format"),
                std::borrow::Cow::Borrowed("required") => format!("{field}: field is required"),
                std::borrow::Cow::Borrowed("range") => {
                    let min = error.params.get("min");
                    let max = error.params.get("max");
                    match (min, max) {
                        (Some(min), Some(max)) => {
                            format!("{field}: must be between {min} and {max}")
                        }
                        (Some(min), None) => format!("{field}: must be at least {min}"),
                        (None, Some(max)) => format!("{field}: must be at most {max}"),
                        _ => format!("{field}: out of range"),
                    }
                }
                std::borrow::Cow::Borrowed("length") => {
                    let min = error.params.get("min");
                    let max = error.params.get("max");
                    match (min, max) {
                        (Some(min), Some(max)) => {
                            format!("{field}: length must be between {min} and {max}")
                        }
                        (Some(min), None) => format!("{field}: length must be at least {min}"),
                        (None, Some(max)) => format!("{field}: length must be at most {max}"),
                        _ => format!("{field}: invalid length"),
                    }
                }
                code => format!("{field}: {code}"),
            };
            messages.push(msg);
        }
    }

    messages.join("; ")
}

/// Format validation errors with additional context
pub fn format_validation_errors_with_context(errors: &ValidationErrors, context: &str) -> String {
    let base_message = format_validation_errors(errors);
    format!("{context}: {base_message}")
}
