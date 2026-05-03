//! Validation utilities for the Sinex system
//!
//! This module provides comprehensive validation utilities including validation chains,
//! path validation, JSON validation, and security checks.

pub mod config_validation;
mod core;
pub mod file_watching_security;
pub mod pg_identifier;
pub mod query_validation;
pub mod validation_chains;

pub use validation_chains::{format_validation_errors, format_validation_errors_with_context};

// Re-export main validation utilities
pub use core::{
    check_json_expansion, contains_shell_metacharacters, deserialize_json_with_validation,
    normalize_unicode, sanitize_filename_component, validate_json, validate_json_value,
    validate_path, validate_path_within_root,
};

// Re-export PostgreSQL identifier validation
pub use pg_identifier::validate_pg_identifier;

// Re-export error types
pub use crate::error::Result;

pub use config_validation::{
    PathValidationLevel, SecurePath, deserialize_optional_sanitized_path,
    deserialize_optional_validated_utf8_path, deserialize_sanitized_path,
    deserialize_sanitized_path_vec, deserialize_validated_utf8_path,
    deserialize_validated_utf8_path_vec,
};

// Re-export file watching security utilities
pub use file_watching_security::{
    FileWatchingSecurityPolicy, check_path_depth, check_sensitive_path, validate_discovered_file,
    validate_watch_path, validate_watch_paths,
};

// Export validator crate types for convenience
pub use validator::{Validate, ValidationError, ValidationErrors};
