//! Validation utilities for the Sinex system
//!
//! This module provides comprehensive validation utilities including validation chains,
//! path validation, JSON validation, and security checks.

pub mod config_validation;
mod core;
pub mod file_watching_security;
pub mod validation_chains;

// Re-export main validation utilities
pub use core::{
    check_json_expansion, contains_shell_metacharacters, deserialize_json_with_validation,
    normalize_unicode, sanitize_filename_component, validate_json, validate_json_value,
    validate_path, validate_path_within_root, Result, ValidationError,
};

pub use config_validation::{
    deserialize_optional_sanitized_path, deserialize_optional_validated_utf8_path,
    deserialize_sanitized_path, deserialize_sanitized_path_vec, deserialize_validated_utf8_path,
    deserialize_validated_utf8_path_vec, PathValidationLevel, SecurePath,
};

// Re-export file watching security utilities
pub use file_watching_security::{
    check_path_depth, validate_discovered_file, validate_watch_path, validate_watch_paths,
    FileWatchingSecurityPolicy,
};

// Export validator crate types for convenience
pub use validator::{Validate, ValidationError as ValidatorError, ValidationErrors};
