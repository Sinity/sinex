//! Validation Utilities
//!
//! This crate provides comprehensive validation utilities extracted from sinex-core,
//! including validation chains, path validation, JSON validation, and security checks.

pub mod validation;
pub mod validation_chains;

// Re-export main validation utilities
pub use validation::{
    ValidationError, Result, validate_path, sanitize_filename_component,
    validate_path_within_root, validate_json, normalize_unicode,
    contains_shell_metacharacters, check_json_expansion
};

pub use validation_chains::{
    ValidationChain, JsonType, Validator, MultiValidator
};