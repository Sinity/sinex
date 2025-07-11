//! Validation Utilities
//!
//! This crate provides comprehensive validation utilities extracted from sinex-core,
//! including validation chains, path validation, JSON validation, and security checks.

pub mod validation;
pub mod validation_chains;

// Re-export main validation utilities
pub use validation::{
    check_json_expansion, contains_shell_metacharacters, normalize_unicode,
    sanitize_filename_component, validate_json, validate_path, validate_path_within_root, Result,
    ValidationError,
};

pub use validation_chains::{JsonType, MultiValidator, ValidationChain, Validator};
