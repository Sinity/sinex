//! Fuzz target for path sanitization and validation functions.
//!
//! Tests security functions that prevent path traversal attacks:
//! - `SecurityValidator::sanitize_path`
//! - `validate_path`
//! - `sanitize_filename_component`
//! - `validate_path_within_root`

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

/// Arbitrary input for path fuzzing that generates diverse attack patterns.
#[derive(Debug, Arbitrary)]
struct PathInput {
    /// The raw path string to test
    path: String,
    /// Whether to include null bytes
    include_null: bool,
    /// Whether to include URL encoding
    include_url_encoding: bool,
    /// Whether to include unicode tricks
    include_unicode: bool,
}

impl PathInput {
    /// Generate a potentially malicious path based on flags.
    fn to_path(&self) -> String {
        let mut path = self.path.clone();

        if self.include_null {
            // Insert null bytes at random positions
            path.push('\0');
        }

        if self.include_url_encoding {
            // Add URL-encoded traversal sequences
            path = path.replace("..", "%2e%2e");
        }

        if self.include_unicode {
            // Add dangerous unicode characters
            path.push('\u{202E}'); // Right-to-left override
        }

        path
    }
}

fuzz_target!(|input: PathInput| {
    let path = input.to_path();

    // Test SecurityValidator::sanitize_path
    // This should never panic, only return Ok/Err
    let _ = sinex_core::db::security::SecurityValidator::sanitize_path(&path);

    // Test validate_path from types/validation
    // This should never panic, only return Ok/Err
    let _ = sinex_core::types::validation::validate_path(&path);

    // Test sanitize_filename_component
    let _ = sinex_core::types::validation::sanitize_filename_component(&path);

    // Test SecurityValidator::sanitize_unicode
    let _ = sinex_core::db::security::SecurityValidator::sanitize_unicode(&path);

    // Test validate_config_content for command injection
    let _ = sinex_core::db::security::SecurityValidator::validate_config_content(&path);

    // Test sanitize_config_value
    let _ = sinex_core::db::security::SecurityValidator::sanitize_config_value(&path);

    // Test sanitize_config_path
    let _ = sinex_core::db::security::SecurityValidator::sanitize_config_path(&path);
});
