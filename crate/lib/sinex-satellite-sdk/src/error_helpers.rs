//! Error Context Helpers and Configuration Parsing Utilities
//!
//! Common error handling and configuration parsing utilities to reduce code duplication
//! across satellites. These helpers provide consistent error context and conversion patterns.

use crate::{stream_processor::StreamProcessorContext, SatelliteError};
use std::io;

/// Convert IO errors to SatelliteError with context
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::io_error_with_context;
///
/// let result = std::fs::read("nonexistent.txt")
///     .map_err(|e| io_error_with_context(e, "Failed to read config file"));
/// ```
pub fn io_error_with_context(error: io::Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Convert UTF-8 conversion errors to SatelliteError with context
pub fn utf8_error_with_context(error: std::string::FromUtf8Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Convert serde_json errors to SatelliteError with context
pub fn json_error_with_context(error: serde_json::Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Create a processing error with formatted context
pub fn processing_error(message: &str) -> SatelliteError {
    SatelliteError::Processing(message.to_string())
}

/// Create a processing error with formatted message
pub fn processing_error_fmt(args: std::fmt::Arguments<'_>) -> SatelliteError {
    SatelliteError::Processing(args.to_string())
}

/// Parse configuration value from context with fallback handling
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::parse_config_value;
///
/// let value: Option<bool> = parse_config_value("enabled", &context);
/// ```
pub fn parse_config_value<T: serde::de::DeserializeOwned>(
    key: &str,
    ctx: &StreamProcessorContext,
) -> Option<T> {
    ctx.config
        .get(key)
        .and_then(|json| serde_json::from_value::<T>(json.clone()).ok())
}

/// Parse strongly-typed configuration from a specific key in the context
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::parse_typed_config;
///
/// #[derive(serde::Deserialize)]
/// struct MyConfig {
///     enabled: bool,
/// }
///
/// let config: Option<MyConfig> = parse_typed_config("my_service", &context);
/// ```
pub fn parse_typed_config<T: serde::de::DeserializeOwned>(
    config_key: &str,
    ctx: &StreamProcessorContext,
) -> Option<T> {
    ctx.config
        .get(config_key)
        .and_then(|json| serde_json::from_value::<T>(json.clone()).ok())
}

/// Path sanitization utilities
pub mod path_utils {
    /// Sanitize a path component for safe storage
    ///
    /// This uses the core sanitization logic and is a convenience wrapper
    /// for satellites that need to sanitize file paths.
    pub fn sanitize_path_component(path_str: &str) -> String {
        let path = std::path::Path::new(path_str);
        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
            let sanitized_name = sinex_core::types::sanitize_filename_component(filename)
                .unwrap_or_else(|_| filename.to_string());
            path.parent()
                .map(|parent| parent.join(&sanitized_name).to_string_lossy().to_string())
                .unwrap_or_else(|| sanitized_name)
        } else {
            path_str.to_string()
        }
    }

    /// Extract file:// URLs from text content
    ///
    /// Returns a list of sanitized file paths if the content appears to be
    /// file URLs or absolute paths.
    pub fn extract_file_paths(content: &str) -> Option<Vec<String>> {
        if content.starts_with("file://") {
            Some(
                content
                    .lines()
                    .filter_map(|line| {
                        line.strip_prefix("file://")
                            .and_then(|p| urlencoding::decode(p).ok())
                            .map(|p| sanitize_path_component(&p.to_string()))
                    })
                    .collect(),
            )
        } else if content.lines().all(|l| l.starts_with('/') || l.is_empty()) {
            Some(
                content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| sanitize_path_component(l))
                    .collect(),
            )
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::prelude::*;
    use std::io::ErrorKind;

    // =============================================================================
    // IO Error Context Tests
    // =============================================================================

    #[sinex_test]
    fn test_io_error_with_context() -> TestResult {
        // Test various IO error types with context
        let test_cases = vec![
            (ErrorKind::NotFound, "File not found error"),
            (ErrorKind::PermissionDenied, "Permission error"),
            (ErrorKind::ConnectionRefused, "Network error"),
            (ErrorKind::TimedOut, "Timeout error"),
            (ErrorKind::InvalidData, "Data validation error"),
        ];

        for (error_kind, context) in test_cases {
            let io_error = std::io::Error::new(error_kind, "original error message");
            let satellite_error = io_error_with_context(io_error, context);

            match satellite_error {
                SatelliteError::Processing(message) => {
                    assert!(
                        message.contains(context),
                        "Error message should contain context: {}",
                        message
                    );
                    assert!(
                        message.contains("original error message"),
                        "Error message should contain original message: {}",
                        message
                    );
                }
                _ => panic!("Expected Processing error variant"),
            }
        }

        Ok(())
    }

    #[sinex_test]
    fn test_io_error_with_empty_context() -> TestResult {
        let io_error = std::io::Error::new(ErrorKind::NotFound, "test error");
        let satellite_error = io_error_with_context(io_error, "");

        match satellite_error {
            SatelliteError::Processing(message) => {
                assert!(
                    message.contains("test error"),
                    "Should contain original error"
                );
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    // =============================================================================
    // UTF-8 Error Context Tests
    // =============================================================================

    #[sinex_test]
    fn test_utf8_error_with_context() -> TestResult {
        // Create invalid UTF-8 bytes
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let utf8_error = String::from_utf8(invalid_utf8).unwrap_err();

        let satellite_error = utf8_error_with_context(utf8_error, "Failed to decode response");

        match satellite_error {
            SatelliteError::Processing(message) => {
                assert!(
                    message.contains("Failed to decode response"),
                    "Error should contain context: {}",
                    message
                );
                assert!(
                    message.contains("invalid utf-8") || message.contains("Invalid UTF-8"),
                    "Error should mention UTF-8 issue: {}",
                    message
                );
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    // =============================================================================
    // JSON Error Context Tests
    // =============================================================================

    #[sinex_test]
    fn test_json_error_with_context() -> TestResult {
        // Test various JSON parsing errors
        let invalid_json_strings = vec![
            ("{invalid_json}", "Malformed JSON object"),
            ("[1, 2, 3,]", "Trailing comma in array"),
            ("\"unclosed string", "Unclosed string literal"),
            ("{\"key\": }", "Missing value"),
            ("null extra", "Extra tokens after null"),
        ];

        for (json_str, test_description) in invalid_json_strings {
            println!("Testing JSON error case: {}", test_description);

            let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
            let satellite_error = json_error_with_context(json_error, "Config parsing failed");

            match satellite_error {
                SatelliteError::Processing(message) => {
                    assert!(
                        message.contains("Config parsing failed"),
                        "Error should contain context: {}",
                        message
                    );
                    // JSON errors should contain position or parsing information
                    assert!(
                        message.len() > "Config parsing failed: ".len(),
                        "Error should contain JSON parsing details: {}",
                        message
                    );
                }
                _ => panic!(
                    "Expected Processing error variant for case: {}",
                    test_description
                ),
            }
        }

        Ok(())
    }

    // =============================================================================
    // Processing Error Tests
    // =============================================================================

    #[sinex_test]
    fn test_processing_error() -> TestResult {
        let error = processing_error("Something went wrong");

        match error {
            SatelliteError::Processing(message) => {
                assert_eq!(message, "Something went wrong");
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    #[sinex_test]
    fn test_processing_error_fmt() -> TestResult {
        let value = 42;
        let error = processing_error_fmt(format_args!("Value {} is invalid", value));

        match error {
            SatelliteError::Processing(message) => {
                assert_eq!(message, "Value 42 is invalid");
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    #[sinex_test]
    fn test_processing_error_with_special_characters() -> TestResult {
        // Test that special characters in error messages are preserved
        let special_message = "Error: 100% failed with UTF-8 chars: ñ, é, 中文";
        let error = processing_error(special_message);

        match error {
            SatelliteError::Processing(message) => {
                assert_eq!(message, special_message);
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    // =============================================================================
    // Error Chain Context Tests
    // =============================================================================

    #[sinex_test]
    fn test_error_chain_context_preservation() -> TestResult {
        // Test that error context is properly preserved through multiple conversions
        let original_io_error = std::io::Error::new(ErrorKind::NotFound, "file.txt");

        // First conversion
        let satellite_error = io_error_with_context(original_io_error, "Config loading");

        // Convert back to string and verify both contexts are present
        let error_string = format!("{}", satellite_error);

        assert!(
            error_string.contains("Config loading"),
            "Should contain first context: {}",
            error_string
        );
        assert!(
            error_string.contains("file.txt"),
            "Should contain original error: {}",
            error_string
        );

        Ok(())
    }

    #[sinex_test]
    fn test_error_helpers_with_empty_strings() -> TestResult {
        // Test edge cases with empty strings

        // Empty JSON should produce a valid error
        let json_error = serde_json::from_str::<serde_json::Value>("").unwrap_err();
        let satellite_error = json_error_with_context(json_error, "Empty config");

        match satellite_error {
            SatelliteError::Processing(message) => {
                assert!(message.contains("Empty config"));
                assert!(message.len() > "Empty config: ".len());
            }
            _ => panic!("Expected Processing error variant"),
        }

        Ok(())
    }

    // =============================================================================
    // Error Display and Debug Tests
    // =============================================================================

    #[sinex_test]
    fn test_error_display_formatting() -> TestResult {
        let error = processing_error("Test error message");

        // Test Display implementation
        let display_str = format!("{}", error);
        assert!(display_str.contains("Test error message"));

        // Test Debug implementation
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Processing"));
        assert!(debug_str.contains("Test error message"));

        Ok(())
    }
}