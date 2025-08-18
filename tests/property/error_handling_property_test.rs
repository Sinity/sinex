//! Property tests for error handling and conversion utilities
//!
//! Tests the robustness of error handling functions with arbitrary inputs,
//! ensuring they handle edge cases gracefully and maintain error context.

use proptest::prelude::*;
use sinex_satellite_sdk::error_helpers::*;
use sinex_test_utils::prelude::*;
use std::io::{Error as IoError, ErrorKind};

// =============================================================================
// Property Test Strategies
// =============================================================================

/// Strategy for generating arbitrary error messages
fn arbitrary_error_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("simple error".to_string()),
        "[a-zA-Z0-9 .,!?-]{1,100}",
        ".*", // Any UTF-8 string
        // Special cases that might break formatting
        Just("Error with \n newlines".to_string()),
        Just("Error with \0 null bytes".to_string()),
        Just("Error with 中文 unicode".to_string()),
        Just("Error with emoji 🚨".to_string()),
        Just("Very long error message that might exceed typical buffer sizes or cause formatting issues when displayed to users or logged to files".repeat(10)),
    ]
}

/// Strategy for generating arbitrary context strings
fn arbitrary_context() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("context".to_string()),
        "[a-zA-Z0-9 _-]{1,50}",
        ".*", // Any UTF-8 string with special characters
    ]
}

/// Strategy for generating IO errors
fn arbitrary_io_error() -> impl Strategy<Value = IoError> {
    prop_oneof![
        Just(IoError::new(ErrorKind::NotFound, "file not found")),
        Just(IoError::new(
            ErrorKind::PermissionDenied,
            "permission denied"
        )),
        Just(IoError::new(
            ErrorKind::ConnectionRefused,
            "connection refused"
        )),
        Just(IoError::new(ErrorKind::TimedOut, "operation timed out")),
        Just(IoError::new(ErrorKind::InvalidData, "invalid data")),
        Just(IoError::new(
            ErrorKind::UnexpectedEof,
            "unexpected end of file"
        )),
        Just(IoError::new(
            ErrorKind::Interrupted,
            "operation interrupted"
        )),
        // Custom error messages
        arbitrary_error_message().prop_map(|msg| IoError::new(ErrorKind::Other, msg)),
    ]
}

/// Strategy for generating malformed JSON strings
fn arbitrary_malformed_json() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("{".to_string()),
        Just("}".to_string()),
        Just("[".to_string()),
        Just("]".to_string()),
        Just("null extra".to_string()),
        Just("{\"key\":}".to_string()),
        Just("{\"key\": null,}".to_string()),
        Just("\"unclosed string".to_string()),
        Just("invalid json".to_string()),
        // Generate random strings that might be parsed as JSON
        ".*",
        // Very large JSON-like strings
        Just("{".repeat(1000)),
        Just("\"".repeat(1000)),
    ]
}

// =============================================================================
// Property Tests for IO Error Handling
// =============================================================================

proptest! {
    #[test]
    fn prop_io_error_with_context_preserves_information(
        io_error in arbitrary_io_error(),
        context in arbitrary_context()
    ) {
        let original_message = io_error.to_string();
        let satellite_error = io_error_with_context(io_error, &context);

        match satellite_error {
            sinex_satellite_sdk::SatelliteError::Processing(message) => {
                // Error message should be non-empty
                prop_assert!(!message.is_empty(), "Error message should not be empty");

                // If context is non-empty, it should be in the message
                if !context.is_empty() {
                    prop_assert!(
                        message.contains(&context),
                        "Error message '{}' should contain context '{}'",
                        message,
                        context
                    );
                }

                // Original error info should be preserved (unless it was empty)
                if !original_message.is_empty() {
                    prop_assert!(
                        message.contains(&original_message) ||
                        message.len() > context.len() + 2, // At least some original info
                        "Error message should preserve original information"
                    );
                }
            }
            _ => prop_assert!(false, "Expected Processing error variant"),
        }
    }
}

proptest! {
    #[test]
    fn prop_io_error_context_is_well_formed(
        io_error in arbitrary_io_error(),
        context in arbitrary_context()
    ) {
        let satellite_error = io_error_with_context(io_error, &context);

        match satellite_error {
            sinex_satellite_sdk::SatelliteError::Processing(message) => {
                // Message should be valid UTF-8 (guaranteed by String type)
                prop_assert!(message.is_valid_utf8());

                // Message should not contain null bytes (which could break logging)
                prop_assert!(!message.contains('\0'), "Error message should not contain null bytes");

                // Message should have reasonable length (not empty, not extremely long)
                prop_assert!(message.len() <= 10000, "Error message should not be excessively long");
            }
            _ => prop_assert!(false, "Expected Processing error variant"),
        }
    }
}

// =============================================================================
// Property Tests for UTF-8 Error Handling
// =============================================================================

proptest! {
    #[test]
    fn prop_utf8_error_with_context_handles_arbitrary_context(
        context in arbitrary_context()
    ) {
        // Create a consistent UTF-8 error for testing
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let utf8_error = String::from_utf8(invalid_utf8).unwrap_err();

        let satellite_error = utf8_error_with_context(utf8_error, &context);

        match satellite_error {
            sinex_satellite_sdk::SatelliteError::Processing(message) => {
                prop_assert!(!message.is_empty(), "Error message should not be empty");

                if !context.is_empty() {
                    prop_assert!(
                        message.contains(&context),
                        "Error message should contain context"
                    );
                }
            }
            _ => prop_assert!(false, "Expected Processing error variant"),
        }
    }
}

// =============================================================================
// Property Tests for JSON Error Handling
// =============================================================================

proptest! {
    #[test]
    fn prop_json_error_with_context_handles_malformed_json(
        json_str in arbitrary_malformed_json(),
        context in arbitrary_context()
    ) {
        // Try to parse the JSON string - if it's valid, skip this test case
        if serde_json::from_str::<serde_json::Value>(&json_str).is_ok() {
            return Ok(()); // Skip valid JSON for this test
        }

        // If parsing failed, we have a JSON error to test with
        if let Err(json_error) = serde_json::from_str::<serde_json::Value>(&json_str) {
            let satellite_error = json_error_with_context(json_error, &context);

            match satellite_error {
                sinex_satellite_sdk::SatelliteError::Processing(message) => {
                    prop_assert!(!message.is_empty(), "Error message should not be empty");

                    if !context.is_empty() {
                        prop_assert!(
                            message.contains(&context),
                            "Error message '{}' should contain context '{}'",
                            message,
                            context
                        );
                    }

                    // Message should be well-formed
                    prop_assert!(message.len() <= 10000, "Error message should not be excessively long");
                    prop_assert!(!message.contains('\0'), "Error message should not contain null bytes");
                }
                _ => prop_assert!(false, "Expected Processing error variant"),
            }
        }
    }
}

// =============================================================================
// Property Tests for Processing Errors
// =============================================================================

proptest! {
    #[test]
    fn prop_processing_error_preserves_message_exactly(
        message in arbitrary_error_message()
    ) {
        let error = processing_error(&message);

        match error {
            sinex_satellite_sdk::SatelliteError::Processing(result_message) => {
                prop_assert_eq!(result_message, message, "Processing error should preserve message exactly");
            }
            _ => prop_assert!(false, "Expected Processing error variant"),
        }
    }
}

proptest! {
    #[test]
    fn prop_processing_error_fmt_handles_various_formats(
        value in any::<i32>(),
        text in "[a-zA-Z0-9 ]{0,20}"
    ) {
        let formatted_message = format!("Value {} with text '{}'", value, text);
        let error = processing_error_fmt(format_args!("Value {} with text '{}'", value, text));

        match error {
            sinex_satellite_sdk::SatelliteError::Processing(result_message) => {
                prop_assert_eq!(
                    result_message,
                    formatted_message,
                    "Formatted processing error should match expected format"
                );
            }
            _ => prop_assert!(false, "Expected Processing error variant"),
        }
    }
}

// =============================================================================
// Property Tests for Error Display and Debug
// =============================================================================

proptest! {
    #[test]
    fn prop_error_display_and_debug_are_well_formed(
        message in arbitrary_error_message()
    ) {
        let error = processing_error(&message);

        // Test Display implementation
        let display_str = format!("{}", error);
        prop_assert!(!display_str.is_empty(), "Display string should not be empty");
        prop_assert!(display_str.len() <= 20000, "Display string should be reasonable length");

        // Test Debug implementation
        let debug_str = format!("{:?}", error);
        prop_assert!(!debug_str.is_empty(), "Debug string should not be empty");
        prop_assert!(debug_str.len() <= 20000, "Debug string should be reasonable length");

        // Debug should contain more structure than Display
        prop_assert!(
            debug_str.len() >= display_str.len() || debug_str.contains("Processing"),
            "Debug string should contain type information or be more detailed"
        );
    }
}

// Helper trait for the is_valid_utf8 check
trait Utf8Validator {
    fn is_valid_utf8(&self) -> bool;
}

impl Utf8Validator for String {
    fn is_valid_utf8(&self) -> bool {
        // String is always valid UTF-8 in Rust
        true
    }
}
