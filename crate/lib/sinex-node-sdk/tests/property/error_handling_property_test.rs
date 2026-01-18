//! Property tests for error handling and conversion utilities.

use proptest::prelude::*;
use sinex_node_sdk::error_helpers::*;
use sinex_test_utils::sinex_proptest;
use std::io::ErrorKind;

fn arbitrary_error_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-zA-Z0-9 .,!?-]{1,100}".prop_map(|s| s.to_string()),
        Just("Error with \n newlines".to_string()),
        Just("Error with 中文 unicode".to_string()),
        Just("Error with emoji 🚨".to_string()),
    ]
}

fn arbitrary_context() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-zA-Z0-9 _-]{0,50}".prop_map(|s| s.to_string())
    ]
}

fn arbitrary_error_kind() -> impl Strategy<Value = ErrorKind> {
    prop_oneof![
        Just(ErrorKind::NotFound),
        Just(ErrorKind::PermissionDenied),
        Just(ErrorKind::ConnectionRefused),
        Just(ErrorKind::TimedOut),
        Just(ErrorKind::InvalidData),
        Just(ErrorKind::Interrupted),
        Just(ErrorKind::Other),
    ]
}

/// Generate byte sequences that are guaranteed to be invalid UTF-8.
fn invalid_utf8_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Lone continuation byte (0x80-0xBF)
        (0x80u8..=0xBFu8).prop_map(|b| vec![b]),
        // Incomplete 2-byte sequence (0xC2-0xDF without continuation)
        (0xC2u8..=0xDFu8).prop_map(|b| vec![b]),
        // Incomplete 3-byte sequence (0xE0-0xEF with only one continuation)
        (0xE0u8..=0xEFu8, 0x80u8..=0xBFu8).prop_map(|(a, b)| vec![a, b]),
        // Incomplete 4-byte sequence (0xF0-0xF4 with only two continuations)
        (0xF0u8..=0xF4u8, 0x80u8..=0xBFu8, 0x80u8..=0xBFu8).prop_map(|(a, b, c)| vec![a, b, c]),
        // Overlong encoding for NULL (0xC0 0x80)
        Just(vec![0xC0u8, 0x80u8]),
        // Invalid byte 0xFF
        Just(vec![0xFFu8]),
    ]
}

sinex_proptest! {
    fn io_error_with_context_preserves_message(
        kind in arbitrary_error_kind(),
        msg in arbitrary_error_message(),
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        let io_error = std::io::Error::new(kind, msg.clone());
        let node_error = io_error_with_context(io_error, &ctx);

        if let sinex_node_sdk::NodeError::Processing(rendered) = node_error {
            // The rendered message should not be empty (at minimum ": " from formatting)
            prop_assert!(!rendered.is_empty());
            // Context should be preserved when provided
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
            // When the message is non-empty, it should appear in rendered output
            if !msg.is_empty() {
                prop_assert!(
                    rendered.contains(&msg),
                    "Error message should contain original: {} not in {}", msg, rendered
                );
            }
        } else {
            prop_assert!(false, "expected processing error variant");
        }
        Ok(())
    }

    fn utf8_error_context_is_preserved(
        bytes in invalid_utf8_bytes(),
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        // bytes is guaranteed to be invalid UTF-8
        let err = String::from_utf8(bytes).expect_err("strategy should generate invalid UTF-8");
        let node_error = utf8_error_with_context(err, &ctx);
        if let sinex_node_sdk::NodeError::Processing(rendered) = node_error {
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
        } else {
            prop_assert!(false, "expected processing error variant");
        }
        Ok(())
    }

    fn json_error_with_context_marked(
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        let malformed = "{\"key\":}";
        let err = serde_json::from_str::<serde_json::Value>(malformed).unwrap_err();
        let node_error = json_error_with_context(err, &ctx);
        if let sinex_node_sdk::NodeError::Processing(rendered) = node_error {
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
            // The error message should contain parsing error details (line/column info)
            prop_assert!(
                rendered.contains("line") || rendered.contains("column") || rendered.contains("expected"),
                "JSON error should contain parsing details: {}", rendered
            );
        } else {
            prop_assert!(false, "expected processing error variant");
        }
        Ok(())
    }
}
