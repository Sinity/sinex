//! Property tests for error handling and conversion utilities.

use proptest::prelude::*;
use sinex_node_sdk::error_helpers::*;
use sinex_primitives::error::SinexError;
use std::io::ErrorKind;
use xtask::sandbox::sinex_proptest;

fn arbitrary_error_message() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-zA-Z0-9 .,!?-]{1,100}".prop_map(|s| s),
        Just("Error with \n newlines".to_string()),
        Just("Error with 中文 unicode".to_string()),
        Just("Error with emoji 🚨".to_string()),
    ]
}

fn arbitrary_context() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[a-zA-Z0-9 _-]{0,50}".prop_map(|s| s)]
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

sinex_proptest! {
    fn io_error_with_context_preserves_message(
        kind in arbitrary_error_kind(),
        msg in arbitrary_error_message(),
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        let io_error = std::io::Error::new(kind, msg.clone());
        let node_error = io_error_with_context(io_error, &ctx);

        if let SinexError::Processing(details) = node_error {
            let rendered = details.message();
            // Should at least contain the separator if both are empty,
            // or the content of whichever is non-empty.
            prop_assert!(!rendered.is_empty());

            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }

            if !msg.is_empty() {
                prop_assert!(rendered.contains(&msg));
            }
        } else {
            prop_assert!(false, "expected processing error variant");
        }
        Ok(())
    }

    fn utf8_error_context_is_preserved(
        // Use a strategy that is more likely to produce invalid UTF-8
        bytes in proptest::collection::vec(0..=255u8, 1..32),
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        if let Err(_err) = std::str::from_utf8(&bytes) {
            let node_error = utf8_error_with_context(
                String::from_utf8(bytes).unwrap_err(),
                &ctx
            );
            if let SinexError::Processing(details) = node_error {
                let rendered = details.message();
                if !ctx.is_empty() {
                    prop_assert!(rendered.contains(&ctx));
                }
                prop_assert!(!rendered.is_empty());
            } else {
                prop_assert!(false, "expected processing error variant");
            }
        }
        Ok(())
    }

    fn json_error_with_context_marked(
        ctx in arbitrary_context()
    ) -> TestResult<()> {
        let malformed = "{\"key\":}";
        let err = serde_json::from_str::<serde_json::Value>(malformed).unwrap_err();
        let node_error = json_error_with_context(err, &ctx);
        if let SinexError::Processing(details) = node_error {
            let rendered = details.message();
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
            // Serde error messages are descriptive enough
            prop_assert!(!rendered.is_empty());
        } else {
            prop_assert!(false, "expected processing error variant");
        }
        Ok(())
    }
}
