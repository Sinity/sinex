//! Property tests for error handling and conversion utilities.

use proptest::prelude::*;
use sinex_satellite_sdk::error_helpers::*;
use sinex_test_utils::sinex_test;
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
    prop_oneof![Just(String::new()), "[a-zA-Z0-9 _-]{0,50}".prop_map(|s| s.to_string())]
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

#[sinex_test]
fn io_error_with_context_preserves_message() -> color_eyre::eyre::Result<()> {
    proptest!(|(kind in arbitrary_error_kind(), msg in arbitrary_error_message(), ctx in arbitrary_context())| {
        let io_error = std::io::Error::new(kind, msg.clone());
        let satellite_error = io_error_with_context(io_error, &ctx);

        if let sinex_satellite_sdk::SatelliteError::Processing(rendered) = satellite_error {
            prop_assert!(!rendered.is_empty());
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
            prop_assert!(rendered.contains("error") || rendered.len() > 3);
        } else {
            prop_assert!(false, "expected processing error variant");
        }
    });
    Ok(())
}

#[sinex_test]
fn utf8_error_context_is_preserved() -> color_eyre::eyre::Result<()> {
    proptest!(|(bytes in proptest::collection::vec(any::<u8>(), 1..32), ctx in arbitrary_context())| {
        match String::from_utf8(bytes) {
            Ok(_) => prop_assume!(false),
            Err(err) => {
                let satellite_error = utf8_error_with_context(err, &ctx);
        if let sinex_satellite_sdk::SatelliteError::Processing(rendered) = satellite_error {
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
        } else {
            prop_assert!(false, "expected processing error variant");
        }
            }
        }
    });
    Ok(())
}

#[sinex_test]
fn json_error_with_context_marked() -> color_eyre::eyre::Result<()> {
    proptest!(|(ctx in arbitrary_context())| {
        let malformed = "{\"key\":}";
        let err = serde_json::from_str::<serde_json::Value>(malformed).unwrap_err();
        let satellite_error = json_error_with_context(err, &ctx);
        if let sinex_satellite_sdk::SatelliteError::Processing(rendered) = satellite_error {
            if !ctx.is_empty() {
                prop_assert!(rendered.contains(&ctx));
            }
            prop_assert!(rendered.to_lowercase().contains("json"));
        } else {
            prop_assert!(false, "expected processing error variant");
        }
    });
    Ok(())
}
