use sinex_node_sdk::error_helpers::{
    io_error_with_context, json_error_with_context, processing_error, processing_error_fmt,
    utf8_error_with_context,
};
use sinex_node_sdk::SinexError;
use std::io::ErrorKind;
use xtask::sandbox::prelude::*;

#[sinex_test]
fn io_error_with_context_includes_message() -> TestResult<()> {
    let cases = [
        (ErrorKind::NotFound, "File not found error"),
        (ErrorKind::PermissionDenied, "Permission error"),
        (ErrorKind::ConnectionRefused, "Network error"),
        (ErrorKind::TimedOut, "Timeout error"),
        (ErrorKind::InvalidData, "Data validation error"),
    ];

    for (kind, context) in cases {
        let err = std::io::Error::new(kind, "original error message");
        match io_error_with_context(err, context) {
            SinexError::processing(message) => {
                assert!(message.contains(context));
                assert!(message.contains("original error message"));
            }
            other => panic!("Expected Processing error, got {other:?}"),
        }
    }

    Ok(())
}

#[sinex_test]
fn io_error_with_empty_context_still_includes_source() -> TestResult<()> {
    let err = std::io::Error::new(ErrorKind::NotFound, "test error");
    match io_error_with_context(err, "") {
        SinexError::processing(message) => assert!(message.contains("test error")),
        other => panic!("Expected Processing error, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
fn utf8_error_context_describes_failure() -> TestResult<()> {
    let bad_bytes = vec![0xFF, 0xFE, 0xFD];
    let utf8_error = String::from_utf8(bad_bytes).unwrap_err();
    match utf8_error_with_context(utf8_error, "Failed to decode response") {
        SinexError::processing(message) => {
            assert!(message.contains("Failed to decode response"));
            assert!(message.to_lowercase().contains("utf-8"));
        }
        other => panic!("Expected Processing error, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
fn json_error_context_preserves_details() -> TestResult<()> {
    let invalid_cases = [
        ("{invalid_json}", "Malformed JSON object"),
        ("[1, 2, 3,]", "Trailing comma in array"),
        ("\"unclosed string", "Unclosed string literal"),
        ("{\"key\": }", "Missing value"),
        ("null extra", "Extra tokens after null"),
    ];

    for (json_str, desc) in invalid_cases {
        let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
        match json_error_with_context(json_error, "Config parsing failed") {
            SinexError::processing(message) => {
                assert!(message.contains("Config parsing failed"), "Case: {desc}");
                assert!(message.len() > "Config parsing failed: ".len());
            }
            other => panic!("Expected Processing error for {desc}, got {other:?}"),
        }
    }

    Ok(())
}

#[sinex_test]
fn processing_error_helpers_round_trip_messages() -> TestResult<()> {
    match processing_error("Something went wrong") {
        SinexError::processing(message) => assert_eq!(message, "Something went wrong"),
        other => panic!("Expected Processing error, got {other:?}"),
    }

    match processing_error_fmt(format_args!("Value {} is invalid", 42)) {
        SinexError::processing(message) => assert_eq!(message, "Value 42 is invalid"),
        other => panic!("Expected Processing error, got {other:?}"),
    }

    let special = "Error: 100% failed with UTF-8 chars: ñ, é, 中文";
    match processing_error(special) {
        SinexError::processing(message) => assert_eq!(message, special),
        other => panic!("Expected Processing error, got {other:?}"),
    }

    Ok(())
}

#[sinex_test]
fn error_chain_context_is_preserved() -> TestResult<()> {
    let original = std::io::Error::new(ErrorKind::NotFound, "file.txt");
    let error_string = io_error_with_context(original, "Config loading").to_string();
    assert!(error_string.contains("Config loading"));
    assert!(error_string.contains("file.txt"));
    Ok(())
}

#[sinex_test]
fn error_helpers_handle_empty_json() -> TestResult<()> {
    let json_error = serde_json::from_str::<serde_json::Value>("").unwrap_err();
    match json_error_with_context(json_error, "Empty config") {
        SinexError::processing(message) => {
            assert!(message.contains("Empty config"));
            assert!(message.len() > "Empty config: ".len());
        }
        other => panic!("Expected Processing error, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
fn error_display_and_debug_include_context() -> TestResult<()> {
    let error = processing_error("Test error message");
    let display_str = format!("{}", error);
    assert!(display_str.contains("Test error message"));

    let debug_str = format!("{:?}", error);
    assert!(debug_str.contains("Processing"));
    assert!(debug_str.contains("Test error message"));
    Ok(())
}
