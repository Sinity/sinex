use camino::Utf8Path;
use sinex_primitives::error::{ErrorDetails, Result, ResultExt, SinexError};
use std::collections::HashMap;
use xtask::sandbox::sinex_test;
use xtask::sandbox::TestResult;

#[sinex_test]
fn error_display_matches_variants() -> TestResult<()> {
    let error = SinexError::database("Connection failed");
    assert_eq!(error.to_string(), "Database error: Connection failed");

    let error = SinexError::validation("Invalid input");
    assert_eq!(error.to_string(), "Validation error: Invalid input");
    Ok(())
}

#[sinex_test]
fn error_context_appends_key_values() -> TestResult<()> {
    let error = SinexError::database("Connection failed")
        .with_context("host", "localhost")
        .with_context("port", 5432);

    let error_str = error.to_string();
    assert!(error_str.contains("Connection failed"));
    assert!(error_str.contains("host: localhost"));
    assert!(error_str.contains("port: 5432"));
    Ok(())
}

#[sinex_test]
fn error_sources_chain() -> TestResult<()> {
    let error = SinexError::service("Processing failed")
        .with_source("Database connection timed out")
        .with_source("Network unreachable");

    let error_str = error.to_string();
    assert!(error_str.contains("Processing failed"));
    assert!(error_str.contains("Database connection timed out"));
    assert!(error_str.contains("Network unreachable"));
    Ok(())
}

#[sinex_test]
fn error_categorization_helpers() -> TestResult<()> {
    assert!(SinexError::timeout("test").is_retryable());
    assert!(SinexError::network("test").is_retryable());
    assert!(!SinexError::validation("test").is_retryable());

    assert!(SinexError::validation("test").is_client_error());
    assert!(SinexError::not_found("test").is_client_error());
    assert!(!SinexError::database("test").is_client_error());

    assert!(SinexError::max_retries_exceeded("test").is_permanent());
    assert!(SinexError::permission_denied("test").is_permanent());
    assert!(!SinexError::timeout("test").is_permanent());
    Ok(())
}

#[sinex_test]
fn status_code_mapping_matches_expectations() -> TestResult<()> {
    assert_eq!(SinexError::validation("test").status_code(), 400);
    assert_eq!(SinexError::not_found("test").status_code(), 404);
    assert_eq!(SinexError::permission_denied("test").status_code(), 403);
    assert_eq!(SinexError::timeout("test").status_code(), 408);
    assert_eq!(SinexError::already_exists("test").status_code(), 409);
    assert_eq!(SinexError::resource_exhausted("test").status_code(), 429);
    assert_eq!(SinexError::database("test").status_code(), 500);
    Ok(())
}

#[sinex_test]
fn error_serializes_and_deserializes() -> TestResult<()> {
    // Note: Context keys must be in the SAFE_KEYS whitelist to be serialized
    let error = SinexError::database("Connection failed")
        .with_context("table_name", "users")
        .with_context("retry_count", 3);

    let json = serde_json::to_string(&error).unwrap();
    assert!(json.contains("Database"));
    assert!(json.contains("Connection failed"));

    let deserialized: SinexError = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.to_string(), error.to_string());
    Ok(())
}

#[sinex_test]
fn sinex_error_integrates_with_anyhow() -> TestResult<()> {
    fn returns_anyhow() -> TestResult<()> {
        Err(SinexError::database("test"))?
    }

    let result = returns_anyhow();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Database error"));
    Ok(())
}

#[sinex_test]
fn error_from_common_types() -> TestResult<()> {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let sinex_err: SinexError = io_err.into();
    assert!(matches!(sinex_err, SinexError::Io(_)));

    let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
    let sinex_err: SinexError = json_err.into();
    assert!(matches!(sinex_err, SinexError::Serialization(_)));
    Ok(())
}

#[sinex_test]
fn context_map_preserves_entries() -> TestResult<()> {
    let error = SinexError::database("Connection failed")
        .with_context("attempt", 3)
        .with_context("retry_after", "5s");

    let context = error.context_map();
    assert_eq!(context.get("attempt"), Some(&"3".to_string()));
    assert_eq!(context.get("retry_after"), Some(&"5s".to_string()));
    Ok(())
}

#[sinex_test]
fn context_display_preserves_order() -> TestResult<()> {
    let error = SinexError::validation("Invalid input")
        .with_context("field", "email")
        .with_context("value", "not-an-email")
        .with_context("reason", "missing @ symbol");

    let error_str = error.to_string();
    assert!(error_str.contains("field: email, value: not-an-email, reason: missing @ symbol"));
    Ok(())
}

#[sinex_test]
fn convenience_context_helpers_work() -> TestResult<()> {
    let error = SinexError::io("File operation failed")
        .with_path(Utf8Path::new("/tmp/test.txt"))
        .with_duration(Duration::from_millis(1500))
        .with_context("retry_count", 3)
        .with_id("request_id", "abc123");

    let context = error.context_map();
    assert_eq!(context.get("path"), Some(&"/tmp/test.txt".to_string()));
    assert_eq!(context.get("duration_ms"), Some(&"1500".to_string()));
    assert_eq!(context.get("retry_count"), Some(&"3".to_string()));
    assert_eq!(context.get("request_id"), Some(&"abc123".to_string()));
    Ok(())
}

#[sinex_test]
fn accessor_methods_reflect_state() -> TestResult<()> {
    let error = SinexError::database("Query failed")
        .with_context("table", "users")
        .with_source("Connection timeout");

    assert_eq!(error.message(), "Query failed");
    assert_eq!(error.variant_name(), "Database");
    assert_eq!(error.sources(), &["Connection timeout"]);
    assert_eq!(error.context_map().get("table"), Some(&"users".to_string()));
    Ok(())
}

#[sinex_test]
fn enumerating_error_variants_is_consistent() -> TestResult<()> {
    let errors = vec![
        (SinexError::database("db"), "Database"),
        (SinexError::validation("val"), "Validation"),
        (SinexError::service("svc"), "Service"),
        (SinexError::io("io"), "Io"),
        (SinexError::configuration("cfg"), "Configuration"),
        (SinexError::serialization("ser"), "Serialization"),
        (SinexError::parse("parse"), "Parse"),
        (SinexError::not_found("nf"), "NotFound"),
        (SinexError::already_exists("ae"), "AlreadyExists"),
        (SinexError::invalid_state("is"), "InvalidState"),
        (SinexError::permission_denied("pd"), "PermissionDenied"),
        (SinexError::network("net"), "Network"),
        (SinexError::channel_send("cs"), "ChannelSend"),
        (SinexError::channel_receive("cr"), "ChannelReceive"),
        (SinexError::timeout("to"), "Timeout"),
        (SinexError::cancelled("can"), "Cancelled"),
        (
            SinexError::max_retries_exceeded("mre"),
            "MaxRetriesExceeded",
        ),
        (SinexError::resource_exhausted("re"), "ResourceExhausted"),
        (SinexError::unknown("unk"), "Unknown"),
    ];

    for (error, expected_variant) in errors {
        assert_eq!(error.variant_name(), expected_variant);
    }
    Ok(())
}

#[sinex_test]
fn error_details_display_formats_chain() -> TestResult<()> {
    let details = ErrorDetails::new("Base error")
        .with_context("key1", "value1")
        .with_context("key2", "value2")
        .with_source("Source 1")
        .with_source("Source 2");

    let display = format!("{details}");
    assert!(display.contains("Base error"));
    assert!(display.contains("key1: value1"));
    assert!(display.contains("key2: value2"));
    assert!(display.contains("Caused by:"));
    assert!(display.contains("1: Source 1"));
    assert!(display.contains("2: Source 2"));
    Ok(())
}

#[sinex_test]
fn error_chains_preserve_sources() -> TestResult<()> {
    let error = SinexError::service("Service unavailable")
        .with_source("Database connection failed")
        .with_source("Network unreachable")
        .with_source("DNS resolution failed");

    assert_eq!(error.sources().len(), 3);
    assert_eq!(error.sources()[0], "Database connection failed");
    assert_eq!(error.sources()[1], "Network unreachable");
    assert_eq!(error.sources()[2], "DNS resolution failed");
    Ok(())
}

#[sinex_test]
fn result_ext_context_adds_message() -> TestResult<()> {
    fn failing_operation() -> std::result::Result<(), std::io::Error> {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
    }

    let result: Result<()> = ResultExt::context(failing_operation(), "Operation failed");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, SinexError::Io(_)));
    assert_eq!(
        err.context_map().get("context"),
        Some(&"Operation failed".to_string())
    );
    Ok(())
}

#[sinex_test]
fn result_ext_with_context_builds_custom_error() -> TestResult<()> {
    fn failing_operation() -> std::result::Result<(), std::io::Error> {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "test"))
    }

    let result: Result<()> = ResultExt::with_context(failing_operation(), || {
        SinexError::service("Custom error").with_context("component", "test-component")
    });

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, SinexError::Service(_)));
    assert!(err.to_string().contains("Custom error"));
    assert_eq!(
        err.context_map().get("component"),
        Some(&"test-component".to_string())
    );
    Ok(())
}

#[sinex_test]
fn error_serialization_roundtrip_preserves_context() -> TestResult<()> {
    // Note: Context keys must be in the SAFE_KEYS whitelist to be serialized
    let original = SinexError::database("Connection failed")
        .with_context("table_name", "users")
        .with_context("retry_count", 5)
        .with_source("Network timeout")
        .with_source("DNS failed");

    let json = serde_json::to_string(&original).unwrap();
    let deserialized: SinexError = serde_json::from_str(&json).unwrap();

    assert_eq!(original.message(), deserialized.message());
    assert_eq!(original.variant_name(), deserialized.variant_name());
    assert_eq!(original.sources(), deserialized.sources());
    assert_eq!(original.context_map(), deserialized.context_map());
    Ok(())
}

#[sinex_test]
fn empty_context_serializes_cleanly() -> TestResult<()> {
    let error = SinexError::validation("Simple error");
    let json = serde_json::to_string(&error).unwrap();
    assert!(!json.contains("context"));
    assert!(!json.contains("sources"));

    let deserialized: SinexError = serde_json::from_str(&json).unwrap();
    assert!(deserialized.context_map().is_empty());
    assert!(deserialized.sources().is_empty());
    Ok(())
}

#[sinex_test]
fn operation_helper_sets_context() -> TestResult<()> {
    let error = SinexError::database("Query failed").with_operation("user.find_by_id");
    assert_eq!(
        error.context_map().get("operation"),
        Some(&"user.find_by_id".to_string())
    );
    Ok(())
}

#[sinex_test]
fn error_conversion_chain_preserves_message() -> TestResult<()> {
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");
    let sinex_error: SinexError = io_error.into();

    assert!(matches!(sinex_error, SinexError::Io(_)));
    assert!(sinex_error.message().contains("Access denied"));
    Ok(())
}

#[sinex_test]
async fn channel_error_conversions_work() -> TestResult<()> {
    use tokio::sync::{mpsc, oneshot};

    let (tx, rx) = mpsc::channel::<i32>(1);
    drop(rx);
    if let Err(e) = tx.send(42).await {
        let sinex_err: SinexError = e.into();
        assert!(matches!(sinex_err, SinexError::ChannelSend(_)));
    }

    let (tx, rx) = oneshot::channel::<i32>();
    drop(tx);
    let err = rx
        .await
        .expect_err("oneshot should error after sender drop");
    let _sinex_err: SinexError = err.into();
    Ok(())
}

#[sinex_test]
fn cloned_errors_retain_data() -> TestResult<()> {
    let error = SinexError::validation("Test error")
        .with_context("field", "email")
        .with_source("Invalid format");

    let cloned = error.clone();
    assert_eq!(error.message(), cloned.message());
    assert_eq!(error.variant_name(), cloned.variant_name());
    assert_eq!(error.context_map(), cloned.context_map());
    assert_eq!(error.sources(), cloned.sources());
    Ok(())
}

#[sinex_test]
fn complex_context_values_are_supported() -> TestResult<()> {
    let mut map = HashMap::new();
    map.insert("key", "value");

    let error = SinexError::service("Processing failed")
        .with_context("json", serde_json::json!({"nested": {"value": 42}}))
        .with_context("array", format!("{:?}", vec![1, 2, 3]))
        .with_context("map", format!("{map:?}"));

    let context = error.context_map();
    assert!(context.get("json").unwrap().contains("nested"));
    assert_eq!(context.get("array").unwrap(), "[1, 2, 3]");
    assert!(context.get("map").unwrap().contains("key"));
    Ok(())
}

#[sinex_test]
fn indexmap_preserves_insertion_order() -> TestResult<()> {
    let error = SinexError::validation("Test")
        .with_context("a", "1")
        .with_context("b", "2")
        .with_context("c", "3")
        .with_context("d", "4");

    let keys: Vec<_> = error.context_map().keys().collect();
    assert_eq!(keys, vec!["a", "b", "c", "d"]);
    Ok(())
}

#[sinex_test]
fn error_edge_cases_still_behave() -> TestResult<()> {
    let error = SinexError::unknown("");
    assert_eq!(error.message(), "");

    let long_msg = "x".repeat(10000);
    let error = SinexError::unknown(&long_msg);
    assert_eq!(error.message().len(), 10000);

    let error = SinexError::parse("Failed")
        .with_context("emoji", "🦀")
        .with_context("chinese", "你好")
        .with_context("arabic", "مرحبا");

    assert_eq!(error.context_map().get("emoji"), Some(&"🦀".to_string()));
    assert_eq!(
        error.context_map().get("chinese"),
        Some(&"你好".to_string())
    );
    assert_eq!(
        error.context_map().get("arabic"),
        Some(&"مرحبا".to_string())
    );
    Ok(())
}
