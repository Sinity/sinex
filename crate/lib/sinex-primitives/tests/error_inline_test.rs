use sinex_primitives::error::{ErrorDetails, SinexError};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_serialization_full_fidelity() -> TestResult<()> {
    let err = ErrorDetails::new("test")
        .with_context("table_name", "users")
        .with_context("nats_url", "nats://localhost:4222")
        .with_context("validation_type", "path")
        .with_context("context", "schema sync failed");

    let json = serde_json::to_value(&err).unwrap();
    let context = json.get("context").unwrap();

    assert!(context.get("table_name").is_some());
    assert!(context.get("nats_url").is_some());
    assert!(context.get("validation_type").is_some());
    assert!(context.get("context").is_some());
    Ok(())
}

#[sinex_test]
async fn test_sources_preserved() -> TestResult<()> {
    let err = ErrorDetails::new("db error")
        .with_source("SELECT * FROM core.events WHERE id = '01HZ...'")
        .with_source("connection to postgresql://user:pass@localhost failed")
        .with_source("timeout after 30s");

    let json = serde_json::to_value(&err).unwrap();
    let sources = json.get("sources").unwrap().as_array().unwrap();

    assert!(sources[0].as_str().unwrap().contains("SELECT"));
    assert!(sources[1].as_str().unwrap().contains("postgresql://"));
    assert_eq!(sources[2], "timeout after 30s");
    Ok(())
}

#[sinex_test]
async fn test_client_message_client_errors_expose_message() -> TestResult<()> {
    let err = SinexError::validation("Event type must not be empty");
    assert_eq!(err.client_message(), "Event type must not be empty");

    let err = SinexError::not_found("Event 01HZ123 not found");
    assert_eq!(err.client_message(), "Event 01HZ123 not found");

    let err = SinexError::permission_denied("Token does not have write access");
    assert_eq!(err.client_message(), "Token does not have write access");
    Ok(())
}

#[sinex_test]
async fn test_client_message_server_errors_are_generic() -> TestResult<()> {
    let err = SinexError::database("SELECT * FROM secrets WHERE id = 1")
        .with_context("nats_url", "nats://internal:4222");
    let msg = err.client_message();
    assert!(
        !msg.contains("SELECT"),
        "SQL must not appear in client message"
    );
    assert!(
        !msg.contains("nats://"),
        "NATS URL must not appear in client message"
    );
    assert_eq!(msg, "A database error occurred");

    let err = SinexError::network("connection refused at nats://10.0.0.1:4222");
    assert_eq!(err.client_message(), "A connectivity error occurred");
    Ok(())
}
