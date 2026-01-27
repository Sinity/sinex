use sinex_core::types::validation::validation_chains::{DatabaseConfig, EventValidation};
use sinex_core::validation::validation_chains::ValidateExt;
use validator::Validate;

use xtask::sandbox::sinex_test;

#[sinex_test]
fn database_config_validation_flags_invalid_fields() -> TestResult<()> {
    let valid_config = DatabaseConfig {
        connection_url: "postgresql://user:pass@localhost/db".to_string(),
        max_connections: 100,
        connection_timeout_ms: 5000,
        database_name: "test_db".to_string(),
    };
    assert!(valid_config.validate().is_ok());

    let invalid_config = DatabaseConfig {
        connection_url: "not-a-url".to_string(),
        max_connections: 2000,
        connection_timeout_ms: 0,
        database_name: "".to_string(),
    };

    let result = invalid_config.validate().unwrap_err();
    assert!(result.field_errors().contains_key("connection_url"));
    assert!(result.field_errors().contains_key("max_connections"));
    assert!(result.field_errors().contains_key("database_name"));
    Ok(())
}

#[sinex_test]
fn event_validation_schema_enforces_rules() -> TestResult<()> {
    let valid_event = EventValidation {
        event_type: "user.created".to_string(),
        source: "api".to_string(),
        host: "api-server-01".to_string(),
        contact_email: Some("admin@example.com".to_string()),
    };
    assert!(valid_event.validate().is_ok());

    let invalid_event = EventValidation {
        event_type: "a".repeat(101),
        source: "".to_string(),
        host: "..".to_string(),
        contact_email: Some("not-an-email".to_string()),
    };
    assert!(invalid_event.validate().is_err());
    Ok(())
}

#[sinex_test]
fn friendly_error_formatting_mentions_fields() -> TestResult<()> {
    let config = DatabaseConfig {
        connection_url: "invalid".to_string(),
        max_connections: 0,
        connection_timeout_ms: 0,
        database_name: "".to_string(),
    };

    let message = config
        .validate_friendly()
        .expect_err("validation should fail");

    assert!(message.contains("connection_url"));
    assert!(message.contains("max_connections"));
    assert!(message.contains("database_name"));
    Ok(())
}
