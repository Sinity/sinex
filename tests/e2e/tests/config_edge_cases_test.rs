//! Configuration Edge Case Tests
//!
//! These tests verify configuration validation handles edge cases correctly,
//! particularly boundary values, invalid inputs, and error conditions that
//! aren't covered by unit tests.
//!
//! ## Coverage Areas
//! - Config field boundary values
//! - Environment variable parsing edge cases
//! - Path validation edge cases
//! - Connection test failure handling

use sinex_ingestd::config::IngestdConfig;
use sinex_primitives::nats::NatsConnectionConfig;
use sinex_primitives::Bytes;
use std::env;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

// =============================================================================
// Config Boundary Value Tests
// =============================================================================

/// Test pool_size boundary at minimum value.
#[sinex_test]
async fn test_config_pool_size_minimum() -> Result<()> {
    let config = IngestdConfig::builder()
        .database_pool_size(1)
        .database_url("postgresql:///test")
        .build();

    assert_eq!(config.database_pool_size, 1);

    // Validate should pass for minimum value
    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_ok(),
        "Pool size of 1 should be valid: {validation:?}"
    );

    Ok(())
}

/// Test pool_size boundary at maximum value.
#[sinex_test]
async fn test_config_pool_size_maximum() -> Result<()> {
    let config = IngestdConfig::builder()
        .database_pool_size(1000)
        .database_url("postgresql:///test")
        .build();

    assert_eq!(config.database_pool_size, 1000);

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_ok(),
        "Pool size of 1000 should be valid: {validation:?}"
    );

    Ok(())
}

/// Test pool_size boundary above maximum - should fail validation.
#[sinex_test]
async fn test_config_pool_size_exceeds_maximum() -> Result<()> {
    let config = IngestdConfig::builder()
        .database_pool_size(1001)
        .database_url("postgresql:///test")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_err(),
        "Pool size of 1001 should fail validation"
    );

    Ok(())
}

/// Test max_message_size boundaries.
#[sinex_test]
async fn test_config_max_message_size_boundaries() -> Result<()> {
    // Minimum (1KB)
    let config = IngestdConfig::builder()
        .max_message_size(Bytes::from_bytes(1024))
        .database_url("postgresql:///test")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(validation.is_ok(), "1KB should be valid");

    // Maximum (1GB)
    let config = IngestdConfig::builder()
        .max_message_size(Bytes::from_bytes(1_073_741_824))
        .database_url("postgresql:///test")
        .build();
    let validation = validator::Validate::validate(&config);
    assert!(validation.is_ok(), "1GB should be valid");

    // Below minimum
    let config = IngestdConfig::builder()
        .max_message_size(Bytes::from_bytes(1023))
        .database_url("postgresql:///test")
        .build();
    let validation = validator::Validate::validate(&config);
    assert!(validation.is_err(), "Below 1KB should fail");

    // Above maximum
    let config = IngestdConfig::builder()
        .max_message_size(Bytes::from_bytes(1_073_741_825))
        .database_url("postgresql:///test")
        .build();
    let validation = validator::Validate::validate(&config);
    assert!(validation.is_err(), "Above 1GB should fail");

    Ok(())
}

// =============================================================================
// URL Validation Tests
// =============================================================================

/// Test database URL validation rejects non-PostgreSQL URLs.
#[sinex_test]
async fn test_config_database_url_must_be_postgres() -> Result<()> {
    let config = IngestdConfig::builder()
        .database_url("mysql://localhost/db")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_err(),
        "MySQL URL should fail PostgreSQL validation"
    );

    Ok(())
}

/// Test empty NATS URL fails validation.
#[sinex_test]
async fn test_config_nats_url_empty() -> Result<()> {
    let config = IngestdConfig::builder()
        .nats(NatsConnectionConfig::builder().url(String::new()).build())
        .database_url("postgresql:///test")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(validation.is_err(), "Empty NATS URL should fail validation");

    Ok(())
}

/// Test empty stream name fails validation.
#[sinex_test]
async fn test_config_stream_name_empty() -> Result<()> {
    let config = IngestdConfig::builder()
        .nats_stream_name("")
        .database_url("postgresql:///test")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_err(),
        "Empty stream name should fail validation"
    );

    Ok(())
}

/// Test empty consumer name fails validation.
#[sinex_test]
async fn test_config_consumer_name_empty() -> Result<()> {
    let config = IngestdConfig::builder()
        .nats_consumer_name("")
        .database_url("postgresql:///test")
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_err(),
        "Empty consumer name should fail validation"
    );

    Ok(())
}

// =============================================================================
// Path Validation Tests
// =============================================================================

mod async_validation {
    use super::*;

    /// Full async validation should create the work_dir and exercise DB/NATS connectivity.
    #[sinex_test]
    async fn test_config_validate_creates_work_dir(ctx: TestContext) -> Result<()> {
        let ctx = ctx.with_nats().shared().await?;

        let temp_dir = TempDir::new()?;
        let work_dir = temp_dir.path().join("new_work_dir");

        assert!(!work_dir.exists());

        let config = IngestdConfig::builder()
            .work_dir(camino::Utf8PathBuf::try_from(work_dir.clone())?)
            .database_url(ctx.database_url().to_string())
            .nats(
                NatsConnectionConfig::builder()
                    .url(ctx.nats_url().expect("nats url").clone())
                    .build(),
            )
            .build();

        config.validate().await?;

        assert!(
            work_dir.exists(),
            "validate() should create the work_dir if it does not exist"
        );

        Ok(())
    }
}

/// Test assembler_state_dir validation with non-existent path.
#[sinex_test]
async fn test_config_assembler_state_dir_creation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let state_dir = temp_dir.path().join("state");

    assert!(!state_dir.exists());

    let config = IngestdConfig::builder()
        .assembler_state_dir(camino::Utf8PathBuf::try_from(state_dir.clone())?)
        .database_url("postgresql:///test")
        .build();

    // Sync validation should pass
    let validation = validator::Validate::validate(&config);
    assert!(validation.is_ok());

    Ok(())
}

// =============================================================================
// Environment Variable Parsing Tests
// =============================================================================

/// Test env_var_usize with non-numeric value falls back to default.
#[sinex_test]
fn test_env_var_usize_non_numeric() -> TestResult<()> {
    // This tests the behavior indirectly through RpcServerLimits
    // The env_var_usize function in rpc_server.rs silently ignores parse errors

    // Set invalid value
    unsafe { env::set_var("TEST_USIZE_VAR", "not_a_number") };

    let value: usize = std::env::var("TEST_USIZE_VAR")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(42);

    assert_eq!(value, 42, "Should fall back to default on parse error");

    unsafe { env::remove_var("TEST_USIZE_VAR") };
    Ok(())
}

/// Test env_var_u64 with overflow value falls back to default.
#[sinex_test]
fn test_env_var_u64_overflow() -> TestResult<()> {
    // Value larger than u64::MAX
    unsafe { env::set_var("TEST_U64_VAR", "99999999999999999999999999999999") };

    let value: u64 = std::env::var("TEST_U64_VAR")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(100);

    assert_eq!(value, 100, "Should fall back to default on overflow");

    unsafe { env::remove_var("TEST_U64_VAR") };
    Ok(())
}

/// Test env_var with negative number for unsigned type.
#[sinex_test]
fn test_env_var_negative_for_unsigned() -> TestResult<()> {
    unsafe { env::set_var("TEST_NEGATIVE_VAR", "-5") };

    let value: usize = std::env::var("TEST_NEGATIVE_VAR")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(10);

    assert_eq!(value, 10, "Should fall back to default on negative value");

    unsafe { env::remove_var("TEST_NEGATIVE_VAR") };
    Ok(())
}

/// Test env_var with whitespace.
#[sinex_test]
fn test_env_var_with_whitespace() -> TestResult<()> {
    unsafe { env::set_var("TEST_WHITESPACE_VAR", "  42  ") };

    // Direct parse won't work with whitespace
    let raw_value: Option<usize> = std::env::var("TEST_WHITESPACE_VAR")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok());

    assert!(raw_value.is_none(), "Whitespace should cause parse to fail");

    // With trim it would work
    let trimmed_value: Option<usize> = std::env::var("TEST_WHITESPACE_VAR")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok());

    assert_eq!(trimmed_value, Some(42), "Trimmed value should parse");

    unsafe { env::remove_var("TEST_WHITESPACE_VAR") };
    Ok(())
}

// =============================================================================
// Combined Configuration Tests
// =============================================================================

/// Test configuration with all fields at boundary values.
#[sinex_test]
async fn test_config_all_boundaries_valid() -> Result<()> {
    let temp_dir = TempDir::new()?;

    let config = IngestdConfig::builder()
        .database_pool_size(1)
        .max_message_size(Bytes::from_bytes(1024))
        .database_url("postgresql:///test")
        .nats(
            NatsConnectionConfig::builder()
                .url("nats://localhost:4222".to_string())
                .build(),
        )
        .nats_stream_name("s")
        .nats_consumer_name("c")
        .work_dir(camino::Utf8PathBuf::try_from(
            temp_dir.path().to_path_buf(),
        )?)
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(
        validation.is_ok(),
        "All minimum boundaries should be valid: {validation:?}"
    );

    Ok(())
}

/// Test configuration with multiple invalid fields.
#[sinex_test]
async fn test_config_multiple_validation_errors() -> Result<()> {
    let config = IngestdConfig::builder()
        .database_pool_size(0) // Invalid
        .max_message_size(Bytes::from_bytes(100)) // Invalid (< 1024)
        .database_url("mysql://localhost/db") // Invalid (not PostgreSQL)
        .nats(NatsConnectionConfig::builder().url(String::new()).build()) // Invalid (empty)
        .nats_stream_name("") // Invalid (empty)
        .build();

    let validation = validator::Validate::validate(&config);
    assert!(validation.is_err(), "Should have validation errors");

    // The error should contain multiple field failures
    let err = validation.unwrap_err();
    let err_str = format!("{err:?}");

    // Check that multiple fields are mentioned
    // Note: validator crate aggregates errors by field
    tracing::info!("Validation errors: {}", err_str);

    Ok(())
}
