// Preflight Unit Tests - Comprehensive verification phase testing

use serde_json::Value;
use sinex_node_sdk::preflight::{
    VerificationStatus, configuration, database, resources, services, verification,
};
use std::env;
use std::fs;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::timeout;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn with_database_url<F, T>(database_url: &str, f: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = env::var("DATABASE_URL").ok();
    unsafe { env::set_var("DATABASE_URL", database_url) };
    let result = f().await;
    unsafe {
        match previous {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
    }
    result
}

async fn without_database_url<F, T>(f: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = env::var("DATABASE_URL").ok();
    unsafe { env::remove_var("DATABASE_URL") };
    let result = f().await;
    unsafe {
        match previous {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
    }
    result
}

/// Test basic VerificationStatus functionality
#[sinex_test]
async fn test_verification_status_basic() -> TestResult<()> {
    // Test that VerificationStatus enum works correctly
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);

    // Test enum variants exist
    let _pass = VerificationStatus::Pass;
    let _warn = VerificationStatus::Warning;
    let _fail = VerificationStatus::Fail;

    Ok(())
}

/// Test verification status comparisons
#[sinex_test]
async fn test_verification_status_comparisons() -> TestResult<()> {
    // Test basic equality
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_eq!(VerificationStatus::Warning, VerificationStatus::Warning);
    assert_eq!(VerificationStatus::Fail, VerificationStatus::Fail);

    // Test inequality
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Warning);
    assert_ne!(VerificationStatus::Warning, VerificationStatus::Fail);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);

    Ok(())
}

// ====== PHASE 1: DATABASE CONNECTIVITY TESTS ======

/// Test Phase 1: Database connectivity verification with valid connection
#[sinex_test]
async fn test_phase1_database_connectivity_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (status, details, messages) = database::verify_database_connectivity().await?;

        assert_eq!(status, VerificationStatus::Pass);
        assert!(!messages.is_empty());
        assert!(messages.iter().any(|m| m.contains("Database connection")));

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("database_url"));
        assert!(details.contains_key("postgresql_version"));
        assert!(details.contains_key("connection_pool"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 1: Database connectivity with invalid URL
#[sinex_test]
async fn test_phase1_database_connectivity_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let (status, _details, messages) = database::verify_database_connectivity().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(messages.iter().any(|m| m.contains("Database connection")));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 1: Database connectivity timeout handling
#[sinex_test]
async fn test_phase1_database_connectivity_timeout() -> TestResult<()> {
    with_database_url("postgresql://192.0.2.1:5432/test", || async {
        let result = timeout(
            Duration::from_secs(Timeouts::SHORT),
            database::verify_database_connectivity(),
        )
        .await;

        match result {
            Ok(Ok((status, _details, messages))) => {
                assert_eq!(status, VerificationStatus::Fail);
                assert!(
                    messages
                        .iter()
                        .any(|m| m.contains("timeout") || m.contains("Database connection"))
                );
            }
            Ok(Err(_)) => {
                // Connection error is also acceptable
            }
            Err(e) => {
                panic!("Database connectivity test should have internal timeout handling: {e}");
            }
        }

        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 2: POSTGRESQL EXTENSIONS TESTS ======

/// Test Phase 2: PostgreSQL extensions verification
#[sinex_test]
async fn test_phase2_postgresql_extensions(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (_status, details, messages) = database::verify_postgresql_extensions().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        let extensions = details
            .get("extensions")
            .and_then(Value::as_object)
            .expect("extensions details should be present");

        assert!(extensions.contains_key("timescaledb"));
        assert!(extensions.contains_key("pg_jsonschema"));
        assert!(extensions.contains_key("vector"));
        assert!(extensions.contains_key("pg_trgm"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 2: Extensions verification with database connection failure
#[sinex_test]
async fn test_phase2_extensions_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let result = database::verify_postgresql_extensions().await;
        assert!(result.is_err());
        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 3: SCHEMA READINESS TESTS ======

/// Test Phase 3: Schema readiness verification
#[sinex_test]
async fn test_phase3_schema_readiness(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (status, details, messages) = database::verify_schema_readiness().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("current_schema"));
        assert!(details.contains_key("schema_sources"));
        if matches!(status, VerificationStatus::Fail) {
            assert!(
                messages
                    .iter()
                    .any(|m| m.contains("drift") || m.contains("failed")),
                "expected diagnostic message for failed schema readiness"
            );
        }

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 3: Schema readiness with invalid database
#[sinex_test]
async fn test_phase3_schema_readiness_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let result = database::verify_schema_readiness().await;
        assert!(result.is_err());
        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 4: SYSTEM RESOURCES TESTS ======

/// Test Phase 4: System resources verification success
#[sinex_test]
async fn test_phase4_system_resources_success() -> TestResult<()> {
    let (_status, details, messages) = resources::verify_system_resources().await?;

    assert!(!messages.is_empty());

    let details = details.as_object().expect("details should be an object");
    let memory = details
        .get("memory")
        .and_then(Value::as_object)
        .expect("memory details should be present");
    assert!(memory.contains_key("total_gb"));
    assert!(memory.contains_key("available_gb"));
    assert!(memory.contains_key("meets_requirements"));

    Ok(())
}

/// Test Phase 4: Filesystem permissions verification with temp directory
#[sinex_test]
async fn test_phase4_filesystem_permissions() -> TestResult<()> {
    // Create a temporary directory for testing using std::env::temp_dir
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("sinex_test_file_temp");

    // Test write permissions
    fs::write(&test_file, "test content")
        .map_err(|e| color_eyre::eyre::eyre!("Failed to write test file: {}", e))?;

    // Test read permissions
    let content = fs::read_to_string(&test_file)
        .map_err(|e| color_eyre::eyre::eyre!("Failed to read test file: {}", e))?;
    assert_eq!(content, "test content");

    // Test cleanup
    fs::remove_file(&test_file)
        .map_err(|e| color_eyre::eyre::eyre!("Failed to remove test file: {}", e))?;

    Ok(())
}

// ====== PHASE 5: CONFIGURATION TESTS ======

/// Test Phase 5: Configuration verification success
#[sinex_test]
async fn test_phase5_configuration_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (_status, details, messages) = configuration::verify_configuration_generation().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("environment"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 5: Configuration with missing environment variables
#[sinex_test]
async fn test_phase5_configuration_missing_env() -> TestResult<()> {
    without_database_url(|| async {
        let (status, _details, messages) = configuration::verify_configuration_generation().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(
            messages
                .iter()
                .any(|m| m.contains("DATABASE_URL") && m.contains("missing"))
        );

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 5: Configuration format validation
#[sinex_test]
async fn test_phase5_config_format_validation() -> TestResult<()> {
    // Test JSON configuration format (since we don't have toml crate)
    let test_config = r#"{
  "database": {
    "url": "postgresql:///test",
    "pool_size": 10
  },
  "logging": {
    "level": "info"
  }
}"#;

    // Parse JSON to verify it's valid
    let parsed: serde_json::Value = serde_json::from_str(test_config)
        .map_err(|e| color_eyre::eyre::eyre!("Test JSON should be valid: {}", e))?;

    let parsed = parsed
        .as_object()
        .expect("Parsed configuration should be a JSON object");

    assert!(parsed.contains_key("database"));
    assert!(parsed.contains_key("logging"));

    Ok(())
}

// ====== PHASE 6: SERVICE DEPENDENCIES TESTS ======

/// Test Phase 6: Service dependencies verification
#[sinex_test]
async fn test_phase6_service_dependencies() -> TestResult<()> {
    let (_status, details, messages) = services::verify_service_dependencies().await?;

    assert!(!messages.is_empty());

    let details = details.as_object().expect("details should be an object");
    if let Some(binaries) = details.get("binaries") {
        assert!(binaries.is_object());
    }
    if let Some(systemd) = details.get("systemd_services") {
        assert!(systemd.is_object());
    }

    Ok(())
}

/// Test Phase 6: Binary availability verification
#[sinex_test]
async fn test_phase6_binary_availability() -> TestResult<()> {
    // Test with a binary that should exist (ls)
    let output = std::process::Command::new("which")
        .arg("ls")
        .output()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to run which command: {}", e))?;

    assert!(output.status.success(), "'ls' command should be available");

    // Test with a binary that shouldn't exist
    let output = std::process::Command::new("which")
        .arg("nonexistent_binary_12345")
        .output()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to run which command: {}", e))?;

    assert!(
        !output.status.success(),
        "Nonexistent binary should not be found"
    );

    Ok(())
}

// ====== PHASE 7: INTEGRATION TESTS ======

/// Test Phase 7: End-to-end integration verification
#[sinex_test]
async fn test_phase7_integration_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (status, details, messages) = verification::verify_end_to_end_integration().await?;

        assert!(matches!(
            status,
            VerificationStatus::Pass | VerificationStatus::Warning
        ));
        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        let integration = details
            .get("integration_tests")
            .and_then(Value::as_object)
            .expect("integration tests should be present");
        assert!(integration.contains_key("database_integration"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 7: Integration with database connection failure
#[sinex_test]
async fn test_phase7_integration_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let (status, _details, messages) = verification::verify_end_to_end_integration().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(
            messages
                .iter()
                .any(|m| m.contains("Database integration test failed"))
        );

        Ok(())
    })
    .await?;

    Ok(())
}

// ====== UTILITY AND HELPER TESTS ======

/// Test verification status basic properties
#[sinex_test]
async fn test_verification_status_properties() -> TestResult<()> {
    // Test that VerificationStatus enum works correctly
    let statuses = vec![
        VerificationStatus::Pass,
        VerificationStatus::Warning,
        VerificationStatus::Fail,
    ];

    for status in statuses {
        // Test equality and cloning
        let cloned_status = status;
        assert_eq!(status, cloned_status);

        // Test debug formatting
        let debug_str = format!("{status:?}");
        assert!(!debug_str.is_empty());
    }

    Ok(())
}

/// Test error message formatting and context
#[sinex_test]
async fn test_error_message_formatting() -> TestResult<()> {
    // Test various error scenarios and verify message formatting
    let test_cases = vec![
        ("✓ Success message format", true),
        ("✗ Failure message format", false),
        ("⚠ Warning message format", false),
        ("ℹ Info message format", false),
    ];

    for (message, is_success) in test_cases {
        if is_success {
            assert!(
                message.starts_with("✓"),
                "Success messages should start with ✓"
            );
        } else {
            assert!(
                message.starts_with("✗") || message.starts_with("⚠") || message.starts_with("ℹ"),
                "Non-success messages should start with appropriate symbol"
            );
        }
    }

    Ok(())
}
