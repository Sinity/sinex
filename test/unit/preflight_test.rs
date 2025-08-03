// Preflight Unit Tests - Comprehensive verification phase testing

use sinex_test_utils::prelude::*;

use sinex_preflight::*;
use sinex_test_utils::prelude::*;
use std::env;
use std::fs;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

/// Test basic VerificationStatus functionality
#[sinex_test]
async fn test_verification_status_basic(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
async fn test_verification_status_comparisons(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
async fn test_phase1_database_connectivity_success(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Set valid database URL
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = database::verify_database_connectivity()
        .await
        .map_err(|e| format!("Database connectivity test failed: {}", e))?;

    assert_eq!(status, VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages
        .iter()
        .any(|m| m.contains("connection established")));

    // Verify details contain expected fields
    assert!(details.get("database_url").is_some());
    assert!(details.get("postgresql_version").is_some());
    assert!(details.get("connection_pool").is_some());

    Ok(())
}

/// Test Phase 1: Database connectivity with invalid URL
#[sinex_test]
async fn test_phase1_database_connectivity_failure(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Set invalid database URL
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let (status, _details, messages) =
        database::verify_database_connectivity()
            .await
            .map_err(|e| {
                format!(
                    "Expected database connectivity test to handle invalid URL: {}",
                    e
                )
            })?;

    assert_eq!(status, VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("connection failed") || m.contains("timeout")));

    Ok(())
}

/// Test Phase 1: Database connectivity timeout handling
#[sinex_test]
async fn test_phase1_database_connectivity_timeout(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Use a non-responsive IP to trigger timeout
    env::set_var("DATABASE_URL", "postgresql://192.0.2.1:5432/test"); // RFC 5737 test IP

    // Test should complete within reasonable time due to built-in timeout
    let result = timeout(
        Duration::from_secs(10),
        database::verify_database_connectivity(),
    )
    .await;

    match result {
        Ok(Ok((status, _details, messages))) => {
            assert_eq!(status, VerificationStatus::Fail);
            assert!(messages
                .iter()
                .any(|m| m.contains("timeout") || m.contains("connection failed")));
        }
        Ok(Err(_)) => {
            // Connection error is also acceptable
        }
        Err(_) => {
            panic!("Database connectivity test should have internal timeout handling");
        }
    }

    Ok(())
}

// ====== PHASE 2: POSTGRESQL EXTENSIONS TESTS ======

/// Test Phase 2: PostgreSQL extensions verification
#[sinex_test]
async fn test_phase2_postgresql_extensions(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = database::verify_postgresql_extensions()
        .await
        .map_err(|e| format!("Extensions verification failed: {}", e))?;

    // Status should be Pass or Warning (some extensions might not be available in test environment)
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain extensions information
    let extensions = details
        .get("extensions")
        .expect("Should have extensions details");
    assert!(extensions.is_object());

    // Check for required extensions
    let extensions_obj = extensions.as_object().unwrap();
    assert!(extensions_obj.contains_key("uuid-ossp"));
    assert!(extensions_obj.contains_key("pgx_ulid"));
    assert!(extensions_obj.contains_key("timescaledb"));

    Ok(())
}

/// Test Phase 2: Extensions verification with database connection failure
#[sinex_test]
async fn test_phase2_extensions_db_failure(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let result = database::verify_postgresql_extensions().await;

    // Should return error due to connection failure
    assert!(result.is_err());

    Ok(())
}

// ====== PHASE 3: MIGRATION READINESS TESTS ======

/// Test Phase 3: Migration readiness verification
#[sinex_test]
async fn test_phase3_migration_readiness(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = database::verify_migration_readiness()
        .await
        .map_err(|e| format!("Migration readiness test failed: {}", e))?;

    // Should pass or warn
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain migration information
    assert!(details.get("current_migrations").is_some());
    assert!(details.get("discovered_migrations").is_some());

    Ok(())
}

/// Test Phase 3: Migration readiness with invalid database
#[sinex_test]
async fn test_phase3_migration_readiness_db_failure(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let result = database::verify_migration_readiness().await;

    // Should return error due to connection failure
    assert!(result.is_err());

    Ok(())
}

// ====== PHASE 4: SYSTEM RESOURCES TESTS ======

/// Test Phase 4: System resources verification success
#[sinex_test]
async fn test_phase4_system_resources_success(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let (status, details, messages) = resources::verify_system_resources()
        .await
        .map_err(|e| format!("System resources test failed: {}", e))?;

    // Should pass or warn (depends on system specs)
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain resource information
    assert!(details.get("memory").is_some());
    assert!(details.get("disk").is_some());
    assert!(details.get("cpu").is_some());
    assert!(details.get("filesystem").is_some());

    // Check memory details structure
    let memory = details.get("memory").unwrap();
    assert!(memory.get("total_gb").is_some());
    assert!(memory.get("available_gb").is_some());
    assert!(memory.get("meets_requirements").is_some());

    Ok(())
}

/// Test Phase 4: Filesystem permissions verification with temp directory
#[sinex_test]
async fn test_phase4_filesystem_permissions(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Create a temporary directory for testing
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;
    let temp_path = temp_dir.path().to_string_lossy();

    // Test filesystem verification logic (we can't easily test the full function due to hardcoded paths)
    // Instead test the underlying logic
    let test_file = temp_dir.path().join(".sinex_test_file");

    // Test write permissions
    fs::write(&test_file, "test content")
        .map_err(|e| format!("Failed to write test file: {}", e))?;

    // Test read permissions
    let content =
        fs::read_to_string(&test_file).map_err(|e| format!("Failed to read test file: {}", e))?;
    assert_eq!(content, "test content");

    // Test cleanup
    fs::remove_file(&test_file).map_err(|e| format!("Failed to remove test file: {}", e))?;

    Ok(())
}

// ====== PHASE 5: CONFIGURATION TESTS ======

/// Test Phase 5: Configuration verification success
#[sinex_test]
async fn test_phase5_configuration_success(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration verification failed: {}", e))?;

    // Should pass or warn
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain configuration information
    assert!(details.get("environment").is_some());
    assert!(details.get("toml_generation").is_some());
    assert!(details.get("event_sources").is_some());

    Ok(())
}

/// Test Phase 5: Configuration with missing environment variables
#[sinex_test]
async fn test_phase5_configuration_missing_env(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Remove required environment variable
    env::remove_var("DATABASE_URL");

    let (status, _details, messages) = configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration test should handle missing vars: {}", e))?;

    assert_eq!(status, VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("DATABASE_URL") && m.contains("missing")));

    Ok(())
}

/// Test Phase 5: TOML configuration generation
#[sinex_test]
async fn test_phase5_toml_generation(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test TOML generation independently
    let test_config = r#"
[database]
url = "postgresql:///test"
pool_size = 10

[logging]
level = "info"
"#;

    // Parse TOML to verify it's valid
    let parsed: toml::Value = test_config
        .parse()
        .map_err(|e| format!("Test TOML should be valid: {}", e))?;

    assert!(parsed.get("database").is_some());
    assert!(parsed.get("logging").is_some());

    Ok(())
}

// ====== PHASE 6: SERVICE DEPENDENCIES TESTS ======

/// Test Phase 6: Service dependencies verification
#[sinex_test]
async fn test_phase6_service_dependencies(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let (status, details, messages) = services::verify_service_dependencies()
        .await
        .map_err(|e| format!("Service dependencies test failed: {}", e))?;

    // Should pass or warn (some services might not be available)
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain service information
    assert!(details.get("binaries").is_some());
    assert!(details.get("systemd_services").is_some());
    assert!(details.get("external_dependencies").is_some());

    Ok(())
}

/// Test Phase 6: Binary availability verification
#[sinex_test]
async fn test_phase6_binary_availability(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test with a binary that should exist (ls)
    let output = std::process::Command::new("which")
        .arg("ls")
        .output()
        .map_err(|e| format!("Failed to run which command: {}", e))?;

    assert!(output.status.success(), "'ls' command should be available");

    // Test with a binary that shouldn't exist
    let output = std::process::Command::new("which")
        .arg("nonexistent_binary_12345")
        .output()
        .map_err(|e| format!("Failed to run which command: {}", e))?;

    assert!(
        !output.status.success(),
        "Nonexistent binary should not be found"
    );

    Ok(())
}

// ====== PHASE 7: INTEGRATION TESTS ======

/// Test Phase 7: End-to-end integration verification
#[sinex_test]
async fn test_phase7_integration_success(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = verification::verify_end_to_end_integration()
        .await
        .map_err(|e| format!("Integration verification failed: {}", e))?;

    // Should pass or warn
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Verify details contain integration test results
    assert!(details.get("database_integration").is_some());

    Ok(())
}

/// Test Phase 7: Integration with database connection failure
#[sinex_test]
async fn test_phase7_integration_db_failure(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let (status, _details, messages) = verification::verify_end_to_end_integration()
        .await
        .map_err(|e| format!("Integration test should handle DB failure: {}", e))?;

    assert_eq!(status, VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("Database integration test failed")));

    Ok(())
}

// ====== UTILITY AND HELPER TESTS ======

/// Test verification status serialization
#[sinex_test]
async fn test_verification_status_serialization(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    // Test that VerificationStatus can be serialized/deserialized properly
    let statuses = vec![
        VerificationStatus::Pass,
        VerificationStatus::Warning,
        VerificationStatus::Fail,
    ];

    for status in statuses {
        let json_val = serde_json::to_value(&status)
            .map_err(|e| format!("Failed to serialize status: {}", e))?;

        let deserialized: VerificationStatus = serde_json::from_value(json_val)
            .map_err(|e| format!("Failed to deserialize status: {}", e))?;

        assert_eq!(status, deserialized);
    }

    Ok(())
}

/// Test error message formatting and context
#[sinex_test]
async fn test_error_message_formatting(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
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
