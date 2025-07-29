// Preflight Failure Scenarios Tests - Comprehensive failure handling and error reporting

use sinex_test_utils::prelude::*;

use sinex_test_utils::prelude::*;
use serde_json::json;
use std::env;
use std::fs;
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::timeout;

// ====== DATABASE FAILURE SCENARIOS ======

/// Test database connection failure scenarios
#[sinex_test]
async fn test_database_connection_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test case 1: Invalid hostname
    env::set_var("DATABASE_URL", "postgresql://invalid-host:5432/test");

    let result = timeout(
        Duration::from_secs(15),
        sinex_preflight::database::verify_database_connectivity(),
    )
    .await;

    match result {
        Ok(Ok((status, _details, messages))) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
            assert!(messages
                .iter()
                .any(|m| m.contains("connection failed") || m.contains("timeout")));
        }
        Ok(Err(e)) => {
            assert!(e.to_string().contains("connect") || e.to_string().contains("resolve"));
        }
        Err(_) => {
            panic!("Database connectivity test should handle timeouts internally");
        }
    }

    // Test case 2: Invalid port
    env::set_var("DATABASE_URL", "postgresql://localhost:99999/test");

    let (status, _details, _messages) = sinex_preflight::database::verify_database_connectivity()
        .await
        .unwrap_or_else(|_| {
            (
                sinex_preflight::VerificationStatus::Fail,
                json!({}),
                vec!["Connection error".to_string()],
            )
        });

    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);

    // Test case 3: Malformed URL
    env::set_var("DATABASE_URL", "not-a-url");

    let result = sinex_preflight::database::verify_database_connectivity().await;

    match result {
        Ok((status, _details, _messages)) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
        }
        Err(e) => {
            assert!(e.to_string().contains("URL") || e.to_string().contains("parse"));
        }
    }

    println!("✓ Database connection failure scenarios tested");

    Ok(())
}

/// Test database extension failures
#[sinex_test]
async fn test_database_extension_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with invalid database URL
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let result = sinex_preflight::database::verify_postgresql_extensions().await;

    // Should fail due to connection error
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("connect")
            || error_msg.contains("database")
            || error_msg.contains("URL")
    );

    println!("✓ Database extension failure scenarios tested");

    Ok(())
}

/// Test migration readiness failures
#[sinex_test]
async fn test_migration_readiness_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with invalid database
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let result = sinex_preflight::database::verify_migration_readiness().await;

    // Should fail due to connection error
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("connect") || error_msg.contains("database"));

    println!("✓ Migration readiness failure scenarios tested");

    Ok(())
}

// ====== RESOURCE CONSTRAINT FAILURES ======

/// Test resource constraint failure scenarios
#[sinex_test]
async fn test_resource_constraint_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // The resources module is designed to warn rather than fail on constraints
    // But we can test edge cases and error conditions

    let (status, details, messages) = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resource verification should not fail: {}", e))?;

    // Should not fail completely (at worst should warn)
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify error handling in resource checks
    assert!(details.get("memory").is_some());
    assert!(details.get("cpu").is_some());

    // Check for warning messages on resource constraints
    let warning_messages: Vec<_> = messages
        .iter()
        .filter(|m| m.contains("⚠") || m.contains("warning"))
        .collect();

    println!(
        "✓ Resource constraint scenarios tested - {} warnings",
        warning_messages.len()
    );

    Ok(())
}

/// Test filesystem permission failures
#[sinex_test]
async fn test_filesystem_permission_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Create a read-only directory to test permission failures
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;
    let readonly_dir = temp_dir.path().join("readonly");

    // Create directory and make it read-only
    fs::create_dir(&readonly_dir).map_err(|e| format!("Failed to create readonly dir: {}", e))?;

    // Test write permission failure
    let test_file = readonly_dir.join("test_file");
    let write_result = fs::write(&test_file, "test");

    // Should succeed initially
    assert!(write_result.is_ok());

    // Now make directory read-only (on systems that support it)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&readonly_dir)
            .map_err(|e| format!("Failed to get directory metadata: {}", e))?
            .permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(&readonly_dir, perms)
            .map_err(|e| format!("Failed to set read-only permissions: {}", e))?;

        // Now test should fail
        let write_result = fs::write(&readonly_dir.join("new_file"), "test");
        assert!(
            write_result.is_err(),
            "Write should fail on read-only directory"
        );
    }

    println!("✓ Filesystem permission failure scenarios tested");

    Ok(())
}

// ====== CONFIGURATION FAILURES ======

/// Test configuration generation failures
#[sinex_test]
async fn test_configuration_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test case 1: Missing required environment variables
    env::remove_var("DATABASE_URL");

    let (status, details, messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Config verification should handle missing vars: {}", e))?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("DATABASE_URL") && m.contains("missing")));

    // Test case 2: Invalid TOML syntax
    let invalid_toml = "[database\nurl = incomplete";
    let parse_result: Result<toml::Value, _> = invalid_toml.parse();
    assert!(parse_result.is_err(), "Invalid TOML should fail to parse");

    // Test case 3: Restore valid environment for remaining tests
    env::set_var("DATABASE_URL", "postgresql:///test");

    let (status, _details, _messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Config verification failed after restore: {}", e))?;

    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Configuration failure scenarios tested");

    Ok(())
}

/// Test environment variable validation failures
#[sinex_test]
async fn test_environment_validation_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Save original values
    let original_db_url = env::var("DATABASE_URL").ok();

    // Test missing required variables
    env::remove_var("DATABASE_URL");

    let (status, details, messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Should handle missing env vars: {}", e))?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);

    let environment = details
        .get("environment")
        .expect("Should have environment details");
    let env_obj = environment.as_object().unwrap();
    let db_url_info = env_obj.get("DATABASE_URL").unwrap();
    assert_eq!(db_url_info.get("present").unwrap(), false);

    // Restore original value
    if let Some(url) = original_db_url {
        env::set_var("DATABASE_URL", url);
    }

    println!("✓ Environment validation failure scenarios tested");

    Ok(())
}

// ====== SERVICE DEPENDENCY FAILURES ======

/// Test service dependency failures
#[sinex_test]
async fn test_service_dependency_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Service verification is designed to warn rather than fail for missing services
    let (status, details, messages) = sinex_preflight::services::verify_service_dependencies()
        .await
        .map_err(|e| format!("Service verification failed: {}", e))?;

    // Should pass or warn (not fail completely)
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Check for binary availability tests
    let binaries = details
        .get("binaries")
        .expect("Should have binaries section");
    assert!(binaries.is_object());

    // Count missing required binaries
    let missing_required = binaries
        .as_object()
        .unwrap()
        .values()
        .filter(|v| v.get("required").and_then(|r| r.as_bool()).unwrap_or(false))
        .filter(|v| {
            !v.get("available")
                .and_then(|a| a.as_bool())
                .unwrap_or(false)
        })
        .count();

    if missing_required > 0 {
        // If required binaries are missing, should be a failure
        assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
        assert!(messages
            .iter()
            .any(|m| m.contains("Missing required binaries")));
    }

    println!("✓ Service dependency failure scenarios tested");

    Ok(())
}

/// Test binary availability failures
#[sinex_test]
async fn test_binary_availability_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with known non-existent binary
    let output = Command::new("which")
        .arg("definitely_nonexistent_binary_12345")
        .output()
        .map_err(|e| format!("Failed to run which command: {}", e))?;

    assert!(
        !output.status.success(),
        "Nonexistent binary should not be found"
    );

    // Test error handling when 'which' command itself fails
    let output = Command::new("nonexistent_which_command").arg("ls").output();

    assert!(output.is_err(), "Nonexistent 'which' command should fail");

    println!("✓ Binary availability failure scenarios tested");

    Ok(())
}

// ====== INTEGRATION FAILURES ======

/// Test end-to-end integration failures
#[sinex_test]
async fn test_integration_failures(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with invalid database URL
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let (status, details, messages) =
        sinex_preflight::verification::verify_end_to_end_integration()
            .await
            .map_err(|e| format!("Integration test should handle DB failure: {}", e))?;

    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("Database integration test failed")));

    // Check that failure details are properly captured
    let db_integration = details.get("database_integration");
    if let Some(db_details) = db_integration {
        // Should capture error information
        assert!(db_details.is_object() || db_details.is_null());
    }

    println!("✓ Integration failure scenarios tested");

    Ok(())
}

// ====== ERROR REPORTING AND AGGREGATION ======

/// Test error message formatting and consistency
#[sinex_test]
async fn test_error_message_formatting(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with various failure scenarios to check message formatting
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let (status, _details, messages) = sinex_preflight::database::verify_database_connectivity()
        .await
        .unwrap_or_else(|_| {
            (
                sinex_preflight::VerificationStatus::Fail,
                json!({}),
                vec!["Connection failed".to_string()],
            )
        });

    // Verify error messages follow consistent format
    for message in &messages {
        if message.contains("✗") {
            // Error messages should have proper format
            assert!(message.len() > 5, "Error messages should be descriptive");
        }
    }

    // Test that errors contain actionable information
    if status == sinex_preflight::VerificationStatus::Fail {
        let error_messages: Vec<_> = messages
            .iter()
            .filter(|m| m.contains("✗") || m.contains("fail"))
            .collect();

        assert!(
            !error_messages.is_empty(),
            "Should have at least one error message"
        );

        // Error messages should contain helpful information
        for error_msg in error_messages {
            assert!(
                error_msg.len() > 10,
                "Error messages should be descriptive: {}",
                error_msg
            );
        }
    }

    println!("✓ Error message formatting tested");

    Ok(())
}

/// Test error aggregation across multiple phases
#[sinex_test]
async fn test_error_aggregation(_ctx: TestContext) -> anyhow::Result<()> {
    // Create conditions that will generate multiple errors/warnings
    env::remove_var("DATABASE_URL");

    let mut all_errors = Vec::new();
    let mut all_warnings = Vec::new();

    // Run configuration phase (will fail due to missing DATABASE_URL)
    if let Ok((status, _details, messages)) =
        sinex_preflight::configuration::verify_configuration_generation().await
    {
        for message in messages {
            if message.contains("✗") || message.contains("fail") {
                all_errors.push(message);
            } else if message.contains("⚠") || message.contains("warning") {
                all_warnings.push(message);
            }
        }

        assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
    }

    // Restore environment and run more phases
    env::set_var("DATABASE_URL", "postgresql:///test");

    // Run resources phase (might generate warnings)
    if let Ok((_status, _details, messages)) =
        sinex_preflight::resources::verify_system_resources().await
    {
        for message in messages {
            if message.contains("⚠") || message.contains("warning") {
                all_warnings.push(message);
            }
        }
    }

    // Verify error/warning aggregation
    assert!(!all_errors.is_empty(), "Should have collected some errors");

    println!(
        "✓ Error aggregation tested - {} errors, {} warnings",
        all_errors.len(),
        all_warnings.len()
    );

    Ok(())
}

// ====== TIMEOUT AND RESOURCE EXHAUSTION ======

/// Test behavior under timeout conditions
#[sinex_test]
async fn test_timeout_conditions(_ctx: TestContext) -> anyhow::Result<()> {
    // Test with a non-responsive database URL that should timeout
    env::set_var("DATABASE_URL", "postgresql://192.0.2.1:5432/test"); // RFC 5737 test IP

    let start_time = Instant::now();

    // Database connectivity should have built-in timeout
    let result = timeout(
        Duration::from_secs(15),
        sinex_preflight::database::verify_database_connectivity(),
    )
    .await;

    let elapsed = start_time.elapsed();

    match result {
        Ok(Ok((status, _details, messages))) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
            assert!(messages
                .iter()
                .any(|m| m.contains("timeout") || m.contains("connection failed")));

            // Should timeout reasonably quickly (within 10 seconds)
            assert!(
                elapsed.as_secs() < 10,
                "Should timeout quickly, took: {:?}",
                elapsed
            );
        }
        Ok(Err(_)) => {
            // Connection error is also acceptable
        }
        Err(_) => {
            panic!("Database connectivity test should have internal timeout, not external timeout");
        }
    }

    println!("✓ Timeout condition testing completed in {:?}", elapsed);

    Ok(())
}

// ====== GRACEFUL DEGRADATION ======

/// Test graceful degradation when optional components fail
#[sinex_test]
async fn test_graceful_degradation(_ctx: TestContext) -> anyhow::Result<()> {
    // Test that optional service failures don't prevent overall success
    let (status, details, messages) = sinex_preflight::services::verify_service_dependencies()
        .await
        .map_err(|e| format!("Service verification failed: {}", e))?;

    // Should handle missing optional services gracefully
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Check external dependencies section
    let external_deps = details.get("external_dependencies");
    if let Some(deps) = external_deps {
        assert!(deps.is_object());

        // May have git dependencies missing, but should still continue
        let git_info = deps.get("git");
        if let Some(git) = git_info {
            // Git availability is optional for basic functionality
            assert!(git.is_object());
        }
    }

    // Test that configuration warnings don't prevent progress
    env::set_var("DATABASE_URL", "postgresql:///test");

    let (config_status, _config_details, _config_messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Configuration test failed: {}", e))?;

    // Should warn about missing optional configs but not fail
    assert!(matches!(
        config_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Graceful degradation tested");

    Ok(())
}

// ====== RECOVERY MECHANISMS ======

/// Test recovery after transient failures
#[sinex_test]
async fn test_recovery_mechanisms(ctx: TestContext) -> anyhow::Result<()> {
    // First, test with a failure condition
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let result1 = sinex_preflight::database::verify_database_connectivity().await;

    // Should fail
    match result1 {
        Ok((status, _details, _messages)) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
        }
        Err(_) => {
            // Error is also acceptable
        }
    }

    // Now test recovery with valid configuration
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status2, _details2, _messages2) =
        sinex_preflight::database::verify_database_connectivity()
            .await
            .map_err(|e| format!("Recovery test failed: {}", e))?;

    // Should recover and pass
    assert_eq!(status2, sinex_preflight::VerificationStatus::Pass);

    println!("✓ Recovery mechanisms tested");

    Ok(())
}
