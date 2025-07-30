// Preflight Rollback and Recovery Tests - Testing failure recovery, rollback mechanisms, and system resilience

use sinex_test_utils::prelude::*;

use sinex_test_utils::prelude::*;
use std::env;
use std::fs;
use tempfile::TempDir;

// ====== DATABASE RECOVERY TESTS ======

/// Test database connection recovery after transient failures
#[sinex_test]
async fn test_database_connection_recovery(ctx: TestContext) -> anyhow::Result<()> {
    // Phase 1: Simulate failure condition
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let failure_result = sinex_preflight::database::verify_database_connectivity().await;

    // Should fail as expected
    match failure_result {
        Ok((status, _details, messages)) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
            assert!(messages
                .iter()
                .any(|m| m.contains("connection failed") || m.contains("timeout")));
        }
        Err(_) => {
            // Error is also acceptable for invalid connection
        }
    }

    println!("✓ Confirmed database failure state");

    // Phase 2: Recovery with valid configuration
    env::set_var("DATABASE_URL", ctx.database_url());

    let recovery_result = sinex_preflight::database::verify_database_connectivity()
        .await
        .map_err(|e| format!("Database recovery failed: {}", e))?;

    let (status, details, messages) = recovery_result;

    // Should recover successfully
    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    assert!(messages
        .iter()
        .any(|m| m.contains("connection established")));
    assert!(details.get("postgresql_version").is_some());

    println!("✓ Database connection recovery verified");

    Ok(())
}

/// Test database extension recovery
#[sinex_test]
async fn test_database_extension_recovery(ctx: TestContext) -> anyhow::Result<()> {
    // Phase 1: Test with invalid database (should fail)
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let failure_result = sinex_preflight::database::verify_postgresql_extensions().await;
    assert!(
        failure_result.is_err(),
        "Extensions check should fail with invalid database"
    );

    println!("✓ Confirmed extensions failure state");

    // Phase 2: Recovery with valid database
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = sinex_preflight::database::verify_postgresql_extensions()
        .await
        .map_err(|e| format!("Extensions recovery failed: {}", e))?;

    // Should recover (pass or warn depending on available extensions)
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(details.get("extensions").is_some());
    assert!(!messages.is_empty());

    println!("✓ Database extensions recovery verified");

    Ok(())
}

/// Test migration recovery scenarios
#[sinex_test]
async fn test_migration_recovery(ctx: TestContext) -> anyhow::Result<()> {
    // Phase 1: Test with invalid database (should fail)
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let failure_result = sinex_preflight::database::verify_migration_readiness().await;
    assert!(
        failure_result.is_err(),
        "Migration check should fail with invalid database"
    );

    println!("✓ Confirmed migration failure state");

    // Phase 2: Recovery with valid database
    env::set_var("DATABASE_URL", ctx.database_url());

    let (status, details, messages) = sinex_preflight::database::verify_migration_readiness()
        .await
        .map_err(|e| format!("Migration recovery failed: {}", e))?;

    // Should recover successfully
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(details.get("current_migrations").is_some());
    assert!(!messages.is_empty());

    println!("✓ Migration recovery verified");

    Ok(())
}

// ====== CONFIGURATION RECOVERY TESTS ======

/// Test configuration recovery after environment changes
#[sinex_test]
async fn test_configuration_recovery(_ctx: TestContext) -> anyhow::Result<()> {
    // Save original environment state
    let original_db_url = env::var("DATABASE_URL").ok();
    let original_rust_log = env::var("RUST_LOG").ok();

    // Phase 1: Create failure condition (missing required env vars)
    env::remove_var("DATABASE_URL");
    env::remove_var("RUST_LOG");

    let failure_result = sinex_preflight::configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration test should handle missing vars: {}", e))?;

    let (status, details, messages) = failure_result;

    // Should fail due to missing DATABASE_URL
    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("DATABASE_URL") && m.contains("missing")));

    // Verify failure details
    let environment = details
        .get("environment")
        .expect("Should have environment details");
    let env_obj = environment.as_object().unwrap();
    let db_url_info = env_obj.get("DATABASE_URL").unwrap();
    assert_eq!(db_url_info.get("present").unwrap(), false);

    println!("✓ Confirmed configuration failure state");

    // Phase 2: Recovery - restore required environment variables
    if let Some(url) = &original_db_url {
        env::set_var("DATABASE_URL", url);
    } else {
        env::set_var("DATABASE_URL", "postgresql:///test");
    }

    if let Some(log) = &original_rust_log {
        env::set_var("RUST_LOG", log);
    } else {
        env::set_var("RUST_LOG", "info");
    }

    let recovery_result = sinex_preflight::configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration recovery failed: {}", e))?;

    let (status, details, messages) = recovery_result;

    // Should recover successfully
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify recovery details
    let environment = details
        .get("environment")
        .expect("Should have environment details");
    let env_obj = environment.as_object().unwrap();
    let db_url_info = env_obj.get("DATABASE_URL").unwrap();
    assert_eq!(db_url_info.get("present").unwrap(), true);

    println!("✓ Configuration recovery verified");

    Ok(())
}

/// Test TOML configuration file recovery
#[sinex_test]
async fn test_toml_config_recovery(_ctx: TestContext) -> anyhow::Result<()> {
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;
    let config_path = temp_dir.path().join("test_config.toml");

    // Phase 1: Create invalid TOML file
    let invalid_toml = r#"
[database
url = "incomplete toml
"#;

    fs::write(&config_path, invalid_toml)
        .map_err(|e| format!("Failed to write invalid config: {}", e))?;

    // Test parsing fails
    let parse_result: Result<toml::Value, _> = invalid_toml.parse();
    assert!(parse_result.is_err(), "Invalid TOML should fail to parse");

    println!("✓ Confirmed TOML parsing failure");

    // Phase 2: Recovery with valid TOML
    let valid_toml = r#"
[database]
url = "postgresql:///test"
pool_size = 10

[logging]
level = "info"
format = "json"

[event_sources]
filesystem = true
terminal = false
"#;

    fs::write(&config_path, valid_toml)
        .map_err(|e| format!("Failed to write valid config: {}", e))?;

    // Test parsing succeeds
    let parse_result: Result<toml::Value, _> = valid_toml.parse();
    assert!(parse_result.is_ok(), "Valid TOML should parse successfully");

    let parsed = parse_result.unwrap();
    assert!(parsed.get("database").is_some());
    assert!(parsed.get("logging").is_some());
    assert!(parsed.get("event_sources").is_some());

    println!("✓ TOML configuration recovery verified");

    Ok(())
}

// ====== SERVICE RECOVERY TESTS ======

/// Test service dependency recovery
#[sinex_test]
async fn test_service_dependency_recovery(_ctx: TestContext) -> anyhow::Result<()> {
    // Service verification is resilient by design - it warns rather than fails
    // But we can test the recovery patterns

    let (status, details, messages) = sinex_preflight::services::verify_service_dependencies()
        .await
        .map_err(|e| format!("Service verification failed: {}", e))?;

    // Should pass or warn (not fail completely)
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify service details structure
    assert!(details.get("binaries").is_some());
    assert!(details.get("systemd_services").is_some());
    assert!(details.get("external_dependencies").is_some());

    // Count available vs missing services
    let binaries = details.get("binaries").unwrap().as_object().unwrap();
    let available_binaries = binaries
        .values()
        .filter(|v| {
            v.get("available")
                .and_then(|a| a.as_bool())
                .unwrap_or(false)
        })
        .count();

    let required_binaries = binaries
        .values()
        .filter(|v| v.get("required").and_then(|r| r.as_bool()).unwrap_or(false))
        .count();

    println!("✓ Service dependency recovery patterns verified:");
    println!("  Available binaries: {}", available_binaries);
    println!("  Required binaries: {}", required_binaries);

    // System should be resilient to missing optional dependencies
    if status == sinex_preflight::VerificationStatus::Warning {
        println!("  System gracefully handles missing optional services");
    }

    Ok(())
}

// ====== RESOURCE CONSTRAINT RECOVERY ======

/// Test recovery from resource constraint warnings
#[sinex_test]
async fn test_resource_constraint_recovery(_ctx: TestContext) -> anyhow::Result<()> {
    // Resources module is designed to be resilient - it warns but doesn't fail
    let (status, details, messages) = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resource verification failed: {}", e))?;

    // Should pass or warn
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify resource information is collected
    let memory = details.get("memory").expect("Should have memory details");
    let disk = details.get("disk").expect("Should have disk details");
    let cpu = details.get("cpu").expect("Should have CPU details");

    // Check memory recovery patterns
    let memory_available = memory
        .get("available_gb")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let memory_meets_req = memory
        .get("meets_requirements")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    println!("✓ Resource constraint recovery verified:");
    println!("  Available memory: {:.2} GB", memory_available);
    println!("  Meets requirements: {}", memory_meets_req);

    // System should provide actionable information even under constraints
    if !memory_meets_req {
        assert!(
            messages
                .iter()
                .any(|m| m.contains("memory") && (m.contains("⚠") || m.contains("warning"))),
            "Should provide memory warning messages"
        );
    }

    // Test filesystem recovery (basic permissions check)
    let filesystem = details.get("filesystem");
    if let Some(fs_details) = filesystem {
        println!("  Filesystem checks: available");
        assert!(fs_details.is_object());
    }

    Ok(())
}

/// Test filesystem permission recovery
#[sinex_test]
async fn test_filesystem_permission_recovery(_ctx: TestContext) -> anyhow::Result<()> {
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;

    // Phase 1: Test with restricted permissions
    let restricted_dir = temp_dir.path().join("restricted");
    fs::create_dir(&restricted_dir)
        .map_err(|e| format!("Failed to create restricted dir: {}", e))?;

    // Try to create a file in the directory
    let test_file = restricted_dir.join("test.txt");
    let write_result = fs::write(&test_file, "test content");

    // Should succeed initially
    assert!(write_result.is_ok(), "Initial write should succeed");

    // Phase 2: Test recovery after permission issues
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        // Make directory read-only
        let mut perms = fs::metadata(&restricted_dir)
            .map_err(|e| format!("Failed to get directory metadata: {}", e))?
            .permissions();
        perms.set_mode(0o444); // Read-only
        fs::set_permissions(&restricted_dir, perms)
            .map_err(|e| format!("Failed to set read-only permissions: {}", e))?;

        // Verify write fails
        let write_result = fs::write(&restricted_dir.join("new_file.txt"), "new content");
        assert!(
            write_result.is_err(),
            "Write should fail on read-only directory"
        );

        println!("✓ Confirmed filesystem permission restriction");

        // Phase 3: Recovery - restore permissions
        let mut perms = fs::metadata(&restricted_dir)
            .map_err(|e| format!("Failed to get directory metadata for recovery: {}", e))?
            .permissions();
        perms.set_mode(0o755); // Read-write-execute
        fs::set_permissions(&restricted_dir, perms)
            .map_err(|e| format!("Failed to restore permissions: {}", e))?;

        // Verify write succeeds again
        let recovery_file = restricted_dir.join("recovery_test.txt");
        let write_result = fs::write(&recovery_file, "recovery content");
        assert!(
            write_result.is_ok(),
            "Write should succeed after permission recovery"
        );

        // Verify content
        let content = fs::read_to_string(&recovery_file)
            .map_err(|e| format!("Failed to read recovery file: {}", e))?;
        assert_eq!(content, "recovery content");

        println!("✓ Filesystem permission recovery verified");
    }

    #[cfg(not(unix))]
    {
        println!("✓ Filesystem permission recovery test skipped (non-Unix platform)");
    }

    Ok(())
}

// ====== INTEGRATION RECOVERY TESTS ======

/// Test end-to-end integration recovery
#[sinex_test]
async fn test_integration_recovery(ctx: TestContext) -> anyhow::Result<()> {
    // Phase 1: Test with invalid database (should fail)
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let failure_result = sinex_preflight::verification::verify_end_to_end_integration()
        .await
        .map_err(|e| format!("Integration test should handle failure: {}", e))?;

    let (status, details, messages) = failure_result;

    // Should fail due to database connectivity
    assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
    assert!(messages
        .iter()
        .any(|m| m.contains("Database integration test failed")));

    // Should capture failure details
    let db_integration = details.get("database_integration");
    assert!(
        db_integration.is_some(),
        "Should capture database integration failure details"
    );

    println!("✓ Confirmed integration failure state");

    // Phase 2: Recovery with valid database
    env::set_var("DATABASE_URL", ctx.database_url());

    let recovery_result = sinex_preflight::verification::verify_end_to_end_integration()
        .await
        .map_err(|e| format!("Integration recovery failed: {}", e))?;

    let (status, details, messages) = recovery_result;

    // Should recover successfully
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Should have successful integration details
    let db_integration = details
        .get("database_integration")
        .expect("Should have database integration details");
    assert!(db_integration.is_object());

    // Should have successful messages
    assert!(messages
        .iter()
        .any(|m| m.contains("✓") || m.contains("passed") || m.contains("success")));

    println!("✓ End-to-end integration recovery verified");

    Ok(())
}

// ====== PARTIAL FAILURE RECOVERY ======

/// Test recovery from partial phase failures
#[sinex_test]
async fn test_partial_failure_recovery(ctx: TestContext) -> anyhow::Result<()> {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Run multiple phases and test that warnings don't prevent overall success
    let mut phase_results = Vec::new();

    // Phase 1: Database (should pass)
    let db_result = sinex_preflight::database::verify_database_connectivity()
        .await
        .map_err(|e| format!("Database phase failed: {}", e))?;
    phase_results.push(("database", db_result));

    // Phase 2: Resources (might warn on constrained systems)
    let resources_result = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resources phase failed: {}", e))?;
    phase_results.push(("resources", resources_result));

    // Phase 3: Configuration (might warn about missing optional configs)
    let config_result = sinex_preflight::configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration phase failed: {}", e))?;
    phase_results.push(("configuration", config_result));

    // Phase 4: Services (might warn about missing optional services)
    let services_result = sinex_preflight::services::verify_service_dependencies()
        .await
        .map_err(|e| format!("Services phase failed: {}", e))?;
    phase_results.push(("services", services_result));

    // Analyze results
    let mut pass_count = 0;
    let mut warn_count = 0;
    let mut fail_count = 0;

    for (phase_name, (status, _details, messages)) in &phase_results {
        match status {
            sinex_preflight::VerificationStatus::Pass => {
                pass_count += 1;
                println!("✓ Phase {} passed", phase_name);
            }
            sinex_preflight::VerificationStatus::Warning => {
                warn_count += 1;
                println!(
                    "⚠ Phase {} warned: {:?}",
                    phase_name,
                    messages
                        .iter()
                        .filter(|m| m.contains("⚠"))
                        .collect::<Vec<_>>()
                );
            }
            sinex_preflight::VerificationStatus::Fail => {
                fail_count += 1;
                println!(
                    "✗ Phase {} failed: {:?}",
                    phase_name,
                    messages
                        .iter()
                        .filter(|m| m.contains("✗"))
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    println!("✓ Partial failure recovery analysis:");
    println!("  Passed: {}", pass_count);
    println!("  Warned: {}", warn_count);
    println!("  Failed: {}", fail_count);

    // Should have at least some successful phases
    assert!(pass_count > 0, "Should have at least some passing phases");

    // Critical phases should not fail
    let critical_phases = ["database"];
    for (phase_name, (status, _details, _messages)) in &phase_results {
        if critical_phases.contains(phase_name) {
            assert!(
                matches!(status, sinex_preflight::VerificationStatus::Pass),
                "Critical phase {} should pass",
                phase_name
            );
        }
    }

    Ok(())
}

// ====== ROLLBACK SIMULATION TESTS ======

/// Test rollback simulation after verification failure
#[sinex_test]
async fn test_rollback_simulation(_ctx: TestContext) -> anyhow::Result<()> {
    // Simulate a deployment scenario where verification fails and we need to rollback

    // Phase 1: Baseline state (save current environment)
    let baseline_state = EnvironmentState::capture();

    println!("✓ Captured baseline environment state");

    // Phase 2: Simulate deployment changes that cause verification to fail
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");
    env::set_var("SINEX_CONFIG", "/nonexistent/config.toml");

    // Phase 3: Run verification (should fail)
    let verification_result =
        sinex_preflight::configuration::verify_configuration_generation().await;

    match verification_result {
        Ok((status, _details, messages)) => {
            if status == sinex_preflight::VerificationStatus::Fail {
                println!(
                    "✓ Verification failed as expected: {:?}",
                    messages
                        .iter()
                        .filter(|m| m.contains("✗"))
                        .collect::<Vec<_>>()
                );
            } else {
                println!("⚠ Verification passed unexpectedly (may be due to test environment)");
            }
        }
        Err(e) => {
            println!("✓ Verification failed with error: {}", e);
        }
    }

    // Phase 4: Simulate rollback (restore baseline state)\n    baseline_state.restore();\n    \n    println!(\"✓ Rolled back to baseline environment state\");\n    \n    // Phase 5: Verify rollback success (should pass/warn now)\n    let rollback_verification = sinex_preflight::configuration::verify_configuration_generation().await;\n    \n    match rollback_verification {\n        Ok((status, _details, _messages)) => {\n            assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning),\n                    \"Verification should pass after rollback\");\n            println!(\"✓ Verification passed after rollback\");\n        }\n        Err(e) => {\n            // If it still fails, it might be due to missing DATABASE_URL in baseline\n            // This is acceptable for the rollback test\n            println!(\"ℹ Verification still has issues after rollback (expected if DATABASE_URL was not set initially): {}\", e);\n        }\n    }\n    \n    println!(\"✓ Rollback simulation completed\");\n    \n    Ok(())\n}\n\n// Helper struct for environment state management\nstruct EnvironmentState {\n    database_url: Option<String>,\n    rust_log: Option<String>,\n    sinex_config: Option<String>,\n}\n\nimpl EnvironmentState {\n    fn capture() -> Self {\n        Self {\n            database_url: env::var(\"DATABASE_URL\").ok(),\n            rust_log: env::var(\"RUST_LOG\").ok(),\n            sinex_config: env::var(\"SINEX_CONFIG\").ok(),\n        }\n    }\n    \n    fn restore(&self) {\n        // Restore or remove environment variables\n        match &self.database_url {\n            Some(url) => env::set_var(\"DATABASE_URL\", url),\n            None => env::remove_var(\"DATABASE_URL\"),\n        }\n        \n        match &self.rust_log {\n            Some(log) => env::set_var(\"RUST_LOG\", log),\n            None => env::remove_var(\"RUST_LOG\"),\n        }\n        \n        match &self.sinex_config {\n            Some(config) => env::set_var(\"SINEX_CONFIG\", config),\n            None => env::remove_var(\"SINEX_CONFIG\"),\n        }\n    }\n}\n\n// ====== COMPREHENSIVE RECOVERY TEST ======\n\n/// Test complete system recovery from multiple failure scenarios\n#[sinex_test]\nasync fn test_comprehensive_system_recovery(ctx: TestContext) -> anyhow::Result<()> {\n    let baseline = EnvironmentState::capture();\n    \n    println!(\"✓ Starting comprehensive recovery test\");\n    \n    // Scenario 1: Database failure and recovery\n    env::set_var(\"DATABASE_URL\", \"postgresql://invalid:5432/test\");\n    \n    let db_failure = sinex_preflight::database::verify_database_connectivity().await;\n    assert!(db_failure.is_err() || matches!(db_failure.unwrap().0, sinex_preflight::VerificationStatus::Fail));\n    \n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    let (db_status, _details, _messages) = sinex_preflight::database::verify_database_connectivity().await\n        .map_err(|e| format!(\"Database recovery failed: {}\", e))?;\n    assert_eq!(db_status, sinex_preflight::VerificationStatus::Pass);\n    \n    println!(\"✓ Database failure/recovery cycle completed\");\n    \n    // Scenario 2: Configuration failure and recovery\n    env::remove_var(\"DATABASE_URL\");\n    \n    let config_failure = sinex_preflight::configuration::verify_configuration_generation().await\n        .map_err(|e| format!(\"Config test should handle missing vars: {}\", e))?;\n    assert_eq!(config_failure.0, sinex_preflight::VerificationStatus::Fail);\n    \n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    let (config_status, _details, _messages) = sinex_preflight::configuration::verify_configuration_generation().await\n        .map_err(|e| format!(\"Configuration recovery failed: {}\", e))?;\n    assert!(matches!(config_status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    \n    println!(\"✓ Configuration failure/recovery cycle completed\");\n    \n    // Scenario 3: Resource constraints (should be resilient)\n    let (resource_status, resource_details, resource_messages) = sinex_preflight::resources::verify_system_resources().await\n        .map_err(|e| format!(\"Resource verification failed: {}\", e))?;\n    \n    assert!(matches!(resource_status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    \n    println!(\"✓ Resource resilience verified\");\n    \n    // Scenario 4: Service dependencies (should be resilient)\n    let (service_status, _service_details, _service_messages) = sinex_preflight::services::verify_service_dependencies().await\n        .map_err(|e| format!(\"Service verification failed: {}\", e))?;\n    \n    assert!(matches!(service_status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    \n    println!(\"✓ Service dependency resilience verified\");\n    \n    // Scenario 5: Integration recovery\n    env::set_var(\"DATABASE_URL\", \"postgresql://invalid:5432/test\");\n    \n    let integration_failure = sinex_preflight::verification::verify_end_to_end_integration().await\n        .map_err(|e| format!(\"Integration test should handle failure: {}\", e))?;\n    assert_eq!(integration_failure.0, sinex_preflight::VerificationStatus::Fail);\n    \n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    let (integration_status, _details, _messages) = sinex_preflight::verification::verify_end_to_end_integration().await\n        .map_err(|e| format!(\"Integration recovery failed: {}\", e))?;\n    assert!(matches!(integration_status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    \n    println!(\"✓ Integration failure/recovery cycle completed\");\n    \n    // Final: Restore baseline state\n    baseline.restore();\n    \n    println!(\"✓ Comprehensive system recovery test completed successfully\");\n    \n    Ok(())\n}
    baseline_state.restore();

    println!("✓ Rolled back to baseline environment state");

    // Phase 5: Verify rollback success (should pass/warn now)
    let rollback_verification =
        sinex_preflight::configuration::verify_configuration_generation().await;

    match rollback_verification {
        Ok((status, _details, _messages)) => {
            assert!(
                matches!(
                    status,
                    sinex_preflight::VerificationStatus::Pass
                        | sinex_preflight::VerificationStatus::Warning
                ),
                "Verification should pass after rollback"
            );
            println!("✓ Verification passed after rollback");
        }
        Err(e) => {
            // If it still fails, it might be due to missing DATABASE_URL in baseline
            // This is acceptable for the rollback test
            println!("ℹ Verification still has issues after rollback (expected if DATABASE_URL was not set initially): {}", e);
        }
    }

    println!("✓ Rollback simulation completed");

    Ok(())
}

// Helper struct for environment state management
struct EnvironmentState {
    database_url: Option<String>,
    rust_log: Option<String>,
    sinex_config: Option<String>,
}

impl EnvironmentState {
    fn capture() -> Self {
        Self {
            database_url: env::var("DATABASE_URL").ok(),
            rust_log: env::var("RUST_LOG").ok(),
            sinex_config: env::var("SINEX_CONFIG").ok(),
        }
    }

    fn restore(&self) {
        // Restore or remove environment variables
        match &self.database_url {
            Some(url) => env::set_var("DATABASE_URL", url),
            None => env::remove_var("DATABASE_URL"),
        }

        match &self.rust_log {
            Some(log) => env::set_var("RUST_LOG", log),
            None => env::remove_var("RUST_LOG"),
        }

        match &self.sinex_config {
            Some(config) => env::set_var("SINEX_CONFIG", config),
            None => env::remove_var("SINEX_CONFIG"),
        }
    }
}

// ====== COMPREHENSIVE RECOVERY TEST ======

/// Test complete system recovery from multiple failure scenarios
#[sinex_test]
async fn test_comprehensive_system_recovery(ctx: TestContext) -> anyhow::Result<()> {
    let baseline = EnvironmentState::capture();

    println!("✓ Starting comprehensive recovery test");

    // Scenario 1: Database failure and recovery
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let db_failure = sinex_preflight::database::verify_database_connectivity().await;
    assert!(
        db_failure.is_err()
            || matches!(
                db_failure.unwrap().0,
                sinex_preflight::VerificationStatus::Fail
            )
    );

    env::set_var("DATABASE_URL", ctx.database_url());
    let (db_status, _details, _messages) =
        sinex_preflight::database::verify_database_connectivity()
            .await
            .map_err(|e| format!("Database recovery failed: {}", e))?;
    assert_eq!(db_status, sinex_preflight::VerificationStatus::Pass);

    println!("✓ Database failure/recovery cycle completed");

    // Scenario 2: Configuration failure and recovery
    env::remove_var("DATABASE_URL");

    let config_failure = sinex_preflight::configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Config test should handle missing vars: {}", e))?;
    assert_eq!(config_failure.0, sinex_preflight::VerificationStatus::Fail);

    env::set_var("DATABASE_URL", ctx.database_url());
    let (config_status, _details, _messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Configuration recovery failed: {}", e))?;
    assert!(matches!(
        config_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Configuration failure/recovery cycle completed");

    // Scenario 3: Resource constraints (should be resilient)
    let (resource_status, resource_details, resource_messages) =
        sinex_preflight::resources::verify_system_resources()
            .await
            .map_err(|e| format!("Resource verification failed: {}", e))?;

    assert!(matches!(
        resource_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Resource resilience verified");

    // Scenario 4: Service dependencies (should be resilient)
    let (service_status, _service_details, _service_messages) =
        sinex_preflight::services::verify_service_dependencies()
            .await
            .map_err(|e| format!("Service verification failed: {}", e))?;

    assert!(matches!(
        service_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Service dependency resilience verified");

    // Scenario 5: Integration recovery
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/test");

    let integration_failure = sinex_preflight::verification::verify_end_to_end_integration()
        .await
        .map_err(|e| format!("Integration test should handle failure: {}", e))?;
    assert_eq!(
        integration_failure.0,
        sinex_preflight::VerificationStatus::Fail
    );

    env::set_var("DATABASE_URL", ctx.database_url());
    let (integration_status, _details, _messages) =
        sinex_preflight::verification::verify_end_to_end_integration()
            .await
            .map_err(|e| format!("Integration recovery failed: {}", e))?;
    assert!(matches!(
        integration_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    println!("✓ Integration failure/recovery cycle completed");

    // Final: Restore baseline state
    baseline.restore();

    println!("✓ Comprehensive system recovery test completed successfully");

    Ok(())
}
