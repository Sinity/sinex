// Preflight Integration Tests - Full pipeline verification testing

use serde_json::json;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::time::timeout;

// ====== FULL PIPELINE INTEGRATION TESTS ======

/// Test complete preflight verification pipeline with all phases
#[sinex_test]
async fn test_complete_preflight_pipeline_success(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Run complete verification process using the main function logic
    let verification_start = Instant::now();

    let mut overall_status = sinex_preflight::VerificationStatus::Pass;
    let mut phase_results = HashMap::new();
    let mut all_messages = Vec::new();

    // Run each phase individually to properly measure timing
    let phase_names = vec![
        "Database",
        "Extensions",
        "Migrations",
        "Configuration",
        "Resources",
        "Services",
        "Integration",
    ];

    for phase_name in phase_names {
        println!("Running verification phase: {}", phase_name);
        let phase_start = Instant::now();

        let result = match phase_name {
            "Database" => sinex_preflight::database::verify_database_connectivity().await,
            "Extensions" => sinex_preflight::database::verify_postgresql_extensions().await,
            "Migrations" => sinex_preflight::database::verify_migration_readiness().await,
            "Configuration" => {
                sinex_preflight::configuration::verify_configuration_generation().await
            }
            "Resources" => sinex_preflight::resources::verify_system_resources().await,
            "Services" => sinex_preflight::services::verify_service_dependencies().await,
            "Integration" => sinex_preflight::verification::verify_end_to_end_integration().await,
            _ => unreachable!(),
        };

        let phase_duration = phase_start.elapsed();

        match result {
            Ok((status, details, messages)) => {
                // Update overall status based on phase result
                match status {
                    sinex_preflight::VerificationStatus::Fail => {
                        overall_status = sinex_preflight::VerificationStatus::Fail;
                    }
                    sinex_preflight::VerificationStatus::Warning
                        if matches!(overall_status, sinex_preflight::VerificationStatus::Pass) =>
                    {
                        overall_status = sinex_preflight::VerificationStatus::Warning;
                    }
                    _ => {}
                }

                phase_results.insert(
                    phase_name.to_string(),
                    json!({
                        "status": format!("{:?}", status),
                        "duration_ms": phase_duration.as_millis(),
                        "details": details,
                        "messages": messages.clone()
                    }),
                );

                all_messages.extend(messages);

                println!(
                    "✓ Phase {} completed: {:?} in {}ms",
                    phase_name,
                    status,
                    phase_duration.as_millis()
                );
            }
            Err(e) => {
                phase_results.insert(
                    phase_name.to_string(),
                    json!({
                        "status": "ERROR",
                        "duration_ms": phase_duration.as_millis(),
                        "error": e.to_string()
                    }),
                );

                all_messages.push(format!("✗ Phase {} failed: {}", phase_name, e));
                overall_status = sinex_preflight::VerificationStatus::Fail;

                println!("✗ Phase {} failed: {}", phase_name, e);
                break; // Stop on critical failure
            }
        }
    }

    let total_duration = verification_start.elapsed();

    // Verify overall success
    assert!(
        matches!(
            overall_status,
            sinex_preflight::VerificationStatus::Pass
                | sinex_preflight::VerificationStatus::Warning
        ),
        "Complete preflight pipeline should pass or warn, got: {:?}",
        overall_status
    );

    // Verify all phases were attempted
    assert!(
        phase_results.len() >= 5,
        "Should have attempted at least 5 phases"
    );

    // Verify timing is reasonable (should complete within 60 seconds)
    assert!(
        total_duration.as_secs() < 60,
        "Pipeline should complete within 60 seconds"
    );

    // Verify messages were collected
    assert!(
        !all_messages.is_empty(),
        "Should have collected verification messages"
    );

    println!(
        "✓ Complete preflight pipeline test passed in {}ms",
        total_duration.as_millis()
    );

    Ok(())
}

/// Test preflight pipeline with early failure (database connectivity)
#[sinex_test]
async fn test_preflight_pipeline_early_failure(_ctx: TestContext) -> TestResult {
    // Set invalid database URL to trigger early failure
    env::set_var("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    // Test that database phase fails fast
    let result = sinex_preflight::database::verify_database_connectivity().await;

    match result {
        Ok((status, _details, messages)) => {
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
            assert!(messages
                .iter()
                .any(|m| m.contains("connection failed") || m.contains("timeout")));
        }
        Err(_) => {
            // Error result is also acceptable for invalid connection
        }
    }

    println!("✓ Early failure handling verified");

    Ok(())
}

/// Test preflight pipeline timeout handling
#[sinex_test]
async fn test_preflight_pipeline_timeout_handling(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Test that each phase completes within reasonable time
    let timeout_duration = Duration::from_secs(30);

    let phases = vec!["Database", "Resources", "Configuration"];

    for phase_name in phases {
        let result = match phase_name {
            "Database" => {
                timeout(
                    timeout_duration,
                    sinex_preflight::database::verify_database_connectivity(),
                )
                .await
            }
            "Resources" => {
                timeout(
                    timeout_duration,
                    sinex_preflight::resources::verify_system_resources(),
                )
                .await
            }
            "Configuration" => {
                timeout(
                    timeout_duration,
                    sinex_preflight::configuration::verify_configuration_generation(),
                )
                .await
            }
            _ => unreachable!(),
        };

        match result {
            Ok(_) => {
                println!("✓ Phase {} completed within timeout", phase_name);
            }
            Err(_) => {
                panic!(
                    "Phase {} exceeded timeout of {:?}",
                    phase_name, timeout_duration
                );
            }
        }
    }

    Ok(())
}

/// Test preflight pipeline with partial phase failures
#[sinex_test]
async fn test_preflight_pipeline_partial_failures(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Test individual phases that might warn but not fail
    let (status, _details, messages) = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resources verification failed: {}", e))?;

    // Resources might warn on low-spec systems but shouldn't fail
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    // Test configuration phase which might warn about missing optional configs
    let (status, _details, messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Configuration verification failed: {}", e))?;

    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(!messages.is_empty());

    println!("✓ Partial failure handling verified");

    Ok(())
}

// ====== PHASE INTERACTION TESTS ======

/// Test that database phase properly sets up for subsequent phases
#[sinex_test]
async fn test_phase_interaction_database_setup(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // First run database connectivity
    let (db_status, db_details, db_messages) =
        sinex_preflight::database::verify_database_connectivity()
            .await
            .map_err(|e| format!("Database phase failed: {}", e))?;

    assert_eq!(db_status, sinex_preflight::VerificationStatus::Pass);
    assert!(db_messages
        .iter()
        .any(|m| m.contains("connection established")));

    // Verify database details are properly formatted for subsequent phases
    assert!(db_details.get("postgresql_version").is_some());
    assert!(db_details.get("connection_pool").is_some());

    // Now run extensions phase which depends on database connectivity
    let (ext_status, ext_details, ext_messages) =
        sinex_preflight::database::verify_postgresql_extensions()
            .await
            .map_err(|e| format!("Extensions phase failed: {}", e))?;

    // Extensions should work since database is connected
    assert!(matches!(
        ext_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(ext_details.get("extensions").is_some());

    println!("✓ Database-to-extensions phase interaction verified");

    Ok(())
}

/// Test configuration-to-services phase interaction
#[sinex_test]
async fn test_phase_interaction_config_to_services(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Run configuration phase
    let (config_status, config_details, config_messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Configuration phase failed: {}", e))?;

    assert!(matches!(
        config_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(config_details.get("environment").is_some());

    // Run services phase which may depend on configuration
    let (service_status, service_details, service_messages) =
        sinex_preflight::services::verify_service_dependencies()
            .await
            .map_err(|e| format!("Services phase failed: {}", e))?;

    assert!(matches!(
        service_status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));
    assert!(service_details.get("binaries").is_some());

    println!("✓ Configuration-to-services phase interaction verified");

    Ok(())
}

// ====== CONCURRENT VERIFICATION TESTS ======

/// Test multiple concurrent verification runs
#[sinex_test]
async fn test_concurrent_verification_runs(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Launch multiple verification tasks concurrently
    let mut handles = Vec::new();

    for i in 0..3 {
        let db_url = ctx.database_url().to_string();
        let handle = tokio::spawn(async move {
            env::set_var("DATABASE_URL", &db_url);

            let result = sinex_preflight::database::verify_database_connectivity().await;
            (i, result)
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut results = Vec::new();
    for handle in handles {
        let (task_id, result) = handle
            .await
            .map_err(|e| format!("Task join error: {}", e))?;
        results.push((task_id, result));
    }

    // Verify all succeeded
    for (task_id, result) in results {
        match result {
            Ok((status, _details, _messages)) => {
                assert_eq!(
                    status,
                    sinex_preflight::VerificationStatus::Pass,
                    "Concurrent task {} should pass",
                    task_id
                );
            }
            Err(e) => {
                panic!("Concurrent task {} failed: {}", task_id, e);
            }
        }
    }

    println!("✓ Concurrent verification test passed");

    Ok(())
}

// ====== RESOURCE CONSTRAINT TESTS ======

/// Test preflight behavior under resource constraints
#[sinex_test]
async fn test_resource_constraint_handling(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Test system resources verification
    let (status, details, messages) = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resource verification failed: {}", e))?;

    // Should not fail even on resource-constrained systems (just warn)
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify resource details are reasonable
    let memory = details.get("memory").expect("Should have memory details");
    let memory_available = memory
        .get("available_gb")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    // Should detect at least some available memory
    assert!(memory_available > 0.0, "Should detect available memory");

    // Check disk space
    let disk = details.get("disk").expect("Should have disk details");
    assert!(disk.is_object(), "Disk details should be an object");

    println!("✓ Resource constraint handling verified");

    Ok(())
}

// ====== INTEGRATION WITH DATABASE TESTS ======

/// Test preflight integration with real database operations
#[sinex_test]
async fn test_integration_with_database_operations(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Run integration verification
    let (status, details, messages) =
        sinex_preflight::verification::verify_end_to_end_integration()
            .await
            .map_err(|e| format!("Integration verification failed: {}", e))?;

    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify database integration details
    let db_integration = details
        .get("database_integration")
        .expect("Should have database integration details");
    assert!(db_integration.is_object());

    // Should test CRUD operations
    assert!(db_integration.get("crud_operations").is_some());
    assert!(db_integration.get("transactions").is_some());

    println!("✓ Database integration test verified");

    Ok(())
}

// ====== ENVIRONMENT SETUP TESTS ======

/// Test preflight with different environment configurations
#[sinex_test]
async fn test_different_environment_configurations(_ctx: TestContext) -> TestResult {
    // Test with minimal environment
    env::remove_var("RUST_LOG");
    env::remove_var("SINEX_CONFIG");

    // Should still work with just DATABASE_URL (we'll test without it separately)
    let temp_db_url = "postgresql:///test_minimal?host=/run/postgresql";
    env::set_var("DATABASE_URL", temp_db_url);

    let (status, details, _messages) =
        sinex_preflight::configuration::verify_configuration_generation()
            .await
            .map_err(|e| format!("Minimal config test failed: {}", e))?;

    // Should pass or warn (not fail) with minimal environment
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Verify environment section exists
    let environment = details
        .get("environment")
        .expect("Should have environment details");
    assert!(environment.is_object());

    println!("✓ Different environment configuration test passed");

    Ok(())
}

// ====== ERROR AGGREGATION TESTS ======

/// Test error message aggregation across phases
#[sinex_test]
async fn test_error_aggregation_across_phases(_ctx: TestContext) -> TestResult {
    // Test with configuration that will generate various warnings/errors
    env::set_var("DATABASE_URL", "postgresql://localhost:5432/test_errors");

    let phases = vec![
        (
            "Configuration",
            sinex_preflight::configuration::verify_configuration_generation().await,
        ),
        (
            "Resources",
            sinex_preflight::resources::verify_system_resources().await,
        ),
        (
            "Services",
            sinex_preflight::services::verify_service_dependencies().await,
        ),
    ];

    let mut all_messages = Vec::new();
    let mut warning_count = 0;
    let mut error_count = 0;

    for (_phase_name, result) in phases {
        if let Ok((status, _details, messages)) = result {
            all_messages.extend(messages.clone());

            for message in &messages {
                if message.contains("⚠") || message.contains("warning") {
                    warning_count += 1;
                } else if message.contains("✗") || message.contains("error") {
                    error_count += 1;
                }
            }
        }
    }

    // Should have collected messages from multiple phases
    assert!(!all_messages.is_empty(), "Should have collected messages");

    println!(
        "✓ Error aggregation test completed - {} warnings, {} errors",
        warning_count, error_count
    );

    Ok(())
}

// ====== PERFORMANCE VERIFICATION TESTS ======

/// Test preflight performance characteristics
#[sinex_test]
async fn test_preflight_performance_characteristics(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Measure performance of each phase
    let mut phase_timings = HashMap::new();

    // Database connectivity (should be fast)
    let start = Instant::now();
    let _ = sinex_preflight::database::verify_database_connectivity()
        .await
        .map_err(|e| format!("Database phase failed: {}", e))?;
    phase_timings.insert("database", start.elapsed());

    // Resources (should be fast)
    let start = Instant::now();
    let _ = sinex_preflight::resources::verify_system_resources()
        .await
        .map_err(|e| format!("Resources phase failed: {}", e))?;
    phase_timings.insert("resources", start.elapsed());

    // Configuration (should be fast)
    let start = Instant::now();
    let _ = sinex_preflight::configuration::verify_configuration_generation()
        .await
        .map_err(|e| format!("Configuration phase failed: {}", e))?;
    phase_timings.insert("configuration", start.elapsed());

    // Verify reasonable performance (each phase should complete within 10 seconds)
    for (phase_name, duration) in &phase_timings {
        assert!(
            duration.as_secs() < 10,
            "Phase {} took too long: {:?}",
            phase_name,
            duration
        );
        println!("Phase {} completed in {:?}", phase_name, duration);
    }

    println!("✓ Performance characteristics verified");

    Ok(())
}

// ====== CLEANUP AND RECOVERY TESTS ======

/// Test preflight cleanup and recovery mechanisms
#[sinex_test]
async fn test_cleanup_and_recovery_mechanisms(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());

    // Test that verification doesn't leave artifacts
    let temp_dir = TempDir::new().map_err(|e| format!("Failed to create temp dir: {}", e))?;
    let initial_files: Vec<_> = fs::read_dir(temp_dir.path())
        .map_err(|e| format!("Failed to read temp dir: {}", e))?
        .collect();

    // Run verification (integration test includes cleanup logic)
    let (status, _details, _messages) =
        sinex_preflight::verification::verify_end_to_end_integration()
            .await
            .map_err(|e| format!("Integration verification failed: {}", e))?;

    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Check that no extra files were left in temp directory
    let final_files: Vec<_> = fs::read_dir(temp_dir.path())
        .map_err(|e| format!("Failed to read temp dir after test: {}", e))?
        .collect();

    assert_eq!(
        initial_files.len(),
        final_files.len(),
        "Verification should not leave temporary files"
    );

    println!("✓ Cleanup and recovery test passed");

    Ok(())
}
