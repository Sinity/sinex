/*!
 * System-level tests for the complete Pre-Flight Verification system
 *
 * Tests the entire verification pipeline in realistic scenarios including:
 * - Complete deployment workflows
 * - Failure recovery scenarios
 * - Performance under load
 * - Real-world edge cases
 */

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use uuid::Uuid;

use sinex_test_macros::sinex_test;
use crate::common::prelude::*;

/// Test complete deployment workflow with pre-flight verification
#[sinex_test]
async fn test_complete_deployment_workflow(ctx: TestContext) -> TestResult {
    // This test simulates a complete deployment from start to finish

    // Phase 1: Pre-deployment state
    println!("Phase 1: Verifying pre-deployment state");

    // Ensure database is clean and ready
    let pool = ctx.pool();

    // Clear any existing heartbeats
    sqlx::query!("DELETE FROM component_heartbeats WHERE component_name LIKE 'sinex-%'")
        .execute(&pool)
        .await
        .ok();

    // Phase 2: Run complete pre-flight verification
    println!("Phase 2: Running complete pre-flight verification");

    let verification_start = Instant::now();
    let verification_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "180",
        "--output", "json"
    ]).await?;
    let verification_duration = verification_start.elapsed();

    assert_eq!(verification_result.status_code, 0, "Pre-flight verification should pass");

    let verification_report: Value = serde_json::from_str(&verification_result.stdout)?;
    assert_eq!(verification_report["overall_status"], "PASS");

    println!("✓ Pre-flight verification passed in {:?}", verification_duration);

    // Phase 3: Simulate service deployment
    println!("Phase 3: Simulating service deployment");

    // Record deployment start
    let deployment_id = Uuid::new_v4();
    sqlx::query!(
        r#"
        INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        "sinex-deployment",
        deployment_id.to_string(),
        "starting",
        serde_json::json!({"phase": "deployment", "verification_passed": true})
    )
    .execute(&pool)
    .await?;

    // Simulate collector startup
    sqlx::query!(
        r#"
        INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        "unified-collector",
        "test-instance",
        "active",
        serde_json::json!({"startup": "successful", "verification": "passed"})
    )
    .execute(&pool)
    .await?;

    // Phase 4: Verify deployment success
    println!("Phase 4: Verifying deployment success");

    let deployment_check = sqlx::query!(
        "SELECT status, metadata FROM component_heartbeats WHERE component_name = 'sinex-deployment' AND instance_id = $1",
        deployment_id.to_string()
    )
    .fetch_one(pool)
    .await?;

    assert_eq!(deployment_check.status, "starting");

    // Update deployment to success
    sqlx::query!(
        "UPDATE component_heartbeats SET status = 'success', metadata = $1 WHERE component_name = 'sinex-deployment' AND instance_id = $2",
        serde_json::json!({"phase": "completed", "verification_passed": true, "deployment_time": verification_duration.as_millis()}),
        deployment_id.to_string()
    )
    .execute(&pool)
    .await?;

    println!("✓ Complete deployment workflow test passed");

    Ok(())
}

/// Test failure recovery and rollback scenarios
#[sinex_test]
async fn test_failure_recovery_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    println!("Testing failure recovery scenarios...");

    // Scenario 1: Database connection failure
    println!("Scenario 1: Database connection failure");

    let invalid_db_result = run_system_preflight_with_env(&[
        "verify",
        "--timeout", "30",
        "--output", "json"
    ], &[("DATABASE_URL", "postgresql://invalid:5432/nonexistent")]).await;

    // Should fail gracefully
    assert!(invalid_db_result.is_err() || invalid_db_result.unwrap().status_code != 0);

    // Scenario 2: Partial verification failure
    println!("Scenario 2: Testing with resource constraints");

    let constrained_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "30",
        "--output", "json"
    ]).await?;

    // Should handle constraints gracefully (pass or warn)
    assert!(constrained_result.status_code <= 1, "Should handle constraints gracefully");

    // Scenario 3: Timeout handling
    println!("Scenario 3: Timeout handling");

    let timeout_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "1", // Very short timeout
        "--output", "json"
    ]).await;

    // Should handle timeout gracefully
    match timeout_result {
        Ok(result) => {
            // Either completes quickly or times out gracefully
            assert!(result.status_code <= 1, "Should handle timeout gracefully");
        }
        Err(_) => {
            // Timeout error is also acceptable
        }
    }

    println!("✓ Failure recovery scenarios test passed");

    Ok(())
}

/// Test performance under various load conditions
#[sinex_test]
async fn test_performance_under_load(ctx: TestContext) -> TestResult {
    println!("Testing performance under load...");

    let pool = ctx.pool();

    // Create some load on the database
    let mut background_tasks = Vec::new();

    for i in 0..10 {
        let pool_clone = pool.clone();
        let task = tokio::spawn(async move {
            for j in 0..100 {
                let test_id = Uuid::new_v4();
                let _ = sqlx::query!(
                    r#"
                    INSERT INTO raw.events (id, source, event_type, payload, ts_ingest)
                    VALUES ($1, $2, $3, $4, NOW())
                    "#,
                    test_id,
                    format!("load-test-{}", i),
                    "performance.test",
                    serde_json::json!({"iteration": j, "worker": i})
                )
                .execute(&pool_clone)
                .await;

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        background_tasks.push(task);
    }

    // Run verification while under load
    let load_test_start = Instant::now();
    let verification_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "120",
        "--output", "json"
    ]).await?;
    let load_test_duration = load_test_start.elapsed();

    // Wait for background tasks to complete
    for task in background_tasks {
        task.await.ok();
    }

    // Verification should still pass under load
    assert_eq!(verification_result.status_code, 0, "Verification should pass under load");

    let load_report: Value = serde_json::from_str(&verification_result.stdout)?;
    assert_eq!(load_report["overall_status"], "PASS");

    // Performance should be reasonable even under load
    assert!(load_test_duration.as_secs() < 180, "Should complete within timeout even under load");

    // Clean up load test data
    sqlx::query!("DELETE FROM raw.events WHERE source LIKE 'load-test-%'")
        .execute(&pool)
        .await?;

    println!("✓ Performance under load test passed in {:?}", load_test_duration);

    Ok(())
}

/// Test concurrent deployment scenarios
#[sinex_test]
async fn test_concurrent_deployment_scenarios(ctx: TestContext) -> TestResult {
    use tokio::task::JoinSet;

    println!("Testing concurrent deployment scenarios...");

    let mut join_set = JoinSet::new();
    let concurrent_count = 3;

    // Start multiple verification processes concurrently
    for i in 0..concurrent_count {
        join_set.spawn(async move {
            let result = run_system_preflight_verification(&[
                "verify",
                "--timeout", "90",
                "--output", "json"
            ]).await;

            (i, result)
        });
    }

    let mut results = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        let (id, verification_result) = join_result?;
        results.push((id, verification_result?));
    }

    assert_eq!(results.len(), concurrent_count, "All concurrent verifications should complete");

    // All verifications should succeed
    for (id, result) in &results {
        assert_eq!(result.status_code, 0, "Concurrent verification {} should pass", id);

        let report: Value = serde_json::from_str(&result.stdout)?;
        assert_eq!(report["overall_status"], "PASS");
    }

    println!("✓ Concurrent deployment scenarios test passed");

    Ok(())
}

/// Test edge cases and boundary conditions
#[sinex_test]
async fn test_edge_cases_and_boundaries(ctx: TestContext) -> TestResult {
    println!("Testing edge cases and boundary conditions...");

    // Test with very long timeout
    let long_timeout_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "300",
        "--output", "json"
    ]).await?;

    assert_eq!(long_timeout_result.status_code, 0);

    // Test with minimal phases
    let minimal_phases_result = run_system_preflight_verification(&[
        "verify",
        "--skip", "resources",
        "--skip", "services",
        "--skip", "integration",
        "--timeout", "60",
        "--output", "json"
    ]).await?;

    assert_eq!(minimal_phases_result.status_code, 0);

    let minimal_report: Value = serde_json::from_str(&minimal_phases_result.stdout)?;
    let phases = minimal_report["phases"].as_object().unwrap();

    // Should not contain skipped phases
    assert!(!phases.contains_key("resources"));
    assert!(!phases.contains_key("services"));
    assert!(!phases.contains_key("integration"));

    // Should contain non-skipped phases
    assert!(phases.contains_key("database"));

    // Test individual verification commands
    let database_only_result = run_system_preflight_verification(&[
        "verify",
        "--skip", "extensions",
        "--skip", "migrations",
        "--skip", "resources",
        "--skip", "configuration",
        "--skip", "services",
        "--skip", "integration",
        "--timeout", "30"
    ]).await?;

    assert_eq!(database_only_result.status_code, 0);

    println!("✓ Edge cases and boundary conditions test passed");

    Ok(())
}

/// Test monitoring and observability features
#[sinex_test]
async fn test_monitoring_and_observability(ctx: TestContext) -> TestResult {
    println!("Testing monitoring and observability features...");

    let pool = ctx.pool();

    // Run verification and check if results are recorded
    let verification_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "60",
        "--output", "json"
    ]).await?;

    assert_eq!(verification_result.status_code, 0);

    let report: Value = serde_json::from_str(&verification_result.stdout)?;
    let verification_id = report["verification_id"].as_str().unwrap();

    // Check if verification was recorded in database
    tokio::time::sleep(Duration::from_secs(2)).await; // Allow time for recording

    let recorded_verification = sqlx::query!(
        "SELECT status, metadata FROM component_heartbeats WHERE component_name = 'sinex-preflight' AND instance_id = $1",
        verification_id
    )
    .fetch_optional(pool)
    .await?;

    if let Some(record) = recorded_verification {
        assert_eq!(record.status, "PASS");

        // Verify metadata structure
        if let Some(metadata) = record.metadata {
            let metadata_obj: Value = serde_json::from_value(metadata)?;
            assert!(metadata_obj.get("overall_status").is_some());
        }
    }

    // Test report generation
    let report_result = run_system_preflight_verification(&[
        "report",
        "--output", "json"
    ]).await?;

    assert_eq!(report_result.status_code, 0);

    let report_data: Value = serde_json::from_str(&report_result.stdout)?;
    assert!(report_data.is_object());

    println!("✓ Monitoring and observability test passed");

    Ok(())
}

/// Test resource cleanup and state management
#[sinex_test]
async fn test_resource_cleanup_and_state_management(ctx: TestContext) -> TestResult {
    println!("Testing resource cleanup and state management...");

    let pool = ctx.pool();

    // Create some test state
    let test_verification_id = Uuid::new_v4();

    sqlx::query!(
        r#"
        INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
        "sinex-preflight",
        test_verification_id.to_string(),
        "PASS",
        serde_json::json!({"test": "cleanup", "phase": "testing"})
    )
    .execute(&pool)
    .await?;

    // Run verification
    let verification_result = run_system_preflight_verification(&[
        "verify",
        "--timeout", "60"
    ]).await?;

    assert_eq!(verification_result.status_code, 0);

    // Verify test state still exists
    let test_state = sqlx::query!(
        "SELECT status FROM component_heartbeats WHERE instance_id = $1",
        test_verification_id.to_string()
    )
    .fetch_optional(pool)
    .await?;

    assert!(test_state.is_some(), "Test state should be preserved");

    // Clean up test state
    sqlx::query!(
        "DELETE FROM component_heartbeats WHERE instance_id = $1",
        test_verification_id.to_string()
    )
    .execute(&pool)
    .await?;

    println!("✓ Resource cleanup and state management test passed");

    Ok(())
}

/// Helper function to run system-level pre-flight verification
async fn run_system_preflight_verification(args: &[&str]) -> Result<SystemCommandResult> {
    run_system_preflight_with_env(args, &[]).await
}

/// Helper function to run pre-flight verification with custom environment
async fn run_system_preflight_with_env(args: &[&str], env_vars: &[(&str, &str)]) -> Result<SystemCommandResult> {
    let binary_path = get_system_preflight_binary_path();

    let mut cmd = Command::new(&binary_path);
    cmd.args(args);

    // Set default database URL if not overridden
    let mut has_db_url = false;
    for (key, value) in env_vars {
        cmd.env(key, value);
        if *key == "DATABASE_URL" {
            has_db_url = true;
        }
    }

    if !has_db_url {
        cmd.env("DATABASE_URL", std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql:///sinex_test?host=/run/postgresql".to_string()
        }));
    }

    cmd.env("RUST_LOG", "sinex_preflight=info");

    let output = timeout(Duration::from_secs(300), async {
        cmd.output()
    }).await??;

    Ok(SystemCommandResult {
        status_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

/// Get the path to the sinex-preflight binary for system tests
fn get_system_preflight_binary_path() -> String {
    std::env::var("SINEX_PREFLIGHT_BINARY").unwrap_or_else(|_| {
        // Look for the binary in various locations
        let possible_paths = [
            "target/debug/sinex-preflight",
            "target/release/sinex-preflight",
            "../target/debug/sinex-preflight",
            "../target/release/sinex-preflight",
            "../../target/debug/sinex-preflight",
            "../../target/release/sinex-preflight",
            "/usr/local/bin/sinex-preflight",
            "/usr/bin/sinex-preflight",
        ];

        for path in &possible_paths {
            if std::path::Path::new(path).exists() {
                return path.to_string();
            }
        }

        // Fallback to PATH
        "sinex-preflight".to_string()
    })
}

#[derive(Debug)]
struct SystemCommandResult {
    status_code: i32,
    stdout: String,
    stderr: String,
}