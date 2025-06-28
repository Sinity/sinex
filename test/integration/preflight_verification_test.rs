/*!
 * Integration tests for Sinex Pre-Flight Verification system
 *
 * Tests the complete pre-flight verification workflow including:
 * - Database verification
 * - Extension checking
 * - Migration dry-runs
 * - Resource validation
 * - Configuration testing
 * - Service integration
 * - End-to-end verification
 */

use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::env;
use std::process::Command;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

use sinex_test_macros::sinex_test;
use crate::common::prelude::*;

/// Test complete pre-flight verification workflow
#[sinex_test]
async fn test_complete_preflight_verification(ctx: TestContext) -> TestResult {
    // Test that the complete verification pipeline works end-to-end
    let result = run_preflight_verification(&["verify", "--timeout", "60"]).await?;

    assert_eq!(result.status_code, 0, "Pre-flight verification should pass");
    assert!(result.stdout.contains("✓"), "Should contain success indicators");

    // Parse JSON output
    let verification_report: serde_json::Value = serde_json::from_str(&result.stdout)
        .expect("Verification should output valid JSON");

    assert_eq!(verification_report["overall_status"], "PASS");
    assert!(verification_report["phases"].is_object());

    Ok(())
}

/// Test database verification phase
#[sinex_test]
async fn test_database_verification_phase(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test basic database connectivity verification
    let result = run_preflight_verification(&["verify", "--skip", "extensions", "--skip", "migrations", "--skip", "resources", "--skip", "configuration", "--skip", "services", "--skip", "integration"]).await?;

    assert_eq!(result.status_code, 0, "Database verification should pass");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;
    let db_phase = &report["phases"]["database"];

    assert_eq!(db_phase["status"], "PASS");
    assert!(db_phase["duration_ms"].as_u64().unwrap() > 0);

    Ok(())
}

/// Test extension verification phase
#[sinex_test]
async fn test_extension_verification_phase(ctx: TestContext) -> TestResult {
    let result = run_preflight_verification(&["extension-check"]).await?;

    // Extension check should pass or warn, but not fail completely in test environment
    assert!(result.status_code == 0 || result.status_code == 1, "Extension check should not crash");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;

    // Should have checked for required extensions
    assert!(report["phase"] == "extension_check");
    assert!(report["details"].is_object());

    Ok(())
}

/// Test migration dry-run verification
#[sinex_test]
async fn test_migration_dry_run_verification(ctx: TestContext) -> TestResult {
    let result = run_preflight_verification(&["migration-dry-run"]).await?;

    assert_eq!(result.status_code, 0, "Migration dry-run should pass");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;
    assert_eq!(report["phase"], "migration_dry_run");
    assert_eq!(report["status"], "PASS");

    Ok(())
}

/// Test resource verification phase
#[sinex_test]
async fn test_resource_verification_phase(ctx: TestContext) -> TestResult {
    let result = run_preflight_verification(&["resource-check"]).await?;

    // Resource check might warn but shouldn't fail in test environment
    assert!(result.status_code <= 1, "Resource check should pass or warn");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;
    assert_eq!(report["phase"], "resource_check");

    // Should have checked memory, disk, CPU, etc.
    if let Some(details) = report["details"].as_object() {
        // At least some resource checks should have been performed
        assert!(!details.is_empty(), "Should have performed resource checks");
    }

    Ok(())
}

/// Test verification report generation
#[sinex_test]
async fn test_verification_report_generation(ctx: TestContext) -> TestResult {
    // First run a verification to generate some data
    let _ = run_preflight_verification(&["verify", "--timeout", "30"]).await?;

    // Then generate a report
    let result = run_preflight_verification(&["report"]).await?;

    assert_eq!(result.status_code, 0, "Report generation should succeed");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;

    // Should have information about recent verifications
    assert!(report.is_object(), "Report should be a JSON object");

    Ok(())
}

/// Test verification with timeout
#[sinex_test]
async fn test_verification_timeout_handling(ctx: TestContext) -> TestResult {
    // Test with a very short timeout to ensure timeout handling works
    let result = run_preflight_verification(&["verify", "--timeout", "1"]).await?;

    // Should either complete quickly or timeout gracefully
    assert!(result.status_code <= 1, "Should handle timeout gracefully");

    if result.status_code == 1 {
        // If it timed out, should have appropriate error message
        assert!(
            result.stderr.contains("timeout") || result.stdout.contains("timeout"),
            "Should indicate timeout occurred"
        );
    }

    Ok(())
}

/// Test verification with skip phases
#[sinex_test]
async fn test_verification_skip_phases(ctx: TestContext) -> TestResult {
    let result = run_preflight_verification(&[
        "verify",
        "--skip", "resources",
        "--skip", "services",
        "--timeout", "30"
    ]).await?;

    assert_eq!(result.status_code, 0, "Verification with skipped phases should pass");

    let report: serde_json::Value = serde_json::from_str(&result.stdout)?;
    let phases = report["phases"].as_object().unwrap();

    // Should not contain skipped phases
    assert!(!phases.contains_key("resources"), "Resources phase should be skipped");
    assert!(!phases.contains_key("services"), "Services phase should be skipped");

    // Should contain non-skipped phases
    assert!(phases.contains_key("database"), "Database phase should be present");

    Ok(())
}

/// Test database integration scenarios
#[sinex_test]
async fn test_database_integration_scenarios(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test 1: Verify we can detect existing tables
    sqlx::query!("CREATE TABLE IF NOT EXISTS test_verification_table (id UUID PRIMARY KEY, data TEXT)")
        .execute(pool)
        .await?;

    let result = run_preflight_verification(&["verify", "--timeout", "30"]).await?;
    assert_eq!(result.status_code, 0, "Should handle existing tables correctly");

    // Test 2: Clean up
    sqlx::query!("DROP TABLE IF EXISTS test_verification_table")
        .execute(pool)
        .await?;

    Ok(())
}

/// Test concurrent verification runs
#[sinex_test]
async fn test_concurrent_verification_runs(ctx: TestContext) -> TestResult {
    use tokio::task::JoinSet;

    let mut join_set = JoinSet::new();

    // Spawn multiple verification runs concurrently
    for i in 0..3 {
        join_set.spawn(async move {
            let result = run_preflight_verification(&["verify", "--timeout", "45"]).await;
            (i, result)
        });
    }

    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        let (id, verification_result) = result?;
        results.push((id, verification_result?));
    }

    assert_eq!(results.len(), 3, "All concurrent verifications should complete");

    // All should succeed (they should be able to run concurrently)
    for (id, result) in results {
        assert_eq!(result.status_code, 0, "Concurrent verification {} should pass", id);
    }

    Ok(())
}

/// Test verification error handling
#[sinex_test]
async fn test_verification_error_handling(ctx: TestContext) -> TestResult {
    // Test with invalid database URL
    let mut cmd = Command::new(get_preflight_binary_path());
    cmd.args(&["verify", "--timeout", "10"])
        .env("DATABASE_URL", "postgresql://invalid:5432/nonexistent");

    let output = cmd.output()?;

    // Should fail gracefully with database connection error
    assert_ne!(output.status.code().unwrap_or(0), 0, "Should fail with invalid database");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("database") || stderr.contains("connection"),
        "Should indicate database connection issue"
    );

    Ok(())
}

/// Test verification JSON output format
#[sinex_test]
async fn test_verification_json_output_format(ctx: TestContext) -> TestResult {
    let result = run_preflight_verification(&["verify", "--output", "json", "--timeout", "30"]).await?;

    // Parse as JSON to validate format
    let report: serde_json::Value = serde_json::from_str(&result.stdout)
        .expect("JSON output should be valid");

    // Verify required fields
    assert!(report.get("overall_status").is_some(), "Should have overall_status");
    assert!(report.get("verification_id").is_some(), "Should have verification_id");
    assert!(report.get("started_at").is_some(), "Should have started_at");
    assert!(report.get("phases").is_some(), "Should have phases");
    assert!(report.get("system_info").is_some(), "Should have system_info");

    // Verify system info structure
    let system_info = &report["system_info"];
    assert!(system_info.get("hostname").is_some(), "Should have hostname");
    assert!(system_info.get("available_memory_gb").is_some(), "Should have memory info");
    assert!(system_info.get("cpu_count").is_some(), "Should have CPU info");

    Ok(())
}

/// Test verification with environment variable configuration
#[sinex_test]
async fn test_verification_environment_configuration(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create a temporary config file
    let temp_dir = std::env::temp_dir();
    let config_path = temp_dir.join("sinex-preflight-test.toml");

    let config_content = r#"
[database]
timeout_seconds = 15

[verification]
skip_phases = []
"#;

    std::fs::write(&config_path, config_content)?;

    let mut cmd = Command::new(get_preflight_binary_path());
    cmd.args(&["verify", "--timeout", "30"])
        .env("SINEX_CONFIG", config_path.to_str().unwrap());

    let output = cmd.output()?;

    // Clean up
    std::fs::remove_file(&config_path).ok();

    let result = CommandResult {
        status_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    };

    assert_eq!(result.status_code, 0, "Verification with config file should pass");

    Ok(())
}

/// Helper function to run pre-flight verification commands
async fn run_preflight_verification(args: &[&str]) -> Result<CommandResult> {
    let binary_path = get_preflight_binary_path();

    let mut cmd = Command::new(&binary_path);
    cmd.args(args)
        .env("DATABASE_URL", env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql:///sinex_test?host=/run/postgresql".to_string()
        }))
        .env("RUST_LOG", "sinex_preflight=info");

    let output = timeout(Duration::from_secs(120), async {
        cmd.output()
    }).await??;

    Ok(CommandResult {
        status_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

/// Get the path to the sinex-preflight binary
fn get_preflight_binary_path() -> String {
    env::var("SINEX_PREFLIGHT_BINARY").unwrap_or_else(|_| {
        // Try to find it in target directory
        let possible_paths = [
            "target/debug/sinex-preflight",
            "target/release/sinex-preflight",
            "../target/debug/sinex-preflight",
            "../target/release/sinex-preflight",
            "../../target/debug/sinex-preflight",
            "../../target/release/sinex-preflight",
        ];

        for path in &possible_paths {
            if std::path::Path::new(path).exists() {
                return path.to_string();
            }
        }

        // Fallback to assuming it's in PATH
        "sinex-preflight".to_string()
    })
}

#[derive(Debug)]
struct CommandResult {
    status_code: i32,
    stdout: String,
    stderr: String,
}