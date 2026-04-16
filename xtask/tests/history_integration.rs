//! Integration tests for xtask history database pipeline
//!
//! Tests the core history database API including invocation recording,
//! diagnostic tracking, stage timing, and delta computation.

use std::time::{SystemTime, UNIX_EPOCH};
use xtask::cargo_diagnostics::CompilerDiagnostic;
use xtask::history::{HistoryDb, InvocationStatus};
use xtask::sandbox::sinex_test;

// ============================================================================
// Helpers
// ============================================================================

fn make_diag(message: &str, package: &str) -> CompilerDiagnostic {
    CompilerDiagnostic {
        level: "error".to_string(),
        code: Some("E0001".to_string()),
        message: message.to_string(),
        file_path: Some("src/lib.rs".to_string()),
        line: Some(1),
        column: Some(1),
        rendered: None,
        suggestion: None,
        package: Some(package.to_string()),
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
    }
}

/// Generate a unique temporary database path.
fn temp_db_path() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("xtask-hist-test-{nonce}.db"))
}

/// Clean up a temporary database file and its associated WAL/SHM files.
fn cleanup_db(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let wal = path.with_extension("db-wal");
    let shm = path.with_extension("db-shm");
    let _ = std::fs::remove_file(&wal);
    let _ = std::fs::remove_file(&shm);
}

#[sinex_test]
async fn test_recording_chain_for_diagnostics() -> xtask::sandbox::TestResult<()> {
    let db_path = temp_db_path();

    let db = HistoryDb::open(&db_path)?;

    // Start an invocation
    let inv_id = db.start_invocation("check", None, None, None)?;
    assert!(inv_id > 0);

    // Record a diagnostic
    let diag = CompilerDiagnostic {
        level: "warning".to_string(),
        code: Some("dead_code".to_string()),
        message: "unused variable".to_string(),
        file_path: Some("src/lib.rs".to_string()),
        line: Some(42),
        column: Some(5),
        rendered: None,
        suggestion: None,
        package: Some("sinex-primitives".to_string()),
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
    };

    db.record_diagnostic(inv_id, &diag)?;

    // Record compiled packages for the invocation (needed for get_current_diagnostics CTE join)
    let mut packages = std::collections::HashSet::new();
    packages.insert("sinex-primitives".to_string());
    db.record_compiled_packages(inv_id, &packages)?;

    // Finish the invocation
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.5)?;

    // Query current diagnostics for "check" command
    let diagnostics = db.get_current_diagnostics(None, None, None, Some("check"), false)?;

    // Verify we got the diagnostic back
    assert!(!diagnostics.is_empty());
    let stored = diagnostics.iter().find(|d| d.message == "unused variable");
    assert!(stored.is_some());
    let stored = stored.unwrap();
    assert_eq!(stored.level, "warning");
    assert_eq!(stored.code, Some("dead_code".to_string()));
    assert_eq!(stored.package, Some("sinex-primitives".to_string()));

    // Clean up
    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_diagnostics_without_byte_offsets_are_queryable() -> xtask::sandbox::TestResult<()> {
    let db_path = temp_db_path();

    let db = HistoryDb::open(&db_path)?;

    // Start an invocation
    let inv_id = db.start_invocation("check", None, None, None)?;

    // Record a diagnostic with MachineApplicable but no byte offsets
    let diag = CompilerDiagnostic {
        level: "warning".to_string(),
        code: Some("unused_imports".to_string()),
        message: "unused import".to_string(),
        file_path: Some("src/main.rs".to_string()),
        line: Some(10),
        column: Some(1),
        rendered: None,
        suggestion: None,
        package: Some("sinex-db".to_string()),
        fix_replacement: Some(String::new()),
        fix_applicability: Some("MachineApplicable".to_string()),
        fix_byte_start: None, // Missing byte offsets
        fix_byte_end: None,
    };

    db.record_diagnostic(inv_id, &diag)?;

    // Record compiled packages for the invocation
    let mut packages = std::collections::HashSet::new();
    packages.insert("sinex-db".to_string());
    db.record_compiled_packages(inv_id, &packages)?;

    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 2.0)?;

    // Query with fixable_only=true (use get_current_diagnostics which requires packages)
    let fixable = db.get_current_diagnostics(None, None, None, None, true)?;

    // Should find the diagnostic despite missing byte offsets
    assert!(!fixable.is_empty());
    let found = fixable.iter().find(|d| d.message == "unused import");
    assert!(found.is_some());
    assert_eq!(
        found.unwrap().fix_applicability,
        Some("MachineApplicable".to_string())
    );

    // Clean up
    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_stage_recording_roundtrip() -> xtask::sandbox::TestResult<()> {
    let db_path = temp_db_path();

    let db = HistoryDb::open(&db_path)?;

    // Start an invocation
    let inv_id = db.start_invocation("check", None, None, None)?;

    // Record two stages
    db.record_stage_timing(inv_id, "preflight", "2026-01-01T00:00:00Z", 0.3, true)?;

    db.record_stage_timing(inv_id, "clippy", "2026-01-01T00:00:05Z", 18.5, true)?;

    // Finish the invocation
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 19.0)?;

    // Retrieve stage timings
    let timings = db.get_stage_timings_for_invocation(inv_id)?;

    // Verify both stages appear with correct data
    assert_eq!(timings.len(), 2);

    let preflight = timings.iter().find(|t| t.stage_name == "preflight");
    assert!(preflight.is_some());
    let preflight = preflight.unwrap();
    assert_eq!(preflight.duration_secs, 0.3);
    assert!(preflight.success);

    let clippy = timings.iter().find(|t| t.stage_name == "clippy");
    assert!(clippy.is_some());
    let clippy = clippy.unwrap();
    assert_eq!(clippy.duration_secs, 18.5);
    assert!(clippy.success);

    // Clean up
    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_diagnostic_delta_new() -> xtask::sandbox::TestResult<()> {
    let db_path = temp_db_path();

    let db = HistoryDb::open(&db_path)?;

    // First invocation with one diagnostic
    let inv1_id = db.start_invocation("check", None, None, None)?;

    let diag_a = CompilerDiagnostic {
        level: "error".to_string(),
        code: Some("E0425".to_string()),
        message: "cannot find value `x` in this scope".to_string(),
        file_path: Some("src/lib.rs".to_string()),
        line: Some(5),
        column: Some(10),
        rendered: None,
        suggestion: None,
        package: Some("sinex-primitives".to_string()),
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
    };

    db.record_diagnostic(inv1_id, &diag_a)?;
    db.finish_invocation(inv1_id, InvocationStatus::Failed, Some(1), 5.0)?;

    // Second invocation with both diagnostic A and a new diagnostic B
    let inv2_id = db.start_invocation("check", None, None, None)?;

    let diag_b = CompilerDiagnostic {
        level: "warning".to_string(),
        code: Some("W0001".to_string()),
        message: "new warning appeared".to_string(),
        file_path: Some("src/lib.rs".to_string()),
        line: Some(10),
        column: Some(5),
        rendered: None,
        suggestion: None,
        package: Some("sinex-primitives".to_string()),
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
    };

    db.record_diagnostic(inv2_id, &diag_a)?;
    db.record_diagnostic(inv2_id, &diag_b)?;
    db.finish_invocation(inv2_id, InvocationStatus::Success, Some(0), 4.5)?;

    // Compute delta from inv1 to inv2
    let delta = db.get_diagnostic_delta(inv1_id, inv2_id)?;

    // Verify that diagnostic B appears as "new"
    assert!(!delta.new.is_empty());
    let new_diag = delta
        .new
        .iter()
        .find(|d| d.message == "new warning appeared");
    assert!(new_diag.is_some());

    // Verify no diagnostics were "resolved" (A persisted)
    assert!(delta.resolved.is_empty());

    // Clean up
    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_live_stage_roundtrip() -> xtask::sandbox::TestResult<()> {
    let db_path = temp_db_path();

    let db = HistoryDb::open(&db_path)?;

    // Start an invocation
    let inv_id = db.start_invocation("check", None, None, None)?;

    // Set a live stage
    db.set_live_stage(inv_id, "clippy")?;
    let stage = db.get_live_stage(inv_id)?;
    assert_eq!(stage, Some("clippy".to_string()));

    // Set a different live stage
    db.set_live_stage(inv_id, "fmt")?;
    let stage = db.get_live_stage(inv_id)?;
    assert_eq!(stage, Some("fmt".to_string()));

    // Finish the invocation
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

    // Clean up
    cleanup_db(&db_path);
    Ok(())
}

// ============================================================================
// History Query Invariant Tests
// ============================================================================

#[sinex_test]
async fn test_diagnostic_trend_is_chronological() -> xtask::sandbox::TestResult<()> {
    // get_diagnostic_trend() returns results in chronological order (oldest first)
    // via ORDER BY started_at DESC + reverse(). This test verifies the .reverse() post-processing.
    let db_path = temp_db_path();
    let db = HistoryDb::open(&db_path)?;

    // Insert 5 check invocations sequentially.
    // Timestamp::now() uses OffsetDateTime::now_utc() (nanosecond precision) formatted as
    // RFC3339 with subsecond digits. Consecutive calls are monotonically increasing on Linux,
    // so no artificial sleep is needed to guarantee distinct, ordered started_at strings.
    for i in 0..5 {
        let inv_id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(
            inv_id,
            InvocationStatus::Success,
            Some(0),
            f64::from(i) + 0.1,
        )?;
    }

    let trend = db.get_diagnostic_trend(10)?;
    assert!(trend.len() >= 2, "should have at least 2 trend points");

    // Verify chronological order (oldest first — ascending started_at)
    for window in trend.windows(2) {
        assert!(
            window[0].started_at <= window[1].started_at,
            "trend not chronological: {} > {}",
            window[0].started_at,
            window[1].started_at
        );
    }

    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_diagnostic_delta_resolved_detection() -> xtask::sandbox::TestResult<()> {
    // Complements test_diagnostic_delta_new: tests the resolved half.
    // inv1 has diag A; inv2 does NOT — delta.resolved should contain A.
    let db_path = temp_db_path();
    let db = HistoryDb::open(&db_path)?;

    let diag_a = make_diag("diag-to-be-resolved", "sinex-primitives");

    // Invocation 1: diag A present
    let inv1_id = db.start_invocation("check", None, None, None)?;
    db.record_diagnostic(inv1_id, &diag_a)?;
    db.finish_invocation(inv1_id, InvocationStatus::Failed, Some(1), 3.0)?;

    // Invocation 2: diag A absent (fixed!)
    let inv2_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv2_id, InvocationStatus::Success, Some(0), 2.0)?;

    let delta = db.get_diagnostic_delta(inv1_id, inv2_id)?;

    assert!(delta.new.is_empty(), "no new diagnostics should appear");
    let resolved = delta
        .resolved
        .iter()
        .find(|d| d.message == "diag-to-be-resolved");
    assert!(
        resolved.is_some(),
        "diag A should appear as resolved when absent in inv2"
    );

    cleanup_db(&db_path);
    Ok(())
}

#[sinex_test]
async fn test_get_slowest_tests_excludes_failed() -> xtask::sandbox::TestResult<()> {
    // Failed tests must not inflate slowest-test averages.
    // A "fail" result with 60s duration (timeout ceiling) should be excluded;
    // only the "pass" result at 1s should count toward the average.
    let db_path = temp_db_path();
    let db = HistoryDb::open(&db_path)?;

    // Pass run: 1s — should count toward average
    let inv_pass = db.start_invocation("test", None, None, None)?;
    db.record_test_result(
        inv_pass,
        "test_target",
        "my-crate",
        "pass",
        1.0,
        None,
        "nextest",
    )?;
    db.finish_invocation(inv_pass, InvocationStatus::Success, Some(0), 1.0)?;

    // Fail run: 60s (simulates a timeout ceiling) — should be excluded from average
    let inv_fail = db.start_invocation("test", None, None, None)?;
    db.record_test_result(
        inv_fail,
        "test_target",
        "my-crate",
        "fail",
        60.0,
        None,
        "nextest",
    )?;
    db.finish_invocation(inv_fail, InvocationStatus::Failed, Some(1), 60.0)?;

    let slowest = db.get_slowest_tests(5)?;
    assert!(!slowest.is_empty(), "should find at least one test");

    let slowest_test = &slowest[0];
    assert_eq!(slowest_test.test_name, "test_target");
    assert!(
        slowest_test.avg_duration_secs < 5.0,
        "avg duration should be ≈1s (pass only), not inflated by fail at 60s; got {}",
        slowest_test.avg_duration_secs
    );

    cleanup_db(&db_path);
    Ok(())
}
