//! Mathematical unit tests for `HistoryAnalysis` heuristics.
//!
//! Phase 4.2: these tests seed exact known data into a temp SQLite DB and
//! assert exact arithmetic — not JSON shape, not "greater than zero", but
//! precise pass rates, diagnostic counts, and regression detection.
//!
//! Each test is self-contained: creates a throwaway DB, seeds rows, calls
//! `HistoryAnalysis`, asserts exact values. No Postgres, no NATS, no infra.

use color_eyre::eyre::Result;
use std::collections::HashSet;
use tempfile::tempdir;
use time;
use xtask::cargo_diagnostics::CompilerDiagnostic;
use xtask::history::{HistoryAnalysis, HistoryDb, InvocationStatus};
use xtask::sandbox::sinex_test;

// ─── Helpers ────────────────────────────────────────────────────────────────

fn temp_db() -> Result<(tempfile::TempDir, HistoryDb)> {
    let dir = tempdir()?;
    let path = dir.path().join("test.db");
    let db = HistoryDb::open(&path)?;
    Ok((dir, db))
}

/// Seed a completed invocation and return its ID.
fn seed_invocation(db: &HistoryDb, command: &str, status: InvocationStatus, duration: f64) -> Result<i64> {
    let id = db.start_invocation(command, None, None, None)?;
    db.finish_invocation(id, status, Some(0), duration)?;
    Ok(id)
}

/// Record test results for an invocation: `pass_count` passes then `fail_count` failures.
fn seed_tests(db: &HistoryDb, inv_id: i64, package: &str, pass_count: usize, fail_count: usize) -> Result<()> {
    for i in 0..pass_count {
        db.record_test_result(inv_id, &format!("{package}::pass_{i}"), package, "pass", 0.1, None, "nextest")?;
    }
    for i in 0..fail_count {
        db.record_test_result(inv_id, &format!("{package}::fail_{i}"), package, "fail", 0.2, Some("assertion failed"), "nextest")?;
    }
    Ok(())
}

/// Build a minimal `CompilerDiagnostic` for seeding.
/// Use a unique `tag` to ensure each diagnostic has a distinct message.
fn make_diag(level: &str, package: &str, fixable: bool, tag: &str) -> CompilerDiagnostic {
    CompilerDiagnostic {
        level: level.to_string(),
        code: Some("E0308".to_string()),
        message: format!("{level} in {package}: {tag}"),
        file_path: Some(format!("src/{package}/lib.rs")),
        line: Some(10),
        column: Some(4),
        rendered: None,
        suggestion: None,
        package: Some(package.to_string()),
        fix_replacement: fixable.then(|| "fixed_value".to_string()),
        fix_applicability: fixable.then(|| "MachineApplicable".to_string()),
        fix_byte_start: fixable.then(|| 100u32),
        fix_byte_end: fixable.then(|| 110u32),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Exact pass-rate arithmetic: 6 passes + 2 failures → 0.75.
#[sinex_test]
async fn test_pass_rate_exact_arithmetic() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let inv = seed_invocation(&db, "test", InvocationStatus::Failed, 5.0)?;
    seed_tests(&db, inv, "sinex-primitives", 6, 2)?;

    let analysis = HistoryAnalysis::new(&db);
    let health = analysis.package_health("sinex-primitives")?;

    let rate = health.test_pass_rate.expect("should have a pass rate");
    // 6 / (6+2) = 0.75 exactly
    assert!(
        (rate - 0.75).abs() < f64::EPSILON,
        "expected pass rate 0.75, got {rate}"
    );
    Ok(())
}

/// Pass rate is None when there are zero recorded tests.
#[sinex_test]
async fn test_pass_rate_none_when_no_tests() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    // No tests recorded — rate should be None, not 0.0 or NaN
    let analysis = HistoryAnalysis::new(&db);
    let health = analysis.package_health("sinex-db")?;
    assert!(
        health.test_pass_rate.is_none(),
        "pass rate should be None with zero tests, got {:?}",
        health.test_pass_rate
    );
    Ok(())
}

/// 100% pass rate when all tests pass.
#[sinex_test]
async fn test_pass_rate_full_green() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let inv = seed_invocation(&db, "test", InvocationStatus::Success, 3.0)?;
    seed_tests(&db, inv, "sinex-schema", 10, 0)?;

    let analysis = HistoryAnalysis::new(&db);
    let health = analysis.package_health("sinex-schema")?;

    let rate = health.test_pass_rate.expect("should have pass rate");
    assert!(
        (rate - 1.0).abs() < f64::EPSILON,
        "expected 1.0, got {rate}"
    );
    Ok(())
}

/// Diagnostic count reflects seeded errors accurately.
#[sinex_test]
async fn test_diagnostic_count_exact() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let inv = seed_invocation(&db, "check", InvocationStatus::Failed, 10.0)?;
    db.record_compiled_packages(inv, &HashSet::from(["sinex-primitives".to_string()]))?;

    // 3 errors + 1 fixable warning
    db.record_diagnostic(inv, &make_diag("error", "sinex-primitives", false, "e1"))?;
    db.record_diagnostic(inv, &make_diag("error", "sinex-primitives", false, "e2"))?;
    db.record_diagnostic(inv, &make_diag("error", "sinex-primitives", false, "e3"))?;
    db.record_diagnostic(inv, &make_diag("warning", "sinex-primitives", true, "w1"))?;

    let analysis = HistoryAnalysis::new(&db);
    let health = analysis.package_health("sinex-primitives")?;

    assert_eq!(health.diagnostic_count, 4, "total diagnostic count");
    assert_eq!(health.fixable_count, 1, "fixable diagnostic count");
    Ok(())
}

/// Regression scan detects exactly the packages with errors in failed invocations.
#[sinex_test]
async fn test_regression_scan_finds_failures() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;

    // One failed invocation with 2 error diagnostics in sinex-db
    let failed_inv = seed_invocation(&db, "check", InvocationStatus::Failed, 8.0)?;
    db.record_compiled_packages(failed_inv, &HashSet::from(["sinex-db".to_string()]))?;
    db.record_diagnostic(failed_inv, &make_diag("error", "sinex-db", false, "r1"))?;
    db.record_diagnostic(failed_inv, &make_diag("error", "sinex-db", false, "r2"))?;

    // One successful invocation — should NOT appear in regressions
    let good_inv = seed_invocation(&db, "check", InvocationStatus::Success, 5.0)?;
    db.record_compiled_packages(good_inv, &HashSet::from(["sinex-primitives".to_string()]))?;
    db.record_diagnostic(good_inv, &make_diag("warning", "sinex-primitives", false, "w1"))?;

    let analysis = HistoryAnalysis::new(&db);
    let since = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let regressions = analysis.regression_scan(since)?;

    // Both errors should appear as regressions in sinex-db
    assert_eq!(regressions.len(), 2, "expected 2 regressions (one per error diag)");
    for r in &regressions {
        assert_eq!(r.package.as_deref(), Some("sinex-db"), "regression package");
        assert_eq!(r.level, "error", "regression level");
    }

    Ok(())
}

/// Regression scan returns empty when no failing invocations exist.
#[sinex_test]
async fn test_regression_scan_empty_on_clean_history() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let inv = seed_invocation(&db, "check", InvocationStatus::Success, 4.0)?;
    db.record_compiled_packages(inv, &HashSet::from(["sinex-schema".to_string()]))?;

    let analysis = HistoryAnalysis::new(&db);
    let since = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let regressions = analysis.regression_scan(since)?;

    assert!(regressions.is_empty(), "clean history should produce no regressions");
    Ok(())
}

/// pass rate aggregates across multiple invocations, not just the last one.
#[sinex_test]
async fn test_pass_rate_aggregates_across_invocations() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;

    // First run: 4 pass
    let inv1 = seed_invocation(&db, "test", InvocationStatus::Success, 2.0)?;
    seed_tests(&db, inv1, "sinex-macros", 4, 0)?;

    // Second run: 2 pass, 2 fail
    let inv2 = seed_invocation(&db, "test", InvocationStatus::Failed, 3.0)?;
    seed_tests(&db, inv2, "sinex-macros", 2, 2)?;

    let analysis = HistoryAnalysis::new(&db);
    let health = analysis.package_health("sinex-macros")?;

    // Total: 6 pass / 8 total = 0.75
    let rate = health.test_pass_rate.expect("should have pass rate");
    assert!(
        (rate - 0.75).abs() < f64::EPSILON,
        "expected aggregate 0.75, got {rate}"
    );
    Ok(())
}
