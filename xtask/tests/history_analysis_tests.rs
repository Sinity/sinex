//! Mathematical unit tests for `HistoryAnalysis` heuristics.
//!
//! These tests seed exact known data into a temp `SQLite` DB and assert exact
//! arithmetic: precise pass rates, diagnostic counts, and regression detection.
//!
//! Each test is self-contained: creates a throwaway DB, seeds rows, calls
//! `HistoryAnalysis`, asserts exact values. No Postgres, no NATS, no infra.

use color_eyre::eyre::Result;
use std::collections::HashSet;
use tempfile::tempdir;
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
fn seed_invocation(
    db: &HistoryDb,
    command: &str,
    status: InvocationStatus,
    duration: f64,
) -> Result<i64> {
    let id = db.start_invocation(command, None, None, None)?;
    db.finish_invocation(id, status, Some(0), duration)?;
    Ok(id)
}

fn seed_invocation_with_scope(
    db: &HistoryDb,
    command: &str,
    subcommand: Option<&str>,
    args: &[&str],
    status: InvocationStatus,
    duration: f64,
) -> Result<i64> {
    let args_json = serde_json::to_string(args)?;
    let id = db.start_invocation(command, subcommand, None, Some(&args_json))?;
    db.finish_invocation(id, status, Some(0), duration)?;
    Ok(id)
}

/// Record test results for an invocation: `pass_count` passes then `fail_count` failures.
fn seed_tests(
    db: &HistoryDb,
    inv_id: i64,
    package: &str,
    pass_count: usize,
    fail_count: usize,
) -> Result<()> {
    for i in 0..pass_count {
        db.record_test_result(
            inv_id,
            &format!("{package}::pass_{i}"),
            package,
            "pass",
            0.1,
            None,
            "nextest",
        )?;
    }
    for i in 0..fail_count {
        db.record_test_result(
            inv_id,
            &format!("{package}::fail_{i}"),
            package,
            "fail",
            0.2,
            Some("assertion failed"),
            "nextest",
        )?;
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
        fix_byte_start: fixable.then_some(100u32),
        fix_byte_end: fixable.then_some(110u32),
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
    db.record_diagnostic(
        good_inv,
        &make_diag("warning", "sinex-primitives", false, "w1"),
    )?;

    let analysis = HistoryAnalysis::new(&db);
    let since = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let regressions = analysis.regression_scan(since)?;

    // Both errors should appear as regressions in sinex-db
    assert_eq!(
        regressions.len(),
        2,
        "expected 2 regressions (one per error diag)"
    );
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

    assert!(
        regressions.is_empty(),
        "clean history should produce no regressions"
    );
    Ok(())
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Seed a check invocation with specific diagnostic counts on a package.
/// Also records `invocation_packages` so the "latest per package" CTE can find it.
fn seed_check_with_diagnostics(
    db: &HistoryDb,
    package: &str,
    errors: usize,
    warnings: usize,
    fixable: usize,
    duration: f64,
) -> Result<i64> {
    let inv = seed_invocation(db, "check", InvocationStatus::Failed, duration)?;
    db.record_compiled_packages(inv, &HashSet::from([package.to_string()]))?;
    for i in 0..errors {
        db.record_diagnostic(inv, &make_diag("error", package, false, &format!("e{i}")))?;
    }
    for i in 0..warnings {
        let is_fixable = i < fixable;
        db.record_diagnostic(
            inv,
            &make_diag("warning", package, is_fixable, &format!("w{i}")),
        )?;
    }
    Ok(inv)
}

// ─── workspace_health_report: build_score formula ────────────────────────────

/// Formula: build_score = clamp(100 - errors*10 - warnings/5, 0, 100)
/// 5 errors + 20 warnings → 100 - 50 - 4 = 46
#[sinex_test]
async fn test_build_score_with_5_errors_20_warnings() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_with_diagnostics(&db, "sinex-primitives", 5, 20, 0, 10.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let report = analysis.workspace_health_report()?;
    assert_eq!(
        report.build_score, 46,
        "build_score: 100 - 5*10 - 20/5 = 46"
    );
    assert_eq!(report.error_count, 5);
    assert_eq!(report.warning_count, 20);
    Ok(())
}

/// Zero diagnostics → build_score = 100
#[sinex_test]
async fn test_build_score_zero_diagnostics_is_100() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let analysis = HistoryAnalysis::new(&db);
    let report = analysis.workspace_health_report()?;
    assert_eq!(
        report.build_score, 100,
        "zero diagnostics → build_score 100"
    );
    Ok(())
}

/// 10 errors → 100 - 100 - 0 = 0 (clamped, not negative)
#[sinex_test]
async fn test_build_score_clamped_to_zero_on_many_errors() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_with_diagnostics(&db, "sinex-db", 10, 0, 0, 8.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let report = analysis.workspace_health_report()?;
    assert_eq!(report.build_score, 0, "10 errors clamps build_score to 0");
    Ok(())
}

/// Full composite score: 5 errors + 20 warnings, no tests, no velocity.
/// build=46, test=75, velocity=75 → score = 46*0.5 + 75*0.3 + 75*0.2 = 23+22.5+15 = 60.5 → 61
#[sinex_test]
async fn test_composite_score_formula() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_with_diagnostics(&db, "sinex-primitives", 5, 20, 0, 10.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let report = analysis.workspace_health_report()?;
    assert_eq!(report.build_score, 46);
    assert_eq!(report.test_score, 75, "no test data → default 75");
    assert_eq!(
        report.velocity_score, 75,
        "insufficient invocations → default 75"
    );
    assert_eq!(
        report.score, 61,
        "46*0.5 + 75*0.3 + 75*0.2 = 60.5 → rounds to 61"
    );
    Ok(())
}

/// Clean workspace → perfect scores → score = 100
#[sinex_test]
async fn test_composite_score_clean_workspace_is_100() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let analysis = HistoryAnalysis::new(&db);
    let report = analysis.workspace_health_report()?;
    // No data: build=100, test=75, velocity=75 → 100*0.5 + 75*0.3 + 75*0.2 = 50+22.5+15 = 87.5 → 88
    assert_eq!(report.build_score, 100);
    assert_eq!(report.score, 88, "100*0.5 + 75*0.3 + 75*0.2 = 87.5 → 88");
    Ok(())
}

// ─── velocity_trends: delta_pct and trend label ───────────────────────────────

/// Seed N successful check invocations. Higher N → higher ID → DESC-first (more "recent").
fn seed_check_invocations(db: &HistoryDb, n: usize, duration: f64) -> Result<()> {
    for _ in 0..n {
        seed_invocation(db, "check", InvocationStatus::Success, duration)?;
    }
    Ok(())
}

/// Insert 4 at 20s (older by ID) then 4 at 10s (newer by ID).
/// DESC order → [10,10,10,10,20,20,20,20], mid=4.
/// delta_pct = (10-20)/20*100 = -50 → "faster"
#[sinex_test]
async fn test_velocity_trend_detects_faster_builds() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_invocations(&db, 4, 20.0)?; // older by insertion order
    seed_check_invocations(&db, 4, 10.0)?; // newer by insertion order → first in DESC
    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend present");
    let delta = check
        .delta_pct
        .expect("delta_pct should be present with 8 data points");
    assert!(
        (delta - (-50.0)).abs() < 1.0,
        "expected delta_pct ≈ -50, got {delta}"
    );
    assert_eq!(check.trend, "faster");
    Ok(())
}

/// Insert 4 at 10s (older) then 4 at 20s (newer by ID → first in DESC).
/// delta_pct = (20-10)/10*100 = 100 → "slower"
#[sinex_test]
async fn test_velocity_trend_detects_slower_builds() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_invocations(&db, 4, 10.0)?; // older by insertion order
    seed_check_invocations(&db, 4, 20.0)?; // newer → first in DESC
    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend present");
    let delta = check.delta_pct.expect("delta_pct should be present");
    assert!(
        (delta - 100.0).abs() < 1.0,
        "expected delta_pct ≈ 100, got {delta}"
    );
    assert_eq!(check.trend, "slower");
    Ok(())
}

/// All same duration → delta_pct = 0 → "stable"
#[sinex_test]
async fn test_velocity_trend_stable_when_constant() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_invocations(&db, 8, 15.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend");
    assert_eq!(check.trend, "stable");
    let delta = check.delta_pct.expect("delta_pct present");
    assert!((delta).abs() < 0.01, "expected delta_pct ≈ 0, got {delta}");
    Ok(())
}

/// < 4 invocations → trend is "no_data", delta_pct is None
#[sinex_test]
async fn test_velocity_trend_no_data_with_few_invocations() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_invocations(&db, 3, 10.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend");
    assert_eq!(check.trend, "no_data");
    assert!(
        check.delta_pct.is_none(),
        "no delta_pct with insufficient data"
    );
    Ok(())
}

/// Velocity trends must not mix incomparable scopes such as workspace checks and `-p` checks.
#[sinex_test]
async fn test_velocity_trend_uses_most_recent_comparable_scope() -> ::xtask::sandbox::TestResult<()>
{
    let (_dir, db) = temp_db()?;

    for _ in 0..4 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["--all"],
            InvocationStatus::Success,
            30.0,
        )?;
    }
    for _ in 0..4 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["--all"],
            InvocationStatus::Success,
            30.0,
        )?;
    }

    for _ in 0..4 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["-p", "sinex-db"],
            InvocationStatus::Success,
            20.0,
        )?;
    }
    for _ in 0..4 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["-p", "sinex-db"],
            InvocationStatus::Success,
            10.0,
        )?;
    }

    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend present");

    assert_eq!(check.scope_label.as_deref(), Some("-p sinex-db"));
    let delta = check.delta_pct.expect("delta_pct should be present");
    assert!(
        (delta - (-50.0)).abs() < 1.0,
        "expected delta_pct ≈ -50 for the package-scoped cluster, got {delta}"
    );
    Ok(())
}

/// Irrelevant execution flags must not split one workload into separate trend scopes.
#[sinex_test]
async fn test_velocity_trend_ignores_non_scope_flags_in_scope_identity()
-> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;

    for _ in 0..2 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["-p", "sinex-db"],
            InvocationStatus::Success,
            20.0,
        )?;
    }
    for _ in 0..2 {
        seed_invocation_with_scope(
            &db,
            "check",
            None,
            &["--json", "--bg", "-p", "sinex-db", "--lint"],
            InvocationStatus::Success,
            10.0,
        )?;
    }

    let analysis = HistoryAnalysis::new(&db);
    let trends = analysis.velocity_trends()?;
    let check = trends
        .iter()
        .find(|t| t.command == "check")
        .expect("check trend present");

    assert_eq!(check.scope_label.as_deref(), Some("-p sinex-db"));
    assert_eq!(check.sample_count, 4);
    let delta = check.delta_pct.expect("delta_pct should be present");
    assert!(
        (delta - (-50.0)).abs() < 1.0,
        "expected delta_pct ≈ -50 when irrelevant flags are normalized, got {delta}"
    );
    Ok(())
}

// ─── package_reliability ─────────────────────────────────────────────────────

/// When all test runs are within 7d (and thus also within 30d), 7d=30d → "stable"
///
/// Note: `package_reliability` uses `get_known_packages()` which reads from
/// `build_diagnostics`. We must seed at least one diagnostic to make the package
/// appear. The test invocations are "recent" so 7d rate == 30d rate → "stable".
#[sinex_test]
async fn test_reliability_stable_when_rates_identical() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    // Register package in build_diagnostics so get_known_packages() finds it
    seed_check_with_diagnostics(&db, "sinex-node-sdk", 0, 1, 0, 2.0)?;
    // 8 pass + 2 fail for "sinex-node-sdk" (80% pass rate, same in both windows)
    let inv = seed_invocation(&db, "test", InvocationStatus::Failed, 5.0)?;
    seed_tests(&db, inv, "sinex-node-sdk", 8, 2)?;

    let analysis = HistoryAnalysis::new(&db);
    let reliability = analysis.package_reliability(10)?;
    let pkg = reliability
        .iter()
        .find(|r| r.package == "sinex-node-sdk")
        .expect("sinex-node-sdk should appear (registered via build_diagnostics)");
    assert_eq!(
        pkg.trend, "stable",
        "same data in 7d and 30d windows → stable"
    );
    assert!(
        (pkg.pass_rate - 0.8).abs() < f64::EPSILON,
        "pass rate {}",
        pkg.pass_rate
    );
    Ok(())
}

// ─── recommendations ─────────────────────────────────────────────────────────

/// Errors → critical "build" recommendation.
#[sinex_test]
async fn test_recommendations_critical_on_errors() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    seed_check_with_diagnostics(&db, "sinex-primitives", 2, 0, 0, 10.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let recs = analysis.recommendations()?;
    let critical = recs
        .iter()
        .find(|r| r.severity == "critical" && r.category == "build");
    assert!(
        critical.is_some(),
        "should emit critical build recommendation on errors"
    );
    assert_eq!(critical.unwrap().action, "xtask check --lint");
    Ok(())
}

/// Fixable diagnostics → warning recommendation with xtask fix --smart.
#[sinex_test]
async fn test_recommendations_warning_on_fixable() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    // 0 errors, 3 warnings of which 2 are fixable
    seed_check_with_diagnostics(&db, "sinex-db", 0, 3, 2, 5.0)?;
    let analysis = HistoryAnalysis::new(&db);
    let recs = analysis.recommendations()?;
    let warn = recs
        .iter()
        .find(|r| r.severity == "warning" && r.action == "xtask fix --smart");
    assert!(
        warn.is_some(),
        "should emit fix --smart warning on fixable diagnostics"
    );
    Ok(())
}

/// Clean workspace → no recommendations at all.
#[sinex_test]
async fn test_recommendations_empty_on_clean_workspace() -> ::xtask::sandbox::TestResult<()> {
    let (_dir, db) = temp_db()?;
    let analysis = HistoryAnalysis::new(&db);
    let recs = analysis.recommendations()?;
    // Without errors, fixable diagnostics, or passing test packages, no recommendations
    let critical_or_warning: Vec<_> = recs
        .iter()
        .filter(|r| {
            r.severity == "critical" || (r.severity == "warning" && r.action == "xtask fix --smart")
        })
        .collect();
    assert!(
        critical_or_warning.is_empty(),
        "clean workspace should emit no critical/fix recommendations"
    );
    Ok(())
}

#[sinex_test]
async fn test_package_reliability_surfaces_flaky_query_failures() -> ::xtask::sandbox::TestResult<()>
{
    let (dir, db) = temp_db()?;
    seed_check_with_diagnostics(&db, "sinex-node-sdk", 0, 1, 0, 2.0)?;
    let conn = rusqlite::Connection::open(dir.path().join("test.db"))?;
    conn.execute("DROP TABLE test_results", rusqlite::params![])?;

    let analysis = HistoryAnalysis::new(&db);
    let error = analysis
        .package_reliability(10)
        .expect_err("flaky test query failure should surface");
    assert!(format!("{error:#}").contains("test_results"));
    Ok(())
}

#[sinex_test]
async fn test_recommendations_surface_flaky_query_failures() -> ::xtask::sandbox::TestResult<()> {
    let (dir, db) = temp_db()?;
    let conn = rusqlite::Connection::open(dir.path().join("test.db"))?;
    conn.execute("DROP TABLE test_results", rusqlite::params![])?;

    let analysis = HistoryAnalysis::new(&db);
    let error = analysis
        .recommendations()
        .expect_err("flaky test query failure should surface");
    assert!(format!("{error:#}").contains("test_results"));
    Ok(())
}

// ─── pass rate aggregation ────────────────────────────────────────────────────

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
