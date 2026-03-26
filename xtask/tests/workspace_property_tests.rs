//! Property-based workspace invariant tests.
//!
//! These tests use `EphemeralWorkspace` as a reliable I/O adapter (isolated
//! temp-dir workspace + cargo toolchain) and `proptest` to generate mutation
//! sequences, then verify behavioral invariants hold after every step.
//!
//! Unlike unit property tests that run 1000+ cases in milliseconds, each case
//! here spawns real cargo subprocesses (~10–60s per case). Limits are kept low
//! (cases = 3–5) to bound total runtime while still getting shrink-on-failure.
//!
//! All tests are `#[ignore]`; run with: `xtask test --heavy -E 'test(workspace_property)'`

use std::process::Command;

use color_eyre::eyre::Result;
use proptest::prelude::*;
use xtask::history::{HistoryDb, InvocationStatus};
use xtask::sandbox::EphemeralWorkspace;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn run_xtask_in(ws: &EphemeralWorkspace, args: &[&str]) -> std::process::Output {
    Command::new("xtask")
        .args(args)
        .current_dir(ws.dir())
        .envs(ws.env_overrides())
        .env("NO_COLOR", "1")
        .env("FORCE_COLOR", "0")
        .output()
        .unwrap_or_else(|error| panic!("failed to execute xtask in {:?}: {error}", ws.dir()))
}

fn check_succeeded(output: &std::process::Output) -> bool {
    output.status.code() == Some(0)
}

// ─── Invariant 1: HistoryDb consistency ──────────────────────────────────────

/// **Invariant**: After any `xtask check` invocation completes, the HistoryDb
/// contains exactly one new invocation record with `command = "check"`.
///
/// This verifies the recording contract: no matter whether check passes or
/// fails, the history DB always grows by exactly one record per run.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy -E 'test(workspace_property)'"]
fn workspace_property_historydb_consistency() -> Result<()> {
    let config = ProptestConfig {
        cases: 3,
        ..ProptestConfig::default()
    };

    proptest!(config, |(inject_error in proptest::bool::ANY)| {
        let ws = EphemeralWorkspace::new()
            .unwrap_or_else(|error| panic!("failed to create ephemeral workspace: {error}"));

        if inject_error {
            ws.inject_compile_error("ws-lib")
                .unwrap_or_else(|error| panic!("failed to inject compile error: {error}"));
        }

        let db_path = ws.history_db_path();

        // Precondition: DB doesn't exist yet (fresh workspace)
        prop_assert!(!db_path.exists(), "DB should not exist before first xtask run");

        // Run xtask check
        run_xtask_in(&ws, &["check", "--json"]);

        // Postcondition: DB exists and has exactly one check invocation
        prop_assert!(
            db_path.exists(),
            "HistoryDb must exist after xtask check completes"
        );
        let db = HistoryDb::open(&db_path)
            .unwrap_or_else(|error| panic!("failed to open history db {db_path:?}: {error}"));
        let invocations = db
            .get_recent(10, Some("check"))
            .unwrap_or_else(|error| panic!("failed to read check invocations: {error}"));
        prop_assert_eq!(
            invocations.len(),
            1,
            "expected exactly 1 check invocation, got {}",
            invocations.len()
        );
        let inv = &invocations[0];
        prop_assert_eq!(&inv.command, "check");
        prop_assert!(
            inv.status == InvocationStatus::Success || inv.status == InvocationStatus::Failed,
            "invocation must be Success or Failed, not {:?}", inv.status
        );
        // Status must match whether the check actually succeeded
        let expected_status = if inject_error {
            InvocationStatus::Failed
        } else {
            InvocationStatus::Success
        };
        prop_assert_eq!(
            inv.status,
            expected_status,
            "DB status mismatch: inject_error={} → expected {:?}",
            inject_error, expected_status
        );
    });
    Ok(())
}

// ─── Invariant 2: Fix idempotency ────────────────────────────────────────────

/// **Invariant**: `xtask fix && xtask fix` produces the same result as
/// `xtask fix` alone.
///
/// After two consecutive fix passes, the second pass must report no changes
/// (exit 0, no modifications). This verifies that the fix operations are
/// idempotent — applying them twice is equivalent to applying them once.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy -E 'test(workspace_property)'"]
fn workspace_property_fix_idempotency() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;
    ws.inject_format_error("ws-lib")?;

    // First fix: should reformat the file
    let first_fix = run_xtask_in(&ws, &["fix", "--json"]);

    // Second fix: source is already well-formatted — nothing to change
    let second_fix = run_xtask_in(&ws, &["fix", "--json"]);

    assert!(
        check_succeeded(&second_fix),
        "second fix pass must exit 0 (nothing to change); stdout: {}",
        String::from_utf8_lossy(&second_fix.stdout)
    );

    // After two fix passes, `check --full` should pass (fmt + clippy clean)
    let check_after = run_xtask_in(&ws, &["check", "--full", "--json"]);
    assert_eq!(
        check_after.status.code(),
        Some(0),
        "check --full must pass after two fix passes; stderr: {}",
        String::from_utf8_lossy(&check_after.stderr)
    );

    // Verify both fix invocations recorded in history
    let db = HistoryDb::open(&ws.history_db_path())?;
    let fix_invocations = db.get_recent(10, Some("fix"))?;
    assert_eq!(
        fix_invocations.len(),
        2,
        "expected exactly 2 fix invocations in history DB"
    );

    let _ = first_fix; // consumed
    Ok(())
}

// ─── Invariant 3: Compile error status propagation ───────────────────────────

/// **Invariant**: The HistoryDb status always matches the actual exit code.
/// A failed `xtask check` (non-zero exit) must record `Failed`.
/// A passing `xtask check` (exit 0) must record `Success`.
///
/// Verified across 5 random mutation combinations using proptest.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy -E 'test(workspace_property)'"]
fn workspace_property_status_matches_exit_code() -> Result<()> {
    let config = ProptestConfig {
        cases: 5,
        ..ProptestConfig::default()
    };

    proptest!(config, |(
        inject_compile_error in proptest::bool::ANY,
        add_extra_member in proptest::bool::ANY,
    )| {
        let ws = EphemeralWorkspace::new()
            .unwrap_or_else(|error| panic!("failed to create workspace: {error}"));

        if add_extra_member {
            ws.add_member("ws-lib-extra")
                .unwrap_or_else(|error| panic!("failed to add workspace member: {error}"));
        }
        if inject_compile_error {
            ws.inject_compile_error("ws-lib")
                .unwrap_or_else(|error| panic!("failed to inject compile error: {error}"));
        }

        let output = run_xtask_in(&ws, &["check", "--json"]);
        let exit_ok = output.status.code() == Some(0);

        let db = HistoryDb::open(&ws.history_db_path())
            .unwrap_or_else(|error| panic!("failed to open history db: {error}"));
        let invocations = db
            .get_recent(5, Some("check"))
            .unwrap_or_else(|error| panic!("failed to read check invocations: {error}"));

        prop_assert!(!invocations.is_empty(), "must have at least one check invocation");
        let latest = &invocations[0];

        if exit_ok {
            prop_assert_eq!(
                latest.status,
                InvocationStatus::Success,
                "exit 0 must record Success"
            );
        } else {
            prop_assert_eq!(
                latest.status,
                InvocationStatus::Failed,
                "non-zero exit must record Failed"
            );
        }
    });
    Ok(())
}

// ─── Invariant 4: Multiple invocations accumulate monotonically ───────────────

/// **Invariant**: After N consecutive `xtask check` runs, the HistoryDb
/// contains exactly N invocation records.
///
/// Verifies that history accumulates without data loss, deduplication, or
/// overwrite. Each run is a distinct record, even with identical outcomes.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy -E 'test(workspace_property)'"]
fn workspace_property_history_accumulates_monotonically() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;

    const RUNS: usize = 3;
    for _ in 0..RUNS {
        run_xtask_in(&ws, &["check", "--json"]);
    }

    let db = HistoryDb::open(&ws.history_db_path())?;
    let invocations = db.get_recent(RUNS + 5, Some("check"))?;
    assert_eq!(
        invocations.len(),
        RUNS,
        "history must contain exactly {RUNS} records after {RUNS} runs, got {}",
        invocations.len()
    );

    // All must be Success (clean workspace)
    for (i, inv) in invocations.iter().enumerate() {
        assert_eq!(
            inv.status,
            InvocationStatus::Success,
            "run {i} must record Success on clean workspace"
        );
    }

    Ok(())
}
