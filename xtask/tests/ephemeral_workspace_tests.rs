//! Integration tests for `EphemeralWorkspace`.
//!
//! These tests spawn real `xtask check` subprocesses against a minimal ephemeral
//! workspace, verifying that diagnostics are captured and written to the history DB.
//!
//! They are marked `#[ignore]` because:
//!   1. Nextest holds the cargo target/ lock — child cargo invocations would deadlock.
//!   2. They require a real cargo toolchain and take ~30–60s each.
//!
//! Run with: `xtask test --heavy -E 'test(ephemeral_workspace)'`

use std::process::Command;

use color_eyre::eyre::Result;
use xtask::history::{HistoryDb, InvocationStatus};
use xtask::sandbox::EphemeralWorkspace;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn run_xtask_in(ws: &EphemeralWorkspace, args: &[&str]) -> std::process::Output {
    Command::new("xtask")
        .args(args)
        .current_dir(ws.dir())
        .envs(ws.env_overrides())
        // Disable color/TTY output for predictable parsing
        .env("NO_COLOR", "1")
        .env("FORCE_COLOR", "0")
        .output()
        .unwrap_or_else(|error| panic!("failed to execute xtask in {:?}: {error}", ws.dir()))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// A compile error in the ephemeral workspace causes `xtask check` to fail and
/// records a `FAILED` invocation in the history DB.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy"]
fn test_check_fails_on_compile_error_and_records_history() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;
    ws.inject_compile_error("ws-lib")?;

    let output = run_xtask_in(&ws, &["check", "--json"]);

    // xtask check should exit non-zero
    assert_ne!(
        output.status.code(),
        Some(0),
        "expected non-zero exit on compile error; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    // History DB should contain a failed invocation
    let db = HistoryDb::open(&ws.history_db_path())?;
    let invocations = db.get_recent(10, None)?;
    let check_invocations: Vec<_> = invocations
        .iter()
        .filter(|i| i.command == "check")
        .collect();
    assert!(
        !check_invocations.is_empty(),
        "expected at least one 'check' invocation in history DB"
    );
    assert_eq!(
        check_invocations[0].status,
        InvocationStatus::Failed,
        "check invocation with compile error should be recorded as Failed"
    );

    Ok(())
}

/// `xtask check --lint` on a workspace with a clippy warning records a diagnostic
/// in the history DB for the ws-lib package.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy"]
fn test_check_lint_records_clippy_warning_in_history() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;
    ws.inject_clippy_warning("ws-lib")?;

    let _output = run_xtask_in(&ws, &["check", "--lint", "--json"]);

    // May succeed (warnings don't fail check by default), but diagnostics should be stored
    let db = HistoryDb::open(&ws.history_db_path())?;
    let invocations = db.get_recent(5, None)?;
    let check_inv = invocations.iter().find(|i| i.command == "check");
    assert!(
        check_inv.is_some(),
        "expected a 'check' invocation in history"
    );

    let inv_id = check_inv
        .unwrap_or_else(|| panic!("expected a 'check' invocation in history"))
        .id;
    let diagnostics = db.get_diagnostics(inv_id)?;
    assert!(
        !diagnostics.is_empty(),
        "expected clippy warning in diagnostics for inv {inv_id}"
    );
    let warning = diagnostics
        .iter()
        .find(|d| d.level == "warning")
        .unwrap_or_else(|| panic!("expected at least one warning diagnostic"));
    assert_eq!(warning.package.as_deref(), Some("ws-lib"));

    Ok(())
}

/// A clean workspace compiles successfully and records a success invocation.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy"]
fn test_check_succeeds_on_clean_workspace() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;

    let output = run_xtask_in(&ws, &["check", "--json"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "expected success on clean workspace; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let db = HistoryDb::open(&ws.history_db_path())?;
    let invocations = db.get_recent(5, None)?;
    let check_inv = invocations.iter().find(|i| i.command == "check");
    assert!(check_inv.is_some(), "expected check invocation in history");
    assert_eq!(
        check_inv
            .unwrap_or_else(|| panic!("expected check invocation in history"))
            .status,
        InvocationStatus::Success,
        "clean workspace check should be Success"
    );

    Ok(())
}

/// A format error causes `xtask check --full` to fail.
#[test]
#[ignore = "spawns real cargo; run with xtask test --heavy"]
fn test_check_full_fails_on_format_error() -> Result<()> {
    let ws = EphemeralWorkspace::new()?;
    ws.inject_format_error("ws-lib")?;

    let output = run_xtask_in(&ws, &["check", "--full", "--json"]);

    assert_ne!(
        output.status.code(),
        Some(0),
        "expected non-zero exit on format error with --full"
    );

    Ok(())
}
