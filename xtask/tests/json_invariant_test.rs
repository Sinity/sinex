//! JSON output invariant tests.
//!
//! Verifies that commands accepting `--json` always produce valid JSON on
//! stdout — including on error paths (bad subcommand, invalid flag values).
//! This catches error paths that bypass the JSON formatter and bleed clap's
//! human-readable prose into machine-parseable output streams.
//!
//! Tests assert behavioral invariants visible to users:
//! "Any xtask --json invocation emits valid JSON with status/errors fields"
//! is a JSON-consumer contract, not an implementation detail.

use std::process::Command;

use color_eyre::eyre::eyre;
use serde_json::Value;
use xtask::sandbox::sinex_test;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Run `xtask <args>` and parse stdout as JSON. Returns `(json, exit_ok)`.
fn run_json(args: &[&str]) -> color_eyre::eyre::Result<(Value, bool)> {
    let output = Command::new("xtask").args(args).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout)
        .map_err(|e| eyre!("stdout is not valid JSON: {e}\nstdout: {stdout}\nargs: {args:?}"))?;
    Ok((json, output.status.success()))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Invariant: `xtask --json bad-command` emits JSON with `status: "error"`,
/// not clap's human error prose.
///
/// This is the critical regression guard: if `get_matches()` is accidentally
/// reverted to bypass `try_get_matches()`, this test immediately fails because
/// the output won't be parseable as JSON.
#[sinex_test]
async fn json_flag_on_unrecognized_subcommand() -> ::xtask::sandbox::TestResult<()> {
    let (json, exit_ok) = run_json(&["--json", "completely-nonexistent-subcommand"])?;

    assert!(!exit_ok, "bad subcommand must exit non-zero");
    assert_eq!(json["status"], "error", "status must be 'error'");
    assert!(
        json["errors"].is_array(),
        "errors field must be an array; got: {json}"
    );
    let errors = json["errors"].as_array().unwrap();
    assert!(!errors.is_empty(), "errors array must not be empty");
    assert!(
        errors[0]["message"].is_string(),
        "each error must have a message string"
    );

    Ok(())
}

/// Invariant: valid commands always emit JSON with a `status` field and
/// `command` field matching the invoked subcommand.
#[sinex_test]
async fn json_envelope_shape_for_valid_commands() -> ::xtask::sandbox::TestResult<()> {
    // Use `xtask check --help` routed through --json isn't practical.
    // Instead verify the status command which is fast and always available.
    let (json, _exit_ok) = run_json(&["status", "--summary", "--json"])?;

    assert!(json["command"].is_string(), "envelope must have command field");
    assert_eq!(json["command"], "status", "command must match invoked command");
    assert!(json["status"].is_string(), "envelope must have status field");
    assert!(
        json["duration_secs"].is_number(),
        "envelope must have duration_secs"
    );
    assert!(json["data"].is_object(), "envelope must have data object");

    Ok(())
}

/// Invariant: `xtask --json` without subcommand emits JSON error, not clap
/// prose. This exercises the "no subcommand provided" branch.
#[sinex_test]
async fn json_flag_without_subcommand_emits_json() -> ::xtask::sandbox::TestResult<()> {
    // xtask with no subcommand succeeds (shows status/help by default) OR
    // fails — either way the output must be valid JSON when --json is passed.
    let output = Command::new("xtask").args(["--json"]).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);

    // The critical assertion: stdout must be parseable as JSON.
    let json: Value = serde_json::from_str(&stdout).map_err(|e| {
        eyre!("xtask --json (no subcommand) stdout is not valid JSON: {e}\nstdout: {stdout}")
    })?;

    // Shape: must have status field at minimum.
    assert!(
        json["status"].is_string(),
        "JSON output must have status field; got: {json}"
    );

    Ok(())
}
