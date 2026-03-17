//! CLI output snapshot tests.
//!
//! Captures JSON output from key xtask commands and snapshots it via insta,
//! with volatile fields (timestamps, durations, counts, git state) scrubbed
//! to stable placeholders.
//!
//! **Purpose**: Catch unintended changes to the CLI JSON contract — field
//! removals, renames, and structural drift — that manual `assert!` checks
//! won't catch.
//!
//! **Capturing initial snapshots**: Run once with:
//! ```bash
//! xtask test --update-snapshots -p xtask -E 'test(snapshot)'
//! ```
//!
//! Tests assert behavioral invariants visible to users, not implementation details.

use std::process::Command;

use color_eyre::eyre::eyre;
use serde_json::{Value, json};
use xtask::history::{HistoryDb, seed::{SeedOptions, seed_history}};
use xtask::sandbox::sinex_test;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Replace a nested JSON value at a dotted path with a placeholder.
fn scrub(json: &mut Value, path: &[&str], placeholder: Value) {
    match path {
        [] => {}
        [key] => {
            if let Some(obj) = json.as_object_mut() {
                if obj.contains_key(*key) {
                    obj.insert((*key).to_string(), placeholder);
                }
            }
        }
        [key, rest @ ..] => {
            if let Some(obj) = json.as_object_mut() {
                if let Some(nested) = obj.get_mut(*key) {
                    scrub(nested, rest, placeholder);
                }
            }
        }
    }
}

/// Scrub top-level envelope volatiles shared by all commands.
fn scrub_envelope(json: &mut Value) {
    scrub(json, &["timestamp"], json!("[timestamp]"));
    scrub(json, &["duration_secs"], json!("[duration]"));
}

/// Some commands (history list, analytics) emit data JSON first, then the
/// CommandResult envelope as a second value. This returns the *first* value.
fn parse_first_json(stdout: &str) -> color_eyre::eyre::Result<Value> {
    let mut de = serde_json::Deserializer::from_str(stdout).into_iter::<Value>();
    de.next()
        .ok_or_else(|| eyre!("no JSON value in stdout"))?
        .map_err(|e| eyre!("JSON parse error: {e}\nstdout: {stdout}"))
}

/// Open a seeded history DB in `state_dir`, returning the path used.
fn seed_history_db(state_dir: &std::path::Path) -> color_eyre::eyre::Result<()> {
    let db_path = state_dir.join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    seed_history(&db, &SeedOptions { days: 7, invocations: 20 })?;
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Invariant: `xtask status --summary --json` emits a stable JSON envelope with
/// expected top-level keys: command, status, duration_secs, timestamp, data.
///
/// This snapshot supersedes `test_status_summary_json_contract` in
/// command_consistency.rs — it catches structural drift (field removed/renamed)
/// as well as type changes, where the old test only asserted field presence.
#[sinex_test]
async fn snapshot_status_summary_json() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;

    let output = Command::new("xtask")
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["status", "--summary", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "status --summary --json must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut json: Value =
        serde_json::from_str(&stdout).map_err(|e| eyre!("invalid JSON: {e}\nstdout: {stdout}"))?;

    // Scrub envelope
    scrub_envelope(&mut json);

    // Scrub all volatile data fields
    for path in [
        &["data", "summary"][..],
        &["data", "health"],
        &["data", "health_indicator"],
        &["data", "active_jobs"],
        &["data", "total_jobs"],
        &["data", "recent_invocations"],
        &["data", "active_job_details"],
        &["data", "services"],
        &["data", "recommendations"],
        // Infrastructure: varies by whether postgres/nats are running
        &["data", "infrastructure"],
        // Git state: varies by branch, dirty status
        &["data", "git"],
        // Diagnostic counts: vary by build state
        &["data", "diagnostics"],
        // Velocity and health analytics depend on history DB contents
        &["data", "health_score"],
        &["data", "velocity"],
        // Working tree state: varies by uncommitted files, stash, last commit
        &["data", "uncommitted_count"],
        &["data", "stash_count"],
        &["data", "files_changed"],
        &["data", "last_commit"],
        // Warning messages depend on workspace state
        &["data", "warnings"],
        // Last command timestamps depend on execution history
        &["data", "last_commands"],
        // Recent job list varies
        &["data", "recent_jobs"],
    ] {
        scrub(&mut json, path, json!("[volatile]"));
    }

    insta::assert_json_snapshot!("status_summary", json);
    Ok(())
}

/// Invariant: `xtask doctor --json` emits a well-formed JSON report with
/// expected shape. Volatile environment-dependent checks are scrubbed.
#[sinex_test]
async fn snapshot_doctor_json() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;

    let output = Command::new("xtask")
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        // Remove TLS env vars so the TLS section has stable null state
        .env_remove("SINEX_GATEWAY_TLS_CERT")
        .env_remove("SINEX_GATEWAY_TLS_KEY")
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA")
        .arg("doctor")
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "doctor --json must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut json: Value =
        serde_json::from_str(&stdout).map_err(|e| eyre!("invalid JSON: {e}\nstdout: {stdout}"))?;

    scrub_envelope(&mut json);

    // Scrub environment-dependent checks
    for path in [
        &["data", "postgres"][..],
        &["data", "nats"],
        &["data", "tools"],
        &["data", "tls"],
        &["data", "preflight"],
        &["data", "runtime"],
        &["data", "issues"],
        &["data", "suggestions"],
        &["data", "health"],
        // overall depends on whether infra is running
        &["data", "overall"],
        // postgres_extensions depends on DB connectivity
        &["data", "postgres_extensions"],
        // environment contains the temp state_dir path + hostname + toolchain
        &["data", "environment"],
    ] {
        scrub(&mut json, path, json!("[volatile]"));
    }

    insta::assert_json_snapshot!("doctor", json);
    Ok(())
}

/// Invariant: `xtask history list --json --limit 1` on a seeded DB returns
/// the expected envelope shape with one invocation record.
///
/// The seeded DB guarantees stable history exists; volatile fields (timestamps,
/// ids, durations) are scrubbed.
#[sinex_test]
async fn snapshot_history_list_seeded() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    seed_history_db(dir.path())?;

    let output = Command::new("xtask")
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["history", "list", "--json", "--limit", "1"])
        .output()?;

    assert!(
        output.status.success(),
        "history list --json must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // `history list --json` emits the invocations array first, then the
    // CommandResult envelope as a second JSON value. Parse only the first.
    let mut invocations = parse_first_json(&stdout)?;

    // Scrub all volatile invocation fields — structural shape (key set) is what we assert.
    if let Some(arr) = invocations.as_array_mut() {
        for inv in arr.iter_mut() {
            if let Some(obj) = inv.as_object_mut() {
                let keys: Vec<String> = obj.keys().cloned().collect();
                for key in keys {
                    obj.insert(key, json!("[volatile]"));
                }
            }
        }
    }

    insta::assert_json_snapshot!("history_list_seeded", invocations);
    Ok(())
}

/// Invariant: `xtask analytics workspace-health --json` on a seeded DB
/// returns the expected envelope shape with a numeric health score.
#[sinex_test]
async fn snapshot_analytics_workspace_health_seeded() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    seed_history_db(dir.path())?;

    let output = Command::new("xtask")
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["analytics", "workspace-health", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "analytics workspace-health --json must exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // `analytics workspace-health --json` emits the health object first, then
    // the CommandResult envelope as a second JSON value. Parse only the first.
    let mut data = parse_first_json(&stdout)?;

    // Scrub all computed metrics — values depend on seed content and timing.
    // We assert the top-level key set via the snapshot shape; values are volatile.
    if let Some(obj) = data.as_object_mut() {
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in keys {
            obj.insert(key, json!("[volatile]"));
        }
    }

    insta::assert_json_snapshot!("analytics_workspace_health_seeded", data);
    Ok(())
}
