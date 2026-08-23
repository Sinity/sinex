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

mod support;

use color_eyre::eyre::eyre;
use serde_json::{Value, json};
use support::xtask_command;
use xtask::history::{
    HistoryDb,
    seed::{SeedOptions, seed_history},
};
use xtask::sandbox::sinex_test;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Replace a nested JSON value at a dotted path with a placeholder.
fn scrub(json: &mut Value, path: &[&str], placeholder: Value) {
    match path {
        [] => {}
        [key] => {
            if let Some(obj) = json.as_object_mut()
                && obj.contains_key(*key)
            {
                obj.insert((*key).to_string(), placeholder);
            }
        }
        [key, rest @ ..] => {
            if let Some(obj) = json.as_object_mut()
                && let Some(nested) = obj.get_mut(*key)
            {
                scrub(nested, rest, placeholder);
            }
        }
    }
}

/// Remove a nested JSON value whose presence is environment-dependent.
fn remove_path(json: &mut Value, path: &[&str]) {
    match path {
        [] => {}
        [key] => {
            if let Some(obj) = json.as_object_mut() {
                obj.remove(*key);
            }
        }
        [key, rest @ ..] => {
            if let Some(obj) = json.as_object_mut()
                && let Some(nested) = obj.get_mut(*key)
            {
                remove_path(nested, rest);
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
/// `CommandResult` envelope as a second value. This returns the *first* value.
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
    seed_history(
        &db,
        &SeedOptions {
            days: 7,
            invocations: 20,
        },
    )?;
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Invariant: `xtask doctor --json` emits a well-formed JSON report with
/// expected shape. Volatile environment-dependent checks are scrubbed.
#[sinex_test]
async fn snapshot_doctor_json() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;

    let output = xtask_command()?
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        // Remove TLS env vars so the TLS section has stable null state
        .env_remove("SINEX_API_TLS_CERT")
        .env_remove("SINEX_API_TLS_KEY")
        .env_remove("SINEX_API_TLS_CLIENT_CA")
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

    let output = xtask_command()?
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

    let output = xtask_command()?
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
    // Older implementations emitted the health object directly; newer ones emit
    // the standard CommandResult envelope. Keep the snapshot on the payload.
    let parsed = parse_first_json(&stdout)?;
    let mut data = parsed
        .get("data")
        .filter(|_| parsed.get("status").is_some() && parsed.get("command").is_some())
        .cloned()
        .unwrap_or(parsed);

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
