//! Automated scenario tests (D11.3, D11.4, D11.6)
//!
//! These verify specific runtime behaviours and data-flow contracts that are
//! hard to exercise at the unit-test level:
//!
//! - D11.3: provenance chain traversal (raw → derived lineage)
//! - D11.4: `xtask status --summary --json` reports event_engine health
//! - D11.6: binaries started with `--log-format json` produce valid JSON logs

mod support;

use serde_json::Value;
use sinex_primitives::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use support::xtask_command;
use xtask::sandbox::sinex_test;

// ============================================================================
// D11.3 — Automated provenance_trace scenario
// ============================================================================

/// D11.3: Publish a raw (material-provenance) event and a derived
/// (derived-provenance) event, then walk the lineage chain to confirm the
/// ancestor link is recorded and traversable.
#[sinex_test]
async fn test_provenance_trace_scenario(ctx: TestContext) -> ::xtask::sandbox::TestResult<()> {
    use sinex_db::DbPoolExt;
    use sinex_primitives::events::DynamicPayload;

    // Create a source material for the root event
    let material_id = ctx
        .create_source_material(Some("d11-3-provenance-trace"))
        .await?;

    // Root event: raw capture (material provenance)
    let raw_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "test.source",
                "raw.capture",
                serde_json::json!({"value": 42}),
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;

    let raw_id = raw_event.id.unwrap();

    // Derived event: derived (from_parents → references the raw event)
    let derived_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new(
                "test.source",
                "derived.summary",
                serde_json::json!({"source_value": 42, "computed": true}),
            )
            .from_parents(vec![raw_id])?
            .build()?,
        )
        .await?;

    let derived_id = derived_event.id.unwrap();

    // Query ancestors of the derived event
    let lineage = ctx
        .pool
        .events()
        .lineage(LineageQuery {
            event_id: derived_id,
            direction: LineageDirection::Ancestors,
            max_depth: u32::MAX,
        })
        .await?;

    // Root of the result is the derived event itself
    assert_eq!(
        lineage.root.id,
        Some(derived_id),
        "lineage root should be the queried event"
    );

    // Exactly one ancestor: the raw event
    assert_eq!(
        lineage.ancestors.len(),
        1,
        "derived event should have exactly one ancestor"
    );

    assert_eq!(
        lineage.ancestors[0].event.id,
        Some(raw_id),
        "the ancestor should be the raw event"
    );

    // Confirm the raw event has no ancestors itself (it's a material root)
    let raw_lineage = ctx
        .pool
        .events()
        .lineage(LineageQuery {
            event_id: raw_id,
            direction: LineageDirection::Ancestors,
            max_depth: u32::MAX,
        })
        .await?;

    assert_eq!(
        raw_lineage.ancestors.len(),
        0,
        "raw (material-provenance) event should have no ancestors"
    );

    Ok(())
}

// ============================================================================
// D11.4 — event_engine_runtime_health scenario
// ============================================================================

/// D11.4: `xtask status --summary --json` reports event_engine as non-healthy when
/// the checkout-local event_engine process is not running. The summary line should
/// surface either a missing heartbeat (`event_engine:down`) or a stale heartbeat
/// (`event_engine:stale`), and lag / batch fields should remain unavailable.
#[sinex_test]
async fn test_event_engine_runtime_health_when_down() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let output = xtask_command()?
        .env("SINEX_STATE_DIR", dir.path())
        .env("NO_COLOR", "1")
        .args(["status", "--summary", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "xtask status --summary --json should succeed"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout)
        .map_err(|e| color_eyre::eyre::eyre!("invalid JSON from status: {e}"))?;

    let summary = json["data"]["summary"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.summary missing or not a string"))?;

    // event_engine is not running in the test environment. Depending on whether the
    // checkout-local runtime database still contains an old heartbeat row,
    // status may surface the service as down or stale.
    assert!(
        summary.contains("event_engine:down") || summary.contains("event_engine:stale"),
        "summary should contain 'event_engine:down' or 'event_engine:stale' when event_engine is not running, got: {summary}"
    );

    // Lag and batch should be absent ("-") when event_engine is not healthy
    assert!(
        summary.contains("lag:-"),
        "summary should contain 'lag:-' when event_engine is not healthy, got: {summary}"
    );

    assert!(
        summary.contains("batch:-"),
        "summary should contain 'batch:-' when event_engine is not healthy, got: {summary}"
    );

    Ok(())
}

// ============================================================================
// D11.6 — Structured log format verification
// ============================================================================

/// D11.6: Start event_engine with `--log-format json`, capture its stderr, and
/// verify that the initial log output consists of valid JSON objects with the
/// fields produced by `tracing_subscriber::fmt::json()`.
///
/// The test starts the binary, waits until it emits something or exits, then
/// kills it. It does not wait for event_engine to become fully ready — we only need
/// the first few log lines that the binary emits during startup.
#[sinex_test]
async fn test_event_engine_log_format_json() -> ::xtask::sandbox::TestResult<()> {
    // Locate the binary
    let workspace = find_workspace_root()?;
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let binary_path = ensure_sinexd_binary(&workspace, profile)?;

    // Spawn with piped stderr and json log format
    let mut child = std::process::Command::new(&binary_path)
        .arg("--log-format")
        .arg("json")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| color_eyre::eyre::eyre!("child stderr pipe was not available"))?;
    let (line_tx, line_rx) = mpsc::channel();
    let stderr_reader = std::thread::spawn(move || -> std::io::Result<Vec<String>> {
        let mut captured = Vec::new();
        for line in BufReader::new(stderr).lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let _ = line_tx.send(());
                captured.push(line);
            }
        }
        Ok(captured)
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match line_rx.recv_timeout(Duration::from_millis(25)) {
            Ok(()) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if child.try_wait()?.is_some() {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    // Kill the process — we only care about initial log output
    let _ = child.kill();
    let _ = child.wait();

    let stderr_lines = stderr_reader
        .join()
        .map_err(|_| color_eyre::eyre::eyre!("stderr reader thread panicked"))??;
    assert_json_log_lines(&binary_path, &stderr_lines)?;

    Ok(())
}

fn assert_json_log_lines(binary_path: &Path, lines: &[String]) -> color_eyre::eyre::Result<()> {
    if lines.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "D11.6 captured no stderr log lines from {}; JSON-log behavior was not exercised",
            binary_path.display()
        ));
    }

    for (index, line) in lines.iter().enumerate() {
        let json: Value = serde_json::from_str(line).map_err(|error| {
            color_eyre::eyre::eyre!(
                "D11.6 stderr line {index} from {} was not valid JSON: {error}; line={line:?}",
                binary_path.display()
            )
        })?;

        if !json.is_object() {
            return Err(color_eyre::eyre::eyre!(
                "D11.6 stderr line {index} from {} should be a JSON object, got: {line}",
                binary_path.display()
            ));
        }

        if json.get("timestamp").is_none() && json.get("ts").is_none() {
            return Err(color_eyre::eyre::eyre!(
                "D11.6 stderr line {index} from {} should include timestamp or ts, got: {json}",
                binary_path.display()
            ));
        }

        if json.get("level").is_none() {
            return Err(color_eyre::eyre::eyre!(
                "D11.6 stderr line {index} from {} should include level, got: {json}",
                binary_path.display()
            ));
        }
    }

    Ok(())
}

#[test]
fn test_json_log_validation_rejects_missing_log_evidence() {
    let error = assert_json_log_lines(Path::new("sinexd"), &[]).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("JSON-log behavior was not exercised"),
        "missing runtime log evidence should be reported explicitly, got: {error}"
    );
}

#[test]
fn test_json_log_validation_rejects_mixed_non_json_stderr() {
    let lines = vec![
        r#"{"timestamp":"2026-06-21T00:00:00Z","level":"INFO","fields":{"message":"ready"}}"#
            .to_string(),
        "panic: wrote directly to stderr".to_string(),
    ];
    let error = assert_json_log_lines(Path::new("sinexd"), &lines).unwrap_err();

    assert!(
        error.to_string().contains("was not valid JSON"),
        "non-JSON stderr should fail instead of being tolerated, got: {error}"
    );
}

#[test]
fn test_json_log_validation_accepts_json_log_objects() -> color_eyre::eyre::Result<()> {
    let lines = vec![
        r#"{"timestamp":"2026-06-21T00:00:00Z","level":"INFO","fields":{"message":"ready"}}"#
            .to_string(),
    ];

    assert_json_log_lines(Path::new("sinexd"), &lines)
}

// ============================================================================
// Helpers
// ============================================================================

fn ensure_sinexd_binary(workspace: &Path, profile: &str) -> color_eyre::eyre::Result<PathBuf> {
    let target_dir = get_target_dir_test(workspace);
    let binary_path = target_dir.join(profile).join("sinexd");
    if binary_path.exists() {
        return Ok(binary_path);
    }

    let mut command = xtask::process::cargo_command();
    command
        .current_dir(workspace)
        .args(["build", "-p", "sinexd", "--bin", "sinexd"]);
    if profile == "release" {
        command.arg("--release");
    }

    let status = command.status().map_err(|error| {
        color_eyre::eyre::eyre!(
            "failed to invoke managed cargo build for D11.6 sinexd binary: {error}"
        )
    })?;
    if !status.success() {
        return Err(color_eyre::eyre::eyre!(
            "D11.6 could not build sinexd binary with status {status}"
        ));
    }

    if !binary_path.exists() {
        return Err(color_eyre::eyre::eyre!(
            "managed cargo build completed but D11.6 sinexd binary is missing at {}",
            binary_path.display()
        ));
    }
    Ok(binary_path)
}

fn find_workspace_root() -> color_eyre::eyre::Result<PathBuf> {
    let mut current = std::env::current_dir()?;
    loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.exists() {
            let content = std::fs::read_to_string(&cargo_toml)?;
            if content.contains("[workspace]") {
                return Ok(current);
            }
        }
        if !current.pop() {
            return Err(color_eyre::eyre::eyre!(
                "Could not find workspace root (Cargo.toml with [workspace])"
            ));
        }
    }
}

fn get_target_dir_test(workspace_root: &Path) -> PathBuf {
    xtask::workspace_target_dir_for(workspace_root)
}
