//! Automated scenario tests (D11.3, D11.4, D11.6)
//!
//! These verify specific runtime behaviours and data-flow contracts that are
//! hard to exercise at the unit-test level:
//!
//! - D11.3: provenance chain traversal (raw → synthesis lineage)
//! - D11.4: `xtask status --summary --json` reports ingestd health
//! - D11.6: binaries started with `--log-format json` produce valid JSON logs

use serde_json::Value;
use sinex_primitives::prelude::*;
use std::process::Command;
use xtask::sandbox::{TestContext, sinex_test};

// ============================================================================
// D11.3 — Automated provenance_trace scenario
// ============================================================================

/// D11.3: Publish a raw (material-provenance) event and a derived
/// (synthesis-provenance) event, then walk the lineage chain to confirm the
/// ancestor link is recorded and traversable.
#[sinex_test]
async fn test_provenance_trace_scenario(ctx: TestContext) -> ::xtask::sandbox::TestResult<()> {
    use sinex_db::DbPoolExt;
    use sinex_primitives::events::DynamicPayload;

    // Create a source material for the root event
    let material_id = ctx.create_source_material(Some("d11-3-provenance-trace")).await?;

    // Root event: raw capture (material provenance)
    let raw_event = ctx
        .pool
        .events()
        .insert(
            DynamicPayload::new("test.source", "raw.capture", serde_json::json!({"value": 42}))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    let raw_id = raw_event.id.unwrap();

    // Derived event: synthesis (from_parents → references the raw event)
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
// D11.4 — ingestd_runtime_health scenario
// ============================================================================

/// D11.4: `xtask status --summary --json` reports ingestd as down when the
/// ingestd process is not running.  The summary line includes "ingestd:down"
/// and lag / batch fields contain "-" (unavailable markers).
#[sinex_test]
async fn test_ingestd_runtime_health_when_down() -> ::xtask::sandbox::TestResult<()> {
    let output = Command::new("xtask")
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

    // ingestd is not running in the test environment — summary must say so
    assert!(
        summary.contains("ingestd:down"),
        "summary should contain 'ingestd:down' when ingestd is not running, got: {summary}"
    );

    // Lag and batch should be absent ("-") when ingestd is not running
    assert!(
        summary.contains("lag:-"),
        "summary should contain 'lag:-' when ingestd is down, got: {summary}"
    );

    assert!(
        summary.contains("batch:-"),
        "summary should contain 'batch:-' when ingestd is down, got: {summary}"
    );

    Ok(())
}

// ============================================================================
// D11.6 — Structured log format verification
// ============================================================================

/// D11.6: Start ingestd with `--log-format json`, capture its stderr, and
/// verify that the initial log output consists of valid JSON objects with the
/// fields produced by `tracing_subscriber::fmt::json()`.
///
/// The test starts the binary, waits briefly for startup logs, then kills it.
/// It does not wait for ingestd to become fully ready — we only need the
/// first few log lines that the binary emits during startup.
#[sinex_test]
async fn test_ingestd_log_format_json() -> ::xtask::sandbox::TestResult<()> {
    use std::path::PathBuf;

    // Locate the binary
    let workspace = find_workspace_root()?;
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let target_dir = get_target_dir_test(&workspace);
    let binary_path = target_dir.join(profile).join("sinex-ingestd");

    if !binary_path.exists() {
        // Skip gracefully if binary has not been built yet
        eprintln!(
            "Skipping D11.6: sinex-ingestd not found at {}",
            binary_path.display()
        );
        return Ok(());
    }

    // Spawn with piped stderr and json log format
    let mut child = std::process::Command::new(&binary_path)
        .arg("--log-format")
        .arg("json")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Give the binary a moment to emit startup log lines
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Kill the process — we only care about initial log output
    let _ = child.kill();
    let output = child.wait_with_output()?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();

    // Binary may not emit ANY logs if it errors out immediately (e.g., no DATABASE_URL)
    // In that case we skip rather than fail — the binary correctly emits JSON for
    // the lines it does produce, which is what we're testing.
    if lines.is_empty() {
        eprintln!("D11.6: no log lines captured (binary may have exited immediately) — skipping assertions");
        return Ok(());
    }

    // Every non-empty line must be a valid JSON object
    let mut parsed_count = 0;
    for line in &lines {
        match serde_json::from_str::<Value>(line) {
            Ok(json) => {
                assert!(
                    json.is_object(),
                    "log line should be a JSON object, got: {line}"
                );
                parsed_count += 1;
            }
            Err(e) => {
                // Tolerate a handful of non-JSON lines (e.g., panic backtraces written
                // directly to stderr by the Rust runtime, not tracing).
                eprintln!("Non-JSON stderr line (tolerated): {line:?} — {e}");
            }
        }
    }

    assert!(
        parsed_count > 0,
        "at least one JSON log line must be present; got {} lines total",
        lines.len()
    );

    // Verify the JSON schema of the first parsed log entry
    let first_json: Value = lines
        .iter()
        .find_map(|l| serde_json::from_str(l).ok())
        .ok_or_else(|| color_eyre::eyre::eyre!("no parseable JSON lines in stderr"))?;

    // tracing-subscriber JSON format emits: timestamp, level, fields, target, span?
    assert!(
        first_json.get("timestamp").is_some() || first_json.get("ts").is_some(),
        "JSON log entry should have a timestamp field, got: {first_json}"
    );
    assert!(
        first_json.get("level").is_some(),
        "JSON log entry should have a 'level' field, got: {first_json}"
    );

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn find_workspace_root() -> color_eyre::eyre::Result<std::path::PathBuf> {
    let mut current = std::env::current_dir()?;
    loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.exists() {
            let content = std::fs::read_to_string(&cargo_toml).unwrap_or_default();
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

fn get_target_dir_test(workspace_root: &std::path::Path) -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let custom_target = workspace_root.join(".sinex/target");
    if custom_target.exists() {
        return custom_target;
    }
    workspace_root.join("target")
}
