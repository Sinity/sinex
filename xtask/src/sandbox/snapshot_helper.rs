//! Test failure diagnostics and retry helpers.
//!
//! This module provides utilities for persisting test failure diagnostics
//! and retrying flaky tests with context capture.

use crate::sandbox::db::pool::get_pool_stats;
use crate::sandbox::evidence;
use crate::sandbox::prelude::*;
use futures::Future;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sinex_primitives::temporal::Timestamp;
use std::fs;
use std::sync::LazyLock as Lazy;

static SNAPSHOT_FILENAME_FORMAT: Lazy<Vec<time::format_description::BorrowedFormatItem<'static>>> =
    Lazy::new(|| {
        time::format_description::parse(
            "[year][month][day]T[hour][minute][second][subsecond digits:3]",
        )
        .expect("static format string is valid")
    });

#[derive(Serialize)]
struct ContextSnapshot {
    name: String,
    baseline_events: i64,
    elapsed_ms: u128,
    background_pending: Option<usize>,
    background_labels: Vec<String>,
    background_busy: bool,
}

#[derive(Serialize)]
struct SlotSnapshot {
    name: String,
    total_connections: usize,
    idle_connections: usize,
    last_clean_time: Option<String>,
    last_clean_result: Option<String>,
    residuals: Option<Vec<(String, i64)>>,
    quarantined: bool,
}

pub enum FailureContext<'a> {
    None,
    Borrowed(&'a Sandbox),
    Snapshot(SandboxFailureSnapshot),
}

fn to_json<T: Serialize>(value: &T) -> JsonValue {
    serde_json::to_value(value).unwrap_or(JsonValue::Null)
}

fn attach_captured_logs(test_name: &str, logs: &[String], evidence: &mut TestEvidence) {
    if logs.is_empty()
        || evidence
            .captures
            .iter()
            .any(|capture| capture.kind == EvidenceCollectorKind::Logs)
    {
        return;
    }

    let summary = LogEvidenceSummary::new(logs, 20);
    let summary_text = format!("{} captured log line(s)", logs.len());
    let artifact = evidence::write_json_artifact(
        test_name,
        "captured_logs",
        "logs",
        &logs,
        Some(summary_text.clone()),
    );
    let capture = match artifact {
        Ok(artifact) => EvidenceCapture::captured(
            "captured_logs",
            EvidenceCollectorKind::Logs,
            Some(summary_text),
            to_json(&summary),
            Some(artifact),
        ),
        Err(error) => EvidenceCapture::failed(
            "captured_logs",
            EvidenceCollectorKind::Logs,
            format!("failed to persist captured logs: {error:#}"),
        ),
    };
    evidence.attach_capture(capture);
}

/// Persist contextual information about a failing test. Artifacts are written to
/// `.sinex/test-artifacts/` by default and can be overridden via the
/// `SINEX_TEST_FAIL_DIR` environment variable.
pub fn persist_failure(test_name: &str, error: impl Into<String>, ctx: FailureContext<'_>) {
    let snapshot_dir = evidence::evidence_root();

    if let Err(err) = fs::create_dir_all(&snapshot_dir) {
        eprintln!(
            "⚠️  failed to create evidence directory {}: {err}",
            snapshot_dir.display()
        );
        return;
    }

    let (ctx_snapshot, logs, mut test_evidence) = match ctx {
        FailureContext::None => (None, None, TestEvidence::default()),
        FailureContext::Borrowed(ctx) => {
            let background = ctx.background_snapshot();
            (
                Some(ContextSnapshot {
                    name: ctx.test_name().to_string(),
                    baseline_events: ctx.baseline_event_count(),
                    elapsed_ms: ctx.elapsed().as_millis(),
                    background_pending: background.pending,
                    background_labels: background.labels,
                    background_busy: background.busy,
                }),
                Some(ctx.captured_logs()),
                ctx.evidence_snapshot(),
            )
        }
        FailureContext::Snapshot(snapshot) => {
            let background = snapshot.background_snapshot();
            (
                Some(ContextSnapshot {
                    name: snapshot.test_name().to_string(),
                    baseline_events: snapshot.baseline_event_count(),
                    elapsed_ms: snapshot.elapsed_ms(),
                    background_pending: background.pending,
                    background_labels: background.labels,
                    background_busy: background.busy,
                }),
                Some(snapshot.captured_logs()),
                snapshot.evidence_snapshot(),
            )
        }
    };

    if let Some(logs) = &logs {
        attach_captured_logs(test_name, logs, &mut test_evidence);
    }

    let slot_detail: Option<Vec<SlotSnapshot>> = {
        let slots = crate::sandbox::db::pool::get_slot_stats();
        if slots.is_empty() {
            None
        } else {
            Some(
                slots
                    .into_iter()
                    .map(|s| SlotSnapshot {
                        name: s.name,
                        total_connections: s.total_connections,
                        idle_connections: s.idle_connections,
                        last_clean_time: s.last_clean_time,
                        last_clean_result: s.last_clean_result,
                        residuals: s.residuals,
                        quarantined: s.quarantined,
                    })
                    .collect(),
            )
        }
    };

    let error = error.into();
    let timestamp = Timestamp::now();
    let stem = format!(
        "{}-{}",
        Timestamp::now()
            .inner()
            .format(&*SNAPSHOT_FILENAME_FORMAT)
            .unwrap_or_else(|_| "unknown".to_string()),
        evidence::sanitize_component(test_name)
    );
    let bundle_path = snapshot_dir.join(format!("{stem}.evidence.json"));
    let summary_path = snapshot_dir.join(format!("{stem}.summary.txt"));
    let bundle = evidence::EvidenceBundle::failed(
        test_name,
        error.clone(),
        timestamp.format_rfc3339(),
        ctx_snapshot.as_ref().map_or(JsonValue::Null, to_json),
        to_json(&get_pool_stats()),
        slot_detail.as_ref().map_or(JsonValue::Null, to_json),
        EvidenceRuntimeSnapshot {
            process_id: std::process::id(),
            process_tree: evidence::current_process_tree_json(std::time::Duration::ZERO),
        },
        test_evidence,
    );
    let summary = evidence::render_human_summary(&bundle);

    match serde_json::to_vec_pretty(&bundle) {
        Ok(data) => {
            if let Err(err) = fs::write(&bundle_path, data) {
                eprintln!(
                    "⚠️  failed to write evidence bundle {}: {err}",
                    bundle_path.display()
                );
            } else {
                if let Err(err) = fs::write(&summary_path, summary) {
                    eprintln!(
                        "⚠️  failed to write evidence summary {}: {err}",
                        summary_path.display()
                    );
                }
                eprintln!(
                    "EVIDENCE: {} SUMMARY: {} ({})",
                    bundle_path.display(),
                    summary_path.display(),
                    error
                );
            }
        }
        Err(err) => {
            eprintln!("⚠️  failed to serialize evidence bundle for {test_name}: {err}");
        }
    }
}

/// Retry a fallible async block once, capturing diagnostics on the first failure.
pub async fn retry_with_snapshot<F, Fut>(
    test_name: &str,
    ctx: &Sandbox,
    f: F,
) -> crate::sandbox::prelude::TestResult<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = crate::sandbox::prelude::TestResult<()>>,
{
    match f().await {
        Ok(()) => Ok(()),
        Err(err) => {
            persist_failure(test_name, err.to_string(), FailureContext::Borrowed(ctx));
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    #[test]
    fn persist_failure_writes_evidence_bundle_and_summary() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let _guard = EnvGuard::set_single("SINEX_TEST_FAIL_DIR", dir.path().as_os_str());

        persist_failure(
            "sample::evidence_failure",
            "assertion exploded",
            FailureContext::None,
        );

        let files = fs::read_dir(dir.path())?.collect::<std::result::Result<Vec<_>, _>>()?;
        let bundle_path = files
            .iter()
            .map(std::fs::DirEntry::path)
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".evidence.json"))
            })
            .ok_or_else(|| color_eyre::eyre::eyre!("missing evidence bundle"))?;
        let summary_path = files
            .iter()
            .map(std::fs::DirEntry::path)
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".summary.txt"))
            })
            .ok_or_else(|| color_eyre::eyre::eyre!("missing evidence summary"))?;

        let bundle: JsonValue = serde_json::from_slice(&fs::read(&bundle_path)?)?;
        let summary = fs::read_to_string(summary_path)?;

        assert_eq!(bundle["schema_version"], EVIDENCE_SCHEMA_VERSION);
        assert_eq!(bundle["kind"], "sinex.test.evidence");
        assert_eq!(bundle["status"], "failed");
        assert_eq!(bundle["error"], "assertion exploded");
        assert!(summary.contains("assertion exploded"));
        Ok(())
    }
}
