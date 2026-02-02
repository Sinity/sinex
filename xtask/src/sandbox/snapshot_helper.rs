//! Test failure diagnostics and retry helpers.
//!
//! This module provides utilities for persisting test failure diagnostics
//! and retrying flaky tests with context capture.

use crate::sandbox::db::pool::get_pool_stats;
use crate::sandbox::prelude::*;
use futures::Future;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use time::OffsetDateTime;

#[derive(Serialize)]
struct FailureSnapshot {
    test: String,
    error: String,
    timestamp: String,
    pool: crate::sandbox::db::pool::PoolStats,
    pool_detail: Option<Vec<SlotSnapshot>>,
    context: Option<ContextSnapshot>,
    logs: Option<Vec<String>>,
}

#[derive(Serialize)]
struct ContextSnapshot {
    name: String,
    baseline_events: i64,
    elapsed_ms: u128,
    background_pending: usize,
    background_labels: Vec<String>,
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

/// Persist contextual information about a failing test. Artifacts are written to
/// `target/test-artifacts/` by default and can be overridden via the
/// `SINEX_TEST_FAIL_DIR` environment variable.
pub fn persist_failure(test_name: &str, error: impl Into<String>, ctx: FailureContext<'_>) {
    let snapshot_dir = env::var("SINEX_TEST_FAIL_DIR")
        .map_or_else(|_| PathBuf::from("target/test-artifacts"), PathBuf::from);

    if let Err(err) = fs::create_dir_all(&snapshot_dir) {
        eprintln!(
            "⚠️  failed to create snapshot directory {}: {err}",
            snapshot_dir.display()
        );
        return;
    }

    let (ctx_snapshot, logs) = match ctx {
        FailureContext::None => (None, None),
        FailureContext::Borrowed(ctx) => (
            Some(ContextSnapshot {
                name: ctx.test_name().to_string(),
                baseline_events: ctx.baseline_event_count(),
                elapsed_ms: ctx.elapsed().as_millis(),
                background_pending: ctx.background_snapshot().pending,
                background_labels: ctx.background_snapshot().labels,
            }),
            Some(ctx.captured_logs()),
        ),
        FailureContext::Snapshot(snapshot) => (
            Some(ContextSnapshot {
                name: snapshot.test_name().to_string(),
                baseline_events: snapshot.baseline_event_count(),
                elapsed_ms: snapshot.elapsed_ms(),
                background_pending: snapshot.background_snapshot().pending,
                background_labels: snapshot.background_snapshot().labels,
            }),
            Some(snapshot.captured_logs()),
        ),
    };

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

    let snapshot = FailureSnapshot {
        test: test_name.to_string(),
        error: error.into(),
        timestamp: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("RFC3339 formatting of current UTC time should always succeed"),
        pool: get_pool_stats(),
        pool_detail: slot_detail,
        context: ctx_snapshot,
        logs,
    };

    let sanitized = test_name.replace("::", "_");
    let filename = format!(
        "{}-{}.json",
        OffsetDateTime::now_utc()
            .format(
                &time::format_description::parse(
                    "[year][month][day]T[hour][minute][second][subsecond digits:3]"
                )
                .unwrap()
            )
            .unwrap(),
        sanitized
    );
    let path = snapshot_dir.join(filename);

    match serde_json::to_vec_pretty(&snapshot) {
        Ok(data) => {
            if let Err(err) = fs::write(&path, data) {
                eprintln!(
                    "⚠️  failed to write failure snapshot {}: {err}",
                    path.display()
                );
            }
        }
        Err(err) => {
            eprintln!("⚠️  failed to serialize failure snapshot for {test_name}: {err}");
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
