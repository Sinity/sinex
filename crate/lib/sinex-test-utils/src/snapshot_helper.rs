//! Snapshot testing helpers using insta

use crate::database_pool::get_pool_stats;
use crate::{TestContext, TestContextFailureSnapshot};
use chrono::Utc;
use futures::Future;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

/// Helper for advanced snapshot testing with custom redactions
pub struct SnapshotTestHelper {
    settings: insta::Settings,
}

impl SnapshotTestHelper {
    /// Create a new snapshot helper with default settings
    pub fn new() -> Self {
        let mut settings = insta::Settings::clone_current();
        settings.set_snapshot_path("../snapshots");
        Self { settings }
    }

    /// Add common redactions for event fields
    pub fn with_redactions(mut self) -> Self {
        self.settings.add_redaction(".id", "[id]");
        self.settings.add_redaction(".ts_ingest", "[timestamp]");
        self.settings.add_redaction(".ts_orig", "[timestamp]");
        self.settings.add_redaction(".host", "[hostname]");
        self
    }

    /// Add a custom redaction
    pub fn add_redaction(mut self, selector: &str, replacement: &str) -> Self {
        self.settings.add_redaction(selector, replacement);
        self
    }

    /// Create a snapshot with the configured settings
    pub fn snapshot<T: Serialize>(&self, value: &T, name: &str) {
        self.settings.bind(|| {
            insta::assert_json_snapshot!(name, value);
        });
    }
}

impl Default for SnapshotTestHelper {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize)]
struct FailureSnapshot {
    test: String,
    error: String,
    timestamp: String,
    pool: crate::database_pool::PoolStats,
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
    Borrowed(&'a TestContext),
    Snapshot(TestContextFailureSnapshot),
}

/// Persist contextual information about a failing test. Artifacts are written to
/// `target/test-artifacts/` by default and can be overridden via the
/// `SINEX_TEST_FAIL_DIR` environment variable.
pub fn persist_failure(test_name: &str, error: impl Into<String>, ctx: FailureContext<'_>) {
    let snapshot_dir = env::var("SINEX_TEST_FAIL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/test-artifacts"));

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
                baseline_events: snapshot.baseline_events(),
                elapsed_ms: snapshot.elapsed_ms(),
                background_pending: snapshot.background_snapshot().pending,
                background_labels: snapshot.background_snapshot().labels,
            }),
            Some(snapshot.captured_logs()),
        ),
    };

    let slot_detail: Option<Vec<SlotSnapshot>> = {
        let slots = crate::database_pool::get_slot_stats();
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
        timestamp: Utc::now().to_rfc3339(),
        pool: get_pool_stats(),
        pool_detail: slot_detail,
        context: ctx_snapshot,
        logs,
    };

    let sanitized = test_name.replace("::", "_");
    let filename = format!(
        "{}-{}.json",
        Utc::now().format("%Y%m%dT%H%M%S%3f"),
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
    ctx: &crate::TestContext,
    f: F,
) -> crate::TestResult<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = crate::TestResult<()>>,
{
    match f().await {
        Ok(()) => Ok(()),
        Err(err) => {
            persist_failure(test_name, err.to_string(), FailureContext::Borrowed(ctx));
            // Best-effort recovery before retrying.
            let _ = ctx.force_cleanup().await;
            let _ = crate::db_common::reset_database(ctx.pool()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            f().await
        }
    }
}
