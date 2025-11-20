//! Snapshot testing helpers using insta

use crate::database_pool::get_pool_stats;
use crate::{TestContext, TestContextFailureSnapshot};
use chrono::Utc;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::PathBuf;

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
    context: Option<ContextSnapshot>,
    logs: Option<Vec<String>>,
}

#[derive(Serialize)]
struct ContextSnapshot {
    name: String,
    baseline_events: i64,
    elapsed_ms: u128,
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
            }),
            Some(ctx.captured_logs()),
        ),
        FailureContext::Snapshot(snapshot) => (
            Some(ContextSnapshot {
                name: snapshot.test_name().to_string(),
                baseline_events: snapshot.baseline_events(),
                elapsed_ms: snapshot.elapsed_ms(),
            }),
            Some(snapshot.captured_logs()),
        ),
    };

    let snapshot = FailureSnapshot {
        test: test_name.to_string(),
        error: error.into(),
        timestamp: Utc::now().to_rfc3339(),
        pool: get_pool_stats(),
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
