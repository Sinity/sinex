use crate::database_pool::{self, PoolStats};
use crate::test_context::ContextTelemetry;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::Serialize;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tracing::warn;

static LOG_ALL_RUNS: Lazy<bool> = Lazy::new(|| read_bool_flag("SINEX_TEST_LOG_ALL"));
static METRICS_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static METRICS_PATH: Lazy<PathBuf> = Lazy::new(resolve_metrics_path);

fn read_bool_flag(key: &str) -> bool {
    match env::var(key) {
        Ok(value) => {
            let trimmed = value.trim();
            !(trimmed.is_empty() || trimmed.eq_ignore_ascii_case("false") || trimmed == "0")
        }
        Err(_) => false,
    }
}

fn resolve_metrics_path() -> PathBuf {
    if let Ok(path) = env::var("SINEX_TEST_METRICS_PATH") {
        return PathBuf::from(path);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .unwrap_or(manifest_dir);

    let target_dir = env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root.join("target"));

    target_dir.join("sinex-test-metrics.jsonl")
}

#[derive(Serialize)]
struct ContextDetails {
    database: String,
    baseline_events: i64,
    created_events: usize,
    event_delta: Option<i64>,
    nats_enabled: bool,
    nats_url: Option<String>,
    environment: String,
    logs: Vec<String>,
}

/// Public handle that allows macro-generated code to pass telemetry around
/// without exposing the internal `ContextTelemetry` type.
pub struct TelemetryHandle(ContextTelemetry);

impl From<ContextTelemetry> for TelemetryHandle {
    fn from(value: ContextTelemetry) -> Self {
        Self(value)
    }
}

impl AsRef<ContextTelemetry> for TelemetryHandle {
    fn as_ref(&self) -> &ContextTelemetry {
        &self.0
    }
}

#[derive(Serialize)]
struct TestRunSummary {
    test: String,
    status: &'static str,
    error: Option<String>,
    duration_ms: u128,
    timestamp: DateTime<Utc>,
    pool: PoolStats,
    context: Option<ContextDetails>,
}

impl TestRunSummary {
    fn new(
        test: &str,
        duration: Duration,
        outcome: &color_eyre::eyre::Result<()>,
        context: Option<ContextDetails>,
    ) -> Self {
        let status = if outcome.is_ok() { "pass" } else { "fail" };
        let error = outcome
            .as_ref()
            .err()
            .map(|err| format!("{err:?}"))
            .map(|mut message| {
                if message.len() > 32_768 {
                    message.truncate(32_768);
                    message.push_str("…");
                }
                message
            });

        Self {
            test: test.to_string(),
            status,
            error,
            duration_ms: duration.as_millis(),
            timestamp: Utc::now(),
            pool: database_pool::get_pool_stats(),
            context,
        }
    }
}

#[derive(Serialize)]
struct MetricsRecord<'a> {
    test: &'a str,
    status: &'static str,
    duration_ms: u128,
    timestamp: DateTime<Utc>,
    pool_total_acquisitions: usize,
    pool_average_wait_time_ms: u64,
    pool_cleanup_failures: usize,
    pool_template_recreations: usize,
    context_created_events: Option<usize>,
    context_event_delta: Option<i64>,
    context_baseline_events: Option<i64>,
    context_database: Option<&'a str>,
    context_environment: Option<&'a str>,
    context_nats_enabled: Option<bool>,
}

impl<'a> From<&'a TestRunSummary> for MetricsRecord<'a> {
    fn from(summary: &'a TestRunSummary) -> Self {
        let pool = &summary.pool;
        let ctx = summary.context.as_ref();
        MetricsRecord {
            test: &summary.test,
            status: summary.status,
            duration_ms: summary.duration_ms,
            timestamp: summary.timestamp,
            pool_total_acquisitions: pool.total_acquisitions,
            pool_average_wait_time_ms: pool.average_wait_time_ms,
            pool_cleanup_failures: pool.cleanup_failures,
            pool_template_recreations: pool.template_recreations,
            context_created_events: ctx.map(|c| c.created_events),
            context_event_delta: ctx.and_then(|c| c.event_delta),
            context_baseline_events: ctx.map(|c| c.baseline_events),
            context_database: ctx.map(|c| c.database.as_str()),
            context_environment: ctx.map(|c| c.environment.as_str()),
            context_nats_enabled: ctx.map(|c| c.nats_enabled),
        }
    }
}

async fn build_context_details(telemetry: &ContextTelemetry, include_logs: bool) -> ContextDetails {
    let event_delta = match telemetry.event_delta().await {
        Ok(delta) => Some(delta),
        Err(err) => {
            warn!(
                "Failed to compute event delta for {}: {}",
                telemetry.test_name(),
                err
            );
            None
        }
    };

    ContextDetails {
        database: telemetry.db_name().to_string(),
        baseline_events: telemetry.baseline_events(),
        created_events: telemetry.created_event_count(),
        event_delta,
        nats_enabled: telemetry.nats_enabled(),
        nats_url: telemetry.nats_url(),
        environment: telemetry.environment().to_string(),
        logs: if include_logs {
            telemetry.logs_snapshot()
        } else {
            Vec::new()
        },
    }
}

fn emit_summary(summary: &TestRunSummary) {
    match serde_json::to_string_pretty(summary) {
        Ok(payload) => eprintln!("📋 test run summary:\n{}", payload),
        Err(err) => warn!("Failed to serialize test run summary: {}", err),
    }
}

fn append_metrics(summary: &TestRunSummary) -> std::io::Result<()> {
    let record: MetricsRecord<'_> = summary.into();

    let guard = METRICS_LOCK.lock();
    if let Some(parent) = METRICS_PATH.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&*METRICS_PATH)?;

    serde_json::to_writer(&mut file, &record)?;
    file.write_all(b"\n")?;
    drop(guard);
    Ok(())
}

pub async fn record_async(
    test_name: &str,
    elapsed: Duration,
    telemetry: Option<&TelemetryHandle>,
    outcome: &color_eyre::eyre::Result<()>,
) {
    let should_emit = outcome.is_err() || *LOG_ALL_RUNS;
    let context = if let Some(handle) = telemetry {
        Some(build_context_details(handle.as_ref(), should_emit).await)
    } else {
        None
    };

    let summary = TestRunSummary::new(test_name, elapsed, outcome, context);

    if should_emit {
        emit_summary(&summary);
    }

    if let Err(err) = append_metrics(&summary) {
        warn!("Failed to append metrics for {}: {}", test_name, err);
    }
}

pub fn record_sync(test_name: &str, elapsed: Duration, outcome: &color_eyre::eyre::Result<()>) {
    let should_emit = outcome.is_err() || *LOG_ALL_RUNS;

    let summary = TestRunSummary::new(test_name, elapsed, outcome, None);

    if should_emit {
        emit_summary(&summary);
    }

    if let Err(err) = append_metrics(&summary) {
        warn!("Failed to append metrics for {}: {}", test_name, err);
    }
}
