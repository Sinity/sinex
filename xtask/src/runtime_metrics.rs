//! Runtime metrics from the Sinex Postgres database.
//!
//! Provides single-shot queries against `core.node_manifests` and telemetry
//! events to surface ingestd health, consumer lag, and batch latency in
//! xtask status/doctor/run commands.

use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use std::fmt;

/// Runtime health status for ingestd
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestdStatus {
    /// Heartbeat fresh within stale threshold
    Healthy,
    /// Heartbeat older than stale threshold
    Stale,
    /// No node_manifests row or status != 'active'
    Down,
    /// Could not query Postgres
    Unknown,
}

impl fmt::Display for IngestdStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => write!(f, "ok"),
            Self::Stale => write!(f, "stale"),
            Self::Down => write!(f, "down"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Aggregated runtime metrics from Postgres
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeMetrics {
    pub ingestd_status: IngestdStatus,
    /// Seconds since last heartbeat (None if no row)
    pub last_heartbeat_age_secs: Option<i64>,
    /// Latest consumer lag (pending messages) from metric.gauge events
    pub consumer_lag_pending: Option<f64>,
    /// Latest batch processing latency from batch.stats events
    pub last_batch_latency_ms: Option<f64>,
}

impl RuntimeMetrics {
    /// Format as a compact one-line summary fragment for status --summary
    pub fn summary_fragment(&self) -> String {
        let ingestd = format!("ingestd:{}", self.ingestd_status);
        let lag = self
            .consumer_lag_pending
            .map(|v| format!("lag:{v:.0}"))
            .unwrap_or_else(|| "lag:-".to_string());
        let batch = self
            .last_batch_latency_ms
            .map(|v| format!("batch:{v:.0}ms"))
            .unwrap_or_else(|| "batch:-".to_string());
        format!("{ingestd} {lag} {batch}")
    }
}

/// Default stale threshold in seconds (matches SINEX_NODE_HEARTBEAT_STALE_SECS)
const HEARTBEAT_STALE_SECS: i64 = 120;

/// Query runtime metrics from Postgres. Returns `Unknown` status if unreachable.
pub async fn query_runtime_metrics(db_url: &str) -> RuntimeMetrics {
    match query_inner(db_url).await {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!("Runtime metrics query failed: {e}");
            RuntimeMetrics {
                ingestd_status: IngestdStatus::Unknown,
                last_heartbeat_age_secs: None,
                consumer_lag_pending: None,
                last_batch_latency_ms: None,
            }
        }
    }
}

async fn query_inner(db_url: &str) -> Result<RuntimeMetrics, sqlx::Error> {
    // Single connection, short-lived
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect(db_url)
        .await?;

    // 1. Heartbeat status from node_manifests
    let heartbeat_row = sqlx::query_as::<_, HeartbeatRow>(
        r#"
        SELECT
            status,
            EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at))::bigint AS age_secs
        FROM core.node_manifests
        WHERE node_name = 'sinex-ingestd'
        ORDER BY last_heartbeat_at DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .fetch_optional(&pool)
    .await?;

    let (ingestd_status, last_heartbeat_age_secs) = match heartbeat_row {
        Some(row) => {
            let age = row.age_secs;
            let status_str = row.status.as_deref().unwrap_or("");
            let status = if status_str != "active" {
                IngestdStatus::Down
            } else if age.is_some_and(|a| a > HEARTBEAT_STALE_SECS) {
                IngestdStatus::Stale
            } else {
                IngestdStatus::Healthy
            };
            (status, age)
        }
        None => (IngestdStatus::Down, None),
    };

    // 2. Latest consumer lag from metric.gauge events
    let lag_row = sqlx::query_scalar::<_, f64>(
        r#"
        SELECT (payload->>'value')::float8
        FROM core.events
        WHERE source = 'sinex'
          AND event_type = 'metric.gauge'
          AND payload->>'name' = 'ingestd.consumer.lag.pending'
        ORDER BY id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&pool)
    .await?;

    // 3. Latest batch latency from batch.stats events
    let batch_row = sqlx::query_scalar::<_, f64>(
        r#"
        SELECT (payload->>'fetch_to_ack_ms')::float8
        FROM core.events
        WHERE source = 'sinex.ingestd'
          AND event_type = 'batch.stats'
        ORDER BY id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&pool)
    .await?;

    pool.close().await;

    Ok(RuntimeMetrics {
        ingestd_status,
        last_heartbeat_age_secs,
        consumer_lag_pending: lag_row,
        last_batch_latency_ms: batch_row,
    })
}

#[derive(sqlx::FromRow)]
struct HeartbeatRow {
    status: Option<String>,
    age_secs: Option<i64>,
}
