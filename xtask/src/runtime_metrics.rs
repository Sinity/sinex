//! Runtime metrics from the Sinex Postgres database.
//!
//! Provides single-shot queries against runtime heartbeat state and telemetry
//! events to surface event_engine health, consumer lag, and batch latency in
//! xtask status/doctor/run commands.

use serde::Serialize;
use sinex_primitives::events::{EventEngineBatchStatsPayload, EventPayload};
use sqlx::postgres::PgPoolOptions;
use std::fmt;

pub(crate) fn event_engine_batch_stats_source() -> &'static str {
    EventEngineBatchStatsPayload::SOURCE.as_static_str()
}

/// Runtime health status for event_engine
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventEngineStatus {
    /// Heartbeat fresh within stale threshold
    Healthy,
    /// Heartbeat older than stale threshold
    Stale,
    /// No live runtime row was found
    Down,
    /// Could not query Postgres
    Unknown,
}

impl fmt::Display for EventEngineStatus {
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
    pub event_engine_status: EventEngineStatus,
    /// Seconds since last heartbeat (None if no row)
    pub last_heartbeat_age_secs: Option<i64>,
    /// Latest consumer lag (pending messages) from metric.gauge events
    pub consumer_lag_pending: Option<f64>,
    /// Age of the latest consumer lag sample in seconds
    pub consumer_lag_age_secs: Option<i64>,
    /// Latest batch processing latency from batch.stats events
    pub last_batch_latency_ms: Option<f64>,
    /// Age of the latest batch latency sample in seconds
    pub last_batch_latency_age_secs: Option<i64>,
    /// Query failure detail when runtime metrics could not be read at all.
    pub query_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeAssessment {
    pub status: RuntimeHealthStatus,
    pub warnings: Vec<String>,
}

impl RuntimeMetrics {
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            event_engine_status: EventEngineStatus::Unknown,
            last_heartbeat_age_secs: None,
            consumer_lag_pending: None,
            consumer_lag_age_secs: None,
            last_batch_latency_ms: None,
            last_batch_latency_age_secs: None,
            query_error: None,
        }
    }

    pub fn query_failure(message: impl Into<String>) -> Self {
        Self {
            query_error: Some(message.into()),
            ..Self::unavailable()
        }
    }

    #[must_use]
    pub fn fresh_consumer_lag_pending(&self) -> Option<f64> {
        self.consumer_lag_pending.filter(|_| {
            self.consumer_lag_age_secs
                .is_some_and(|age| age <= TELEMETRY_STALE_SECS)
        })
    }

    #[must_use]
    pub fn consumer_lag_is_stale(&self) -> bool {
        self.consumer_lag_pending.is_some() && self.fresh_consumer_lag_pending().is_none()
    }

    #[must_use]
    pub fn fresh_batch_latency_ms(&self) -> Option<f64> {
        self.last_batch_latency_ms.filter(|_| {
            self.last_batch_latency_age_secs
                .is_some_and(|age| age <= TELEMETRY_STALE_SECS)
        })
    }

    #[must_use]
    pub fn batch_latency_is_stale(&self) -> bool {
        self.last_batch_latency_ms.is_some() && self.fresh_batch_latency_ms().is_none()
    }

    fn describe_sample_age(age_secs: Option<i64>) -> String {
        match age_secs {
            Some(age) => format!("last sample {age}s ago"),
            None => "sample age unavailable".to_string(),
        }
    }

    #[must_use]
    pub fn consumer_lag_stale_note(&self) -> Option<String> {
        self.consumer_lag_is_stale()
            .then(|| Self::describe_sample_age(self.consumer_lag_age_secs))
    }

    #[must_use]
    pub fn batch_latency_stale_note(&self) -> Option<String> {
        self.batch_latency_is_stale()
            .then(|| Self::describe_sample_age(self.last_batch_latency_age_secs))
    }

    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if let Some(error) = &self.query_error {
            warnings.push(format!(
                "Runtime health: failed to query runtime metrics ({error})"
            ));
        }
        match self.event_engine_status {
            EventEngineStatus::Healthy => {}
            EventEngineStatus::Stale => {
                warnings.push("Runtime health: event_engine heartbeat is stale".into());
            }
            EventEngineStatus::Down => warnings.push("Runtime health: event_engine is down".into()),
            EventEngineStatus::Unknown => {
                warnings.push("Runtime health: event_engine status is unknown".into());
            }
        }

        if let Some(lag) = self.fresh_consumer_lag_pending()
            && lag > RUNTIME_LAG_WARN_THRESHOLD
        {
            warnings.push(format!(
                "Runtime health: consumer lag is high ({lag:.0} pending)"
            ));
        }
        if let Some(note) = self.consumer_lag_stale_note() {
            warnings.push(format!(
                "Runtime health: consumer lag telemetry is stale ({note})"
            ));
        }

        if let Some(latency) = self.fresh_batch_latency_ms()
            && latency > RUNTIME_BATCH_LATENCY_WARN_THRESHOLD_MS
        {
            warnings.push(format!(
                "Runtime health: batch latency is high ({latency:.0}ms)"
            ));
        }
        if let Some(note) = self.batch_latency_stale_note() {
            warnings.push(format!(
                "Runtime health: batch latency telemetry is stale ({note})"
            ));
        }

        warnings
    }

    #[must_use]
    pub fn assessment(&self) -> RuntimeAssessment {
        let warnings = self.warnings();
        let status = if matches!(self.event_engine_status, EventEngineStatus::Unknown)
            && self.consumer_lag_pending.is_none()
            && self.last_batch_latency_ms.is_none()
        {
            RuntimeHealthStatus::Unavailable
        } else if warnings.is_empty() {
            RuntimeHealthStatus::Healthy
        } else {
            RuntimeHealthStatus::Degraded
        };

        RuntimeAssessment { status, warnings }
    }

    /// Format as a compact one-line summary fragment for status --summary
    #[must_use]
    pub fn summary_fragment(&self) -> String {
        let event_engine = format!("event_engine:{}", self.event_engine_status);
        let lag = self.fresh_consumer_lag_pending().map_or_else(
            || {
                if self.consumer_lag_is_stale() {
                    "lag:stale".to_string()
                } else {
                    "lag:-".to_string()
                }
            },
            |v| format!("lag:{v:.0}"),
        );
        let batch = self.fresh_batch_latency_ms().map_or_else(
            || {
                if self.batch_latency_is_stale() {
                    "batch:stale".to_string()
                } else {
                    "batch:-".to_string()
                }
            },
            |v| format!("batch:{v:.0}ms"),
        );
        let query = if self.query_error.is_some() {
            " query:error"
        } else {
            ""
        };
        format!("{event_engine} {lag} {batch}{query}")
    }
}

/// Default stale threshold in seconds for runtime heartbeats.
const HEARTBEAT_STALE_SECS: i64 = 120;
const TELEMETRY_STALE_SECS: i64 = 120;
const RUNTIME_LAG_WARN_THRESHOLD: f64 = 1000.0;
const RUNTIME_BATCH_LATENCY_WARN_THRESHOLD_MS: f64 = 5000.0;

/// Query runtime metrics from Postgres. Returns `Unknown` status if unreachable.
pub async fn query_runtime_metrics(db_url: &str) -> RuntimeMetrics {
    match query_inner(db_url).await {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!("Runtime metrics query failed: {e}");
            RuntimeMetrics::query_failure(e.to_string())
        }
    }
}

async fn query_inner(db_url: &str) -> Result<RuntimeMetrics, sqlx::Error> {
    // Single connection, short-lived — tight timeout since this is for status display
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(500))
        .connect(db_url)
        .await?;

    // 1. Heartbeat status from concrete module runs. Manifest rows are
    // inventory/provenance only and are not runtime liveness evidence.
    let heartbeat_row = sqlx::query_as!(
        HeartbeatRow,
        r#"
        SELECT
            nr.status,
            EXTRACT(EPOCH FROM (NOW() - nr.last_heartbeat_at))::bigint as "age_secs: i64"
        FROM core.runs nr
        JOIN core.manifests nm ON nm.id = nr.manifest_id
        WHERE nm.name = 'sinexd'
          AND nr.status = 'running'
        ORDER BY nr.last_heartbeat_at DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .fetch_optional(&pool)
    .await?;

    let (event_engine_status, last_heartbeat_age_secs) = match heartbeat_row {
        Some(row) => {
            let age = row.age_secs;
            let status = interpret_event_engine_status(row.status.as_deref(), age);
            (status, age)
        }
        None => (EventEngineStatus::Down, None),
    };

    // 2. Latest consumer lag from metric.gauge events
    // `(payload->>'value')::float8` is non-null when the row exists (gauge always has a value)
    let consumer_lag = sqlx::query_as!(
        TimedMetricRow,
        r#"
        SELECT
            (payload->>'value')::float8 AS "value!",
            EXTRACT(EPOCH FROM (NOW() - ts_coided))::bigint AS "age_secs!: i64"
        FROM core.events
        WHERE source = 'sinex'
          AND event_type = 'metric.gauge'
          AND payload->>'name' = 'event_engine.consumer.lag.pending'
        ORDER BY id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&pool)
    .await?;

    // 3. Latest batch latency from batch.stats events
    // `(payload->>'fetch_to_ack_ms')::float8` is non-null when the row exists
    let last_batch_latency = sqlx::query_as!(
        TimedMetricRow,
        r#"
        SELECT
            (payload->>'fetch_to_ack_ms')::float8 AS "value!",
            EXTRACT(EPOCH FROM (NOW() - ts_coided))::bigint AS "age_secs!: i64"
        FROM core.events
        WHERE source = $1
          AND event_type = 'batch.stats'
        ORDER BY id DESC
        LIMIT 1
        "#,
        event_engine_batch_stats_source(),
    )
    .fetch_optional(&pool)
    .await?;

    pool.close().await;

    Ok(RuntimeMetrics {
        event_engine_status,
        last_heartbeat_age_secs,
        consumer_lag_pending: consumer_lag.as_ref().map(|row| row.value),
        consumer_lag_age_secs: consumer_lag.as_ref().map(|row| row.age_secs),
        last_batch_latency_ms: last_batch_latency.as_ref().map(|row| row.value),
        last_batch_latency_age_secs: last_batch_latency.as_ref().map(|row| row.age_secs),
        query_error: None,
    })
}

#[derive(sqlx::FromRow)]
struct HeartbeatRow {
    status: Option<String>,
    age_secs: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct TimedMetricRow {
    value: f64,
    age_secs: i64,
}

fn interpret_event_engine_status(status: Option<&str>, age_secs: Option<i64>) -> EventEngineStatus {
    match status {
        Some("running" | "active") => match age_secs {
            Some(age) if age > HEARTBEAT_STALE_SECS => EventEngineStatus::Stale,
            Some(_) => EventEngineStatus::Healthy,
            None => EventEngineStatus::Down,
        },
        Some(_) | None => EventEngineStatus::Down,
    }
}

#[cfg(test)]
#[path = "runtime_metrics_test.rs"]
mod tests;
