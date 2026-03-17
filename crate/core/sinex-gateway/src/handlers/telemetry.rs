//! Telemetry RPC handlers
//!
//! Queries the `sinex_telemetry.*` continuous-aggregate views and returns
//! structured responses for the `telemetry.*` RPC method namespace.

use color_eyre::eyre::{Result, WrapErr};
use serde_json::Value;
use sinex_primitives::rpc::telemetry::{
    CommandFrequencyEntry, FileActivityEntry, RecentActivityEntry, SystemStateBucket,
    TelemetryCommandFrequencyRequest, TelemetryCommandFrequencyResponse,
    TelemetryFileActivityRequest, TelemetryFileActivityResponse,
    TelemetryRecentActivityRequest, TelemetryRecentActivityResponse,
    TelemetrySystemStateRequest, TelemetrySystemStateResponse,
    TelemetryWindowFocusRequest, TelemetryWindowFocusResponse, WindowFocusBucket,
};
use sqlx::PgPool;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

// ─────────────────────────────────────────────────────────────
// Time-range helpers
// ─────────────────────────────────────────────────────────────

fn parse_rfc3339(s: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339)
        .wrap_err_with(|| format!("invalid RFC 3339 timestamp: {s:?}"))
}

fn fmt_rfc3339(dt: OffsetDateTime) -> String {
    dt.format(&Rfc3339).unwrap_or_else(|_| dt.to_string())
}

/// Resolve an optional (from, to) pair, falling back to `(now - default_hours, now)`.
fn resolve_time_range(
    from: Option<&str>,
    to: Option<&str>,
    default_hours: i64,
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let now = OffsetDateTime::now_utc();
    let resolved_to = match to {
        Some(s) => parse_rfc3339(s)?,
        None => now,
    };
    let resolved_from = match from {
        Some(s) => parse_rfc3339(s)?,
        None => resolved_to - time::Duration::hours(default_hours),
    };
    Ok((resolved_from, resolved_to))
}

// ─────────────────────────────────────────────────────────────
// Row structs
// ─────────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct WindowFocusRow {
    bucket: OffsetDateTime,
    app_name: Option<String>,
    focus_count: i64,
    total_duration_secs: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct CommandFrequencyRow {
    command: String,
    total_count: i64,
    bucket_count: i64,
}

#[derive(sqlx::FromRow)]
struct FileActivityRow {
    bucket: OffsetDateTime,
    directory: Option<String>,
    event_count: i64,
}

#[derive(sqlx::FromRow)]
struct RecentActivityRow {
    activity_type: String,
    summary: Option<String>,
    recorded_at: Option<OffsetDateTime>,
}

#[derive(sqlx::FromRow)]
struct SystemStateRow {
    bucket: OffsetDateTime,
    avg_cpu_pct: Option<f64>,
    avg_memory_bytes: Option<f64>,
    avg_disk_io_bps: Option<f64>,
}

// ─────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────

/// Handle `telemetry.window_focus`
///
/// Queries `sinex_telemetry.current_window_focus` (5-minute CA) for the
/// requested time range (default: last 3 hours).
pub async fn handle_telemetry_window_focus(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryWindowFocusRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.window_focus request")?;

    let (from, to) =
        resolve_time_range(req.time_range.from.as_deref(), req.time_range.to.as_deref(), 3)?;
    let limit = req.limit.unwrap_or(50);

    let rows = sqlx::query_as::<_, WindowFocusRow>(
        r#"
        SELECT
            bucket,
            app_name,
            focus_count,
            total_duration_secs
        FROM sinex_telemetry.current_window_focus
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC
        LIMIT $3
        "#,
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.current_window_focus")?;

    let buckets = rows
        .into_iter()
        .map(|r| WindowFocusBucket {
            bucket: fmt_rfc3339(r.bucket),
            app_name: r.app_name,
            focus_count: r.focus_count,
            total_duration_secs: r.total_duration_secs,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryWindowFocusResponse { buckets })?)
}

/// Handle `telemetry.command_frequency`
///
/// Queries `sinex_telemetry.command_frequency_hourly` (1-hour CA), aggregating
/// total invocation counts and bucket spans for the requested window (default: last 24 hours).
pub async fn handle_telemetry_command_frequency(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryCommandFrequencyRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.command_frequency request")?;

    let (from, to) =
        resolve_time_range(req.time_range.from.as_deref(), req.time_range.to.as_deref(), 24)?;
    let limit = req.limit.unwrap_or(50);

    let rows = sqlx::query_as::<_, CommandFrequencyRow>(
        r#"
        SELECT
            command,
            SUM(execution_count)::bigint AS total_count,
            COUNT(*)::bigint AS bucket_count
        FROM sinex_telemetry.command_frequency_hourly
        WHERE bucket >= $1
          AND bucket <= $2
        GROUP BY command
        ORDER BY total_count DESC
        LIMIT $3
        "#,
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.command_frequency_hourly")?;

    let entries = rows
        .into_iter()
        .map(|r| CommandFrequencyEntry {
            command: r.command,
            total_count: r.total_count,
            bucket_count: r.bucket_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryCommandFrequencyResponse { entries })?)
}

/// Handle `telemetry.file_activity`
///
/// Queries `sinex_telemetry.file_activity_summary` (1-hour CA) for the
/// requested time range (default: last 24 hours).
pub async fn handle_telemetry_file_activity(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryFileActivityRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.file_activity request")?;

    let (from, to) =
        resolve_time_range(req.time_range.from.as_deref(), req.time_range.to.as_deref(), 24)?;
    let limit = req.limit.unwrap_or(50);

    let rows = sqlx::query_as::<_, FileActivityRow>(
        r#"
        SELECT
            bucket,
            directory,
            event_count
        FROM sinex_telemetry.file_activity_summary
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, event_count DESC
        LIMIT $3
        "#,
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.file_activity_summary")?;

    let entries = rows
        .into_iter()
        .map(|r| FileActivityEntry {
            bucket: fmt_rfc3339(r.bucket),
            directory: r.directory,
            event_count: r.event_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryFileActivityResponse { entries })?)
}

/// Handle `telemetry.recent_activity`
///
/// Queries `sinex_telemetry.recent_activity_summary` (regular view with
/// hardcoded lookback). No time parameters — the view defines its own window.
pub async fn handle_telemetry_recent_activity(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryRecentActivityRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.recent_activity request")?;

    let limit = req.limit.unwrap_or(50);

    let rows = sqlx::query_as::<_, RecentActivityRow>(
        r#"
        SELECT
            activity_type,
            summary,
            recorded_at
        FROM sinex_telemetry.recent_activity_summary
        ORDER BY recorded_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.recent_activity_summary")?;

    let entries = rows
        .into_iter()
        .map(|r| RecentActivityEntry {
            activity_type: r.activity_type,
            summary: r.summary,
            recorded_at: r.recorded_at.map(fmt_rfc3339),
        })
        .collect();

    Ok(serde_json::to_value(TelemetryRecentActivityResponse { entries })?)
}

/// Handle `telemetry.system_state`
///
/// Queries `sinex_telemetry.current_system_state` (5-minute CA) for the
/// requested time range (default: last 3 hours).
pub async fn handle_telemetry_system_state(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetrySystemStateRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.system_state request")?;

    let (from, to) =
        resolve_time_range(req.time_range.from.as_deref(), req.time_range.to.as_deref(), 3)?;
    let limit = req.limit.unwrap_or(50);

    let rows = sqlx::query_as::<_, SystemStateRow>(
        r#"
        SELECT
            bucket,
            avg_cpu_pct,
            avg_memory_bytes,
            avg_disk_io_bps
        FROM sinex_telemetry.current_system_state
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC
        LIMIT $3
        "#,
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.current_system_state")?;

    let buckets = rows
        .into_iter()
        .map(|r| SystemStateBucket {
            bucket: fmt_rfc3339(r.bucket),
            avg_cpu_pct: r.avg_cpu_pct,
            avg_memory_bytes: r.avg_memory_bytes,
            avg_disk_io_bps: r.avg_disk_io_bps,
        })
        .collect();

    Ok(serde_json::to_value(TelemetrySystemStateResponse { buckets })?)
}
