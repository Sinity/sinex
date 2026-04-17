//! Telemetry RPC handlers
//!
//! Queries the `sinex_telemetry.*` read models and returns
//! structured responses for the `telemetry.*` RPC method namespace.

use color_eyre::eyre::{Result, WrapErr, eyre};
use serde_json::Value;
use sinex_primitives::rpc::telemetry::{
    CommandFrequencyEntry, FileActivityEntry, IngestdValidationSnapshot, RecentActivityEntry,
    SystemStateBucket, TelemetryCommandFrequencyRequest, TelemetryCommandFrequencyResponse,
    TelemetryFileActivityRequest, TelemetryFileActivityResponse, TelemetryIngestdValidationRequest,
    TelemetryIngestdValidationResponse, TelemetryRecentActivityRequest,
    TelemetryRecentActivityResponse, TelemetrySystemStateRequest, TelemetrySystemStateResponse,
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
    if resolved_from > resolved_to {
        return Err(eyre!(
            "invalid telemetry time range: 'from' must be earlier than or equal to 'to'"
        ));
    }
    Ok((resolved_from, resolved_to))
}

fn resolve_positive_limit(limit: Option<i64>) -> Result<i64> {
    let limit = limit.unwrap_or(50);
    if limit <= 0 {
        return Err(eyre!("telemetry limit must be positive, got {limit}"));
    }
    Ok(limit)
}

// ─────────────────────────────────────────────────────────────
// Row structs
// ─────────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct WindowFocusRow {
    bucket: OffsetDateTime,
    workspace: Option<String>,
    window_class: Option<String>,
    window_title: Option<String>,
    window_id: Option<String>,
    last_focus_time: Option<OffsetDateTime>,
    focus_event_count: i64,
}

#[derive(sqlx::FromRow)]
struct CommandFrequencyRow {
    command: String,
    shell: Option<String>,
    total_executions: i64,
    successful_executions: i64,
    failed_executions: i64,
    avg_duration_ms: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct FileActivityRow {
    bucket: OffsetDateTime,
    directory: Option<String>,
    event_type: String,
    total_events: i64,
    unique_files: i64,
}

#[derive(sqlx::FromRow)]
struct RecentActivityRow {
    activity_type: String,
    context: Option<String>,
    detail: Option<String>,
    timestamp: Option<OffsetDateTime>,
}

#[derive(sqlx::FromRow)]
struct SystemStateRow {
    bucket: OffsetDateTime,
    avg_cpu_percent: Option<f64>,
    max_cpu_percent: Option<f64>,
    avg_memory_percent: Option<f64>,
    max_memory_percent: Option<f64>,
    avg_disk_percent: Option<f64>,
    current_active_units: Option<i32>,
    sample_count: i64,
}

// ─────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────

/// Handle `telemetry.window_focus`
///
/// Queries `sinex_telemetry.current_window_focus` (event-time activity view) for the
/// requested time range (default: last 3 hours).
pub async fn handle_telemetry_window_focus(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryWindowFocusRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.window_focus request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        3,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, WindowFocusRow>(
        r"
        SELECT
            bucket,
            workspace,
            window_class,
            window_title,
            window_id,
            last_focus_time,
            focus_event_count
        FROM sinex_telemetry.current_window_focus
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC
        LIMIT $3
        ",
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
            workspace: r.workspace,
            window_class: r.window_class,
            window_title: r.window_title,
            window_id: r.window_id,
            last_focus_time: r.last_focus_time.map(fmt_rfc3339),
            focus_event_count: r.focus_event_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryWindowFocusResponse {
        buckets,
    })?)
}

/// Handle `telemetry.command_frequency`
///
/// Queries `sinex_telemetry.command_frequency_hourly` (event-time activity view), aggregating
/// total invocation counts and bucket spans for the requested window (default: last 24 hours).
pub async fn handle_telemetry_command_frequency(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryCommandFrequencyRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.command_frequency request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, CommandFrequencyRow>(
        r"
        SELECT
            command,
            shell,
            SUM(total_executions)::bigint AS total_executions,
            SUM(successful_executions)::bigint AS successful_executions,
            SUM(failed_executions)::bigint AS failed_executions,
            AVG(avg_duration_ms)::float8 AS avg_duration_ms
        FROM sinex_telemetry.command_frequency_hourly
        WHERE bucket >= $1
          AND bucket <= $2
        GROUP BY command, shell
        ORDER BY total_executions DESC, command ASC
        LIMIT $3
        ",
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
            shell: r.shell,
            total_executions: r.total_executions,
            successful_executions: r.successful_executions,
            failed_executions: r.failed_executions,
            avg_duration_ms: r.avg_duration_ms,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryCommandFrequencyResponse {
        entries,
    })?)
}

/// Handle `telemetry.file_activity`
///
/// Queries `sinex_telemetry.file_activity_summary` (event-time activity view) for the
/// requested time range (default: last 24 hours).
pub async fn handle_telemetry_file_activity(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryFileActivityRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.file_activity request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, FileActivityRow>(
        r"
        SELECT
            bucket,
            directory,
            event_type,
            total_events,
            unique_files
        FROM sinex_telemetry.file_activity_summary
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, total_events DESC, event_type ASC
        LIMIT $3
        ",
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
            event_type: r.event_type,
            total_events: r.total_events,
            unique_files: r.unique_files,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryFileActivityResponse {
        entries,
    })?)
}

/// Handle `telemetry.recent_activity`
///
/// Queries `sinex_telemetry.recent_activity_summary` (regular view with
/// hardcoded lookback). No time parameters — the view defines its own window.
pub async fn handle_telemetry_recent_activity(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryRecentActivityRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.recent_activity request")?;

    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, RecentActivityRow>(
        r"
        SELECT
            activity_type,
            context,
            detail,
            timestamp
        FROM sinex_telemetry.recent_activity_summary
        ORDER BY timestamp DESC
        LIMIT $1
        ",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.recent_activity_summary")?;

    let entries = rows
        .into_iter()
        .map(|r| RecentActivityEntry {
            activity_type: r.activity_type,
            context: r.context,
            detail: r.detail,
            timestamp: r.timestamp.map(fmt_rfc3339),
        })
        .collect();

    Ok(serde_json::to_value(TelemetryRecentActivityResponse {
        entries,
    })?)
}

/// Handle `telemetry.system_state`
///
/// Queries `sinex_telemetry.current_system_state` (event-time activity view) for the
/// requested time range (default: last 3 hours).
pub async fn handle_telemetry_system_state(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetrySystemStateRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.system_state request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        3,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, SystemStateRow>(
        r"
        SELECT
            bucket,
            avg_cpu_percent,
            max_cpu_percent,
            avg_memory_percent,
            max_memory_percent,
            avg_disk_percent,
            current_active_units,
            sample_count
        FROM sinex_telemetry.current_system_state
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC
        LIMIT $3
        ",
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
            avg_cpu_percent: r.avg_cpu_percent,
            max_cpu_percent: r.max_cpu_percent,
            avg_memory_percent: r.avg_memory_percent,
            max_memory_percent: r.max_memory_percent,
            avg_disk_percent: r.avg_disk_percent,
            current_active_units: r.current_active_units.map(i64::from),
            sample_count: r.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetrySystemStateResponse {
        buckets,
    })?)
}

/// Handle `telemetry.ingestd_validation`
///
/// Returns the latest ingestd batch / validation snapshot for operator visibility.
pub async fn handle_telemetry_ingestd_validation(pool: &PgPool, params: Value) -> Result<Value> {
    let _req: TelemetryIngestdValidationRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.ingestd_validation request")?;

    let row = sqlx::query!(
        r#"
        SELECT
            ts_coided AS "observed_at!: time::OffsetDateTime",
            (payload->>'batch_size')::bigint AS "batch_size!",
            (payload->>'fetch_to_ack_ms')::bigint AS "fetch_to_ack_ms!",
            (payload->>'events_deferred')::bigint AS "events_deferred!",
            (payload->>'events_failed')::bigint AS "events_failed!",
            (payload->>'had_synthesis')::boolean AS "had_synthesis!",
            payload->>'insert_path' AS "insert_path!",
            (payload->>'validation_valid')::bigint AS "validation_valid!",
            (payload->>'validation_skipped')::bigint AS "validation_skipped!",
            (payload->>'validation_no_schema')::bigint AS "validation_no_schema!",
            (payload->>'validation_schema_not_found')::bigint AS "validation_schema_not_found!",
            (payload->>'validation_invalid')::bigint AS "validation_invalid!",
            (payload->>'validation_coverage_pct')::float8 AS "validation_coverage_pct!",
            COALESCE((payload->>'suspicious_future_ts_orig')::bigint, 0) AS "suspicious_future_ts_orig!"
        FROM core.events
        WHERE source = 'sinex.ingestd'
          AND event_type = 'batch.stats'
        ORDER BY id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .wrap_err("failed to query latest ingestd validation stats")?;

    let snapshot = row.map(|row| IngestdValidationSnapshot {
        observed_at: fmt_rfc3339(row.observed_at),
        batch_size: row.batch_size,
        fetch_to_ack_ms: row.fetch_to_ack_ms,
        events_deferred: row.events_deferred,
        events_failed: row.events_failed,
        had_synthesis: row.had_synthesis,
        insert_path: row.insert_path,
        validation_valid: row.validation_valid,
        validation_skipped: row.validation_skipped,
        validation_no_schema: row.validation_no_schema,
        validation_schema_not_found: row.validation_schema_not_found,
        validation_invalid: row.validation_invalid,
        validation_coverage_pct: row.validation_coverage_pct,
        suspicious_future_ts_orig: row.suspicious_future_ts_orig,
    });

    Ok(serde_json::to_value(TelemetryIngestdValidationResponse {
        snapshot,
    })?)
}
