//! Telemetry RPC handlers
//!
//! Queries the `sinex_telemetry.*` read models and returns structured responses
//! for the `telemetry.*` RPC method namespace.

use color_eyre::eyre::{Result, WrapErr, eyre};
use serde_json::Value;
use sinex_primitives::rpc::telemetry::{
    AssemblyStatsBucket, CommandFrequencyEntry, CurrentDeviceStateEntry, CurrentHealthEntry,
    FileActivityEntry, GatewayStatsBucket, IngestdBatchStatsBucket, IngestdValidationSnapshot,
    MetricCounterBucket, NodeStatsBucket, RecentActivityEntry, StreamStatsBucket,
    SystemStateBucket, TelemetryAssemblyStatsRequest, TelemetryAssemblyStatsResponse,
    TelemetryCommandFrequencyRequest, TelemetryCommandFrequencyResponse,
    TelemetryCurrentDeviceStateRequest, TelemetryCurrentDeviceStateResponse,
    TelemetryCurrentHealthRequest, TelemetryCurrentHealthResponse, TelemetryFileActivityRequest,
    TelemetryFileActivityResponse, TelemetryGatewayStatsRequest, TelemetryGatewayStatsResponse,
    TelemetryIngestdBatchStatsRequest, TelemetryIngestdBatchStatsResponse,
    TelemetryIngestdValidationRequest, TelemetryIngestdValidationResponse,
    TelemetryMetricCountersRequest, TelemetryMetricCountersResponse, TelemetryNodeStatsRequest,
    TelemetryNodeStatsResponse, TelemetryRecentActivityRequest, TelemetryRecentActivityResponse,
    TelemetryStreamStatsRequest, TelemetryStreamStatsResponse, TelemetrySystemStateRequest,
    TelemetrySystemStateResponse, TelemetryWindowFocusRequest, TelemetryWindowFocusResponse,
    WindowFocusBucket,
};
use sqlx::PgPool;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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

#[derive(sqlx::FromRow)]
struct CurrentHealthRow {
    source: String,
    event_type: String,
    component: Option<String>,
    status: Option<String>,
    reason: Option<String>,
    last_update: OffsetDateTime,
}

#[derive(sqlx::FromRow)]
struct CurrentDeviceStateRow {
    unit_name: Option<String>,
    unit_type: Option<String>,
    state: Option<String>,
    sub_state: Option<String>,
    last_update: OffsetDateTime,
}

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
    current_active_units: Option<i64>,
    sample_count: i64,
}

#[derive(sqlx::FromRow)]
struct GatewayStatsRow {
    bucket: OffsetDateTime,
    source: String,
    stat_events: i64,
    avg_total_requests: Option<f64>,
    total_rate_limited: Option<i64>,
    avg_latency_ms: Option<f64>,
    max_p99_latency_ms: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct StreamStatsRow {
    bucket: OffsetDateTime,
    stream_name: Option<String>,
    avg_fill_pct: Option<f64>,
    max_fill_pct: Option<f64>,
    avg_messages: Option<f64>,
    max_messages: Option<i64>,
    sample_count: i64,
}

#[derive(sqlx::FromRow)]
struct AssemblyStatsRow {
    bucket: OffsetDateTime,
    max_active_assemblies: Option<i64>,
    total_completed: Option<i64>,
    total_cancelled: Option<i64>,
    total_failed: Option<i64>,
    total_timed_out: Option<i64>,
    avg_duration_ms: Option<f64>,
    sample_count: i64,
}

#[derive(sqlx::FromRow)]
struct NodeStatsRow {
    bucket: OffsetDateTime,
    node_type: Option<String>,
    total_events_processed: Option<i64>,
    total_events_dropped: Option<i64>,
    avg_latency_ms: Option<f64>,
    max_queue_depth: Option<i64>,
    total_errors: Option<i64>,
    sample_count: i64,
}

#[derive(sqlx::FromRow)]
struct MetricCounterRow {
    bucket: OffsetDateTime,
    component: Option<String>,
    metric_name: Option<String>,
    total_value: Option<i64>,
    max_value: Option<i64>,
    sample_count: i64,
}

#[derive(sqlx::FromRow)]
struct IngestdBatchStatsRow {
    bucket: OffsetDateTime,
    avg_batch_size: Option<f64>,
    max_batch_size: Option<i64>,
    avg_latency_ms: Option<f64>,
    max_latency_ms: Option<f64>,
    total_deferred: Option<i64>,
    total_failed: Option<i64>,
    synthesis_batches: i64,
    batch_count: i64,
    validation_valid: Option<i64>,
    validation_skipped: Option<i64>,
    validation_no_schema: Option<i64>,
    validation_schema_not_found: Option<i64>,
    validation_invalid: Option<i64>,
    avg_validation_coverage_pct: Option<f64>,
}

pub async fn handle_telemetry_current_health(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryCurrentHealthRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.current_health request")?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, CurrentHealthRow>(
        r"
        SELECT
            source,
            event_type,
            component,
            status,
            reason,
            last_update
        FROM sinex_telemetry.current_health
        ORDER BY last_update DESC, component ASC NULLS LAST
        LIMIT $1
        ",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.current_health")?;

    let entries = rows
        .into_iter()
        .map(|row| CurrentHealthEntry {
            source: row.source,
            event_type: row.event_type,
            component: row.component,
            status: row.status,
            reason: row.reason,
            last_update: fmt_rfc3339(row.last_update),
        })
        .collect();

    Ok(serde_json::to_value(TelemetryCurrentHealthResponse {
        entries,
    })?)
}

pub async fn handle_telemetry_current_device_state(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryCurrentDeviceStateRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.current_device_state request")?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, CurrentDeviceStateRow>(
        r"
        SELECT
            unit_name,
            unit_type,
            state,
            sub_state,
            last_update
        FROM sinex_telemetry.current_device_state
        ORDER BY last_update DESC, unit_name ASC NULLS LAST
        LIMIT $1
        ",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.current_device_state")?;

    let entries = rows
        .into_iter()
        .map(|row| CurrentDeviceStateEntry {
            unit_name: row.unit_name,
            unit_type: row.unit_type,
            state: row.state,
            sub_state: row.sub_state,
            last_update: fmt_rfc3339(row.last_update),
        })
        .collect();

    Ok(serde_json::to_value(TelemetryCurrentDeviceStateResponse {
        entries,
    })?)
}

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
        .map(|row| WindowFocusBucket {
            bucket: fmt_rfc3339(row.bucket),
            workspace: row.workspace,
            window_class: row.window_class,
            window_title: row.window_title,
            window_id: row.window_id,
            last_focus_time: row.last_focus_time.map(fmt_rfc3339),
            focus_event_count: row.focus_event_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryWindowFocusResponse {
        buckets,
    })?)
}

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
        .map(|row| CommandFrequencyEntry {
            command: row.command,
            shell: row.shell,
            total_executions: row.total_executions,
            successful_executions: row.successful_executions,
            failed_executions: row.failed_executions,
            avg_duration_ms: row.avg_duration_ms,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryCommandFrequencyResponse {
        entries,
    })?)
}

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
        .map(|row| FileActivityEntry {
            bucket: fmt_rfc3339(row.bucket),
            directory: row.directory,
            event_type: row.event_type,
            total_events: row.total_events,
            unique_files: row.unique_files,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryFileActivityResponse {
        entries,
    })?)
}

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
        .map(|row| RecentActivityEntry {
            activity_type: row.activity_type,
            context: row.context,
            detail: row.detail,
            timestamp: row.timestamp.map(fmt_rfc3339),
        })
        .collect();

    Ok(serde_json::to_value(TelemetryRecentActivityResponse {
        entries,
    })?)
}

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
            current_active_units::bigint AS current_active_units,
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
        .map(|row| SystemStateBucket {
            bucket: fmt_rfc3339(row.bucket),
            avg_cpu_percent: row.avg_cpu_percent,
            max_cpu_percent: row.max_cpu_percent,
            avg_memory_percent: row.avg_memory_percent,
            max_memory_percent: row.max_memory_percent,
            avg_disk_percent: row.avg_disk_percent,
            current_active_units: row.current_active_units,
            sample_count: row.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetrySystemStateResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_gateway_stats(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryGatewayStatsRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.gateway_stats request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, GatewayStatsRow>(
        r"
        SELECT
            bucket,
            source,
            stat_events,
            avg_total_requests::float8 AS avg_total_requests,
            total_rate_limited::bigint AS total_rate_limited,
            avg_latency_ms::float8 AS avg_latency_ms,
            max_p99_latency_ms::float8 AS max_p99_latency_ms
        FROM sinex_telemetry.gateway_stats_1h
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, source ASC
        LIMIT $3
        ",
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.gateway_stats_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| GatewayStatsBucket {
            bucket: fmt_rfc3339(row.bucket),
            source: row.source,
            stat_events: row.stat_events,
            avg_total_requests: row.avg_total_requests,
            total_rate_limited: row.total_rate_limited,
            avg_latency_ms: row.avg_latency_ms,
            max_p99_latency_ms: row.max_p99_latency_ms,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryGatewayStatsResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_stream_stats(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryStreamStatsRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.stream_stats request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, StreamStatsRow>(
        r"
        SELECT
            bucket,
            stream_name,
            avg_fill_pct::float8 AS avg_fill_pct,
            max_fill_pct::float8 AS max_fill_pct,
            avg_messages::float8 AS avg_messages,
            max_messages::bigint AS max_messages,
            sample_count
        FROM sinex_telemetry.stream_stats_1h
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, stream_name ASC NULLS LAST
        LIMIT $3
        ",
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.stream_stats_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| StreamStatsBucket {
            bucket: fmt_rfc3339(row.bucket),
            stream_name: row.stream_name,
            avg_fill_pct: row.avg_fill_pct,
            max_fill_pct: row.max_fill_pct,
            avg_messages: row.avg_messages,
            max_messages: row.max_messages,
            sample_count: row.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryStreamStatsResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_assembly_stats(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryAssemblyStatsRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.assembly_stats request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, AssemblyStatsRow>(
        r"
        SELECT
            bucket,
            max_active_assemblies::bigint AS max_active_assemblies,
            total_completed::bigint AS total_completed,
            total_cancelled::bigint AS total_cancelled,
            total_failed::bigint AS total_failed,
            total_timed_out::bigint AS total_timed_out,
            avg_duration_ms::float8 AS avg_duration_ms,
            sample_count
        FROM sinex_telemetry.assembly_stats_1h
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
    .wrap_err("failed to query sinex_telemetry.assembly_stats_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| AssemblyStatsBucket {
            bucket: fmt_rfc3339(row.bucket),
            max_active_assemblies: row.max_active_assemblies,
            total_completed: row.total_completed,
            total_cancelled: row.total_cancelled,
            total_failed: row.total_failed,
            total_timed_out: row.total_timed_out,
            avg_duration_ms: row.avg_duration_ms,
            sample_count: row.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryAssemblyStatsResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_node_stats(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryNodeStatsRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.node_stats request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, NodeStatsRow>(
        r"
        SELECT
            bucket,
            node_type,
            total_events_processed::bigint AS total_events_processed,
            total_events_dropped::bigint AS total_events_dropped,
            avg_latency_ms::float8 AS avg_latency_ms,
            max_queue_depth::bigint AS max_queue_depth,
            total_errors::bigint AS total_errors,
            sample_count
        FROM sinex_telemetry.node_stats_1h
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, node_type ASC NULLS LAST
        LIMIT $3
        ",
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.node_stats_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| NodeStatsBucket {
            bucket: fmt_rfc3339(row.bucket),
            node_type: row.node_type,
            total_events_processed: row.total_events_processed,
            total_events_dropped: row.total_events_dropped,
            avg_latency_ms: row.avg_latency_ms,
            max_queue_depth: row.max_queue_depth,
            total_errors: row.total_errors,
            sample_count: row.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryNodeStatsResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_metric_counters(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryMetricCountersRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.metric_counters request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, MetricCounterRow>(
        r"
        SELECT
            bucket,
            component,
            metric_name,
            total_value::bigint AS total_value,
            max_value::bigint AS max_value,
            sample_count
        FROM sinex_telemetry.metric_counters_1h
        WHERE bucket >= $1
          AND bucket <= $2
        ORDER BY bucket DESC, total_value DESC NULLS LAST, metric_name ASC NULLS LAST
        LIMIT $3
        ",
    )
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await
    .wrap_err("failed to query sinex_telemetry.metric_counters_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| MetricCounterBucket {
            bucket: fmt_rfc3339(row.bucket),
            component: row.component,
            metric_name: row.metric_name,
            total_value: row.total_value,
            max_value: row.max_value,
            sample_count: row.sample_count,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryMetricCountersResponse {
        buckets,
    })?)
}

pub async fn handle_telemetry_ingestd_batch_stats(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TelemetryIngestdBatchStatsRequest = super::parse_default_on_null(params)
        .wrap_err("failed to parse telemetry.ingestd_batch_stats request")?;

    let (from, to) = resolve_time_range(
        req.time_range.from.as_deref(),
        req.time_range.to.as_deref(),
        24,
    )?;
    let limit = resolve_positive_limit(req.limit)?;

    let rows = sqlx::query_as::<_, IngestdBatchStatsRow>(
        r"
        SELECT
            bucket,
            avg_batch_size::float8 AS avg_batch_size,
            max_batch_size::bigint AS max_batch_size,
            avg_latency_ms::float8 AS avg_latency_ms,
            max_latency_ms::float8 AS max_latency_ms,
            total_deferred::bigint AS total_deferred,
            total_failed::bigint AS total_failed,
            synthesis_batches,
            batch_count,
            validation_valid::bigint AS validation_valid,
            validation_skipped::bigint AS validation_skipped,
            validation_no_schema::bigint AS validation_no_schema,
            validation_schema_not_found::bigint AS validation_schema_not_found,
            validation_invalid::bigint AS validation_invalid,
            avg_validation_coverage_pct::float8 AS avg_validation_coverage_pct
        FROM sinex_telemetry.ingestd_batch_stats_1h
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
    .wrap_err("failed to query sinex_telemetry.ingestd_batch_stats_1h")?;

    let buckets = rows
        .into_iter()
        .map(|row| IngestdBatchStatsBucket {
            bucket: fmt_rfc3339(row.bucket),
            avg_batch_size: row.avg_batch_size,
            max_batch_size: row.max_batch_size,
            avg_latency_ms: row.avg_latency_ms,
            max_latency_ms: row.max_latency_ms,
            total_deferred: row.total_deferred,
            total_failed: row.total_failed,
            synthesis_batches: row.synthesis_batches,
            batch_count: row.batch_count,
            validation_valid: row.validation_valid,
            validation_skipped: row.validation_skipped,
            validation_no_schema: row.validation_no_schema,
            validation_schema_not_found: row.validation_schema_not_found,
            validation_invalid: row.validation_invalid,
            avg_validation_coverage_pct: row.avg_validation_coverage_pct,
        })
        .collect();

    Ok(serde_json::to_value(TelemetryIngestdBatchStatsResponse {
        buckets,
    })?)
}

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
