//! Regression coverage for telemetry RPC handlers against the live read-model schema.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_telemetry_assembly_stats, handle_telemetry_command_frequency,
    handle_telemetry_current_device_state, handle_telemetry_current_health,
    handle_telemetry_file_activity, handle_telemetry_gateway_stats,
    handle_telemetry_ingestd_batch_stats, handle_telemetry_ingestd_validation,
    handle_telemetry_metric_counters, handle_telemetry_node_stats,
    handle_telemetry_recent_activity, handle_telemetry_stream_stats, handle_telemetry_system_state,
    handle_telemetry_window_focus,
};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::telemetry::{
    TelemetryAssemblyStatsResponse, TelemetryCommandFrequencyResponse,
    TelemetryCurrentDeviceStateResponse, TelemetryCurrentHealthResponse,
    TelemetryFileActivityResponse, TelemetryGatewayStatsResponse,
    TelemetryIngestdBatchStatsResponse, TelemetryIngestdValidationResponse,
    TelemetryMetricCountersResponse, TelemetryNodeStatsResponse, TelemetryRecentActivityResponse,
    TelemetryStreamStatsResponse, TelemetrySystemStateResponse, TelemetryWindowFocusResponse,
};
use time::format_description::well_known::Rfc3339;
use xtask::sandbox::prelude::*;

async fn insert_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
    ts_orig: Option<time::OffsetDateTime>,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    let event = {
        let builder = DynamicPayload::new(source, event_type, payload).from_material(material_id);
        match ts_orig {
            Some(ts_orig) => builder.at_time(ts_orig.into()).build()?,
            None => builder.build()?,
        }
    };
    ctx.pool().events().insert(event).await?;
    Ok(())
}

async fn refresh_current_device_state(ctx: &TestContext) -> TestResult<()> {
    sqlx::query("REFRESH MATERIALIZED VIEW sinex_telemetry.current_device_state")
        .execute(ctx.pool())
        .await?;
    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_follow_current_read_model_schema(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(ctx.pool()).await?;

    let now = time::OffsetDateTime::now_utc();
    let from = (now - time::Duration::hours(1)).format(&Rfc3339)?;
    let to = (now + time::Duration::hours(1)).format(&Rfc3339)?;

    insert_event(
        &ctx,
        "wm.hyprland",
        "window.focused",
        json!({
            "workspace_id": 3,
            "window_class": "foot",
            "window_title": "cargo test",
            "window_id": "0xabc",
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "shell.atuin",
        "command.executed",
        json!({
            "command_string": "cargo test",
            "cwd": "/tmp",
            "exit_code": 0,
            "duration_ns": 123_000_000u64,
            "atuin_history_id": "hist-1",
            "atuin_session_id": "sess-1",
            "timestamp": 1_775_263_628_752_798_659i64,
            "ts_start_orig": "2026-04-04T00:47:08.752798659Z",
            "ts_end_orig": "2026-04-04T00:47:08.875798659Z",
            "hostname": "sinnix-prime",
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "fs-watcher",
        "file.created",
        json!({
            "path": "/tmp/telemetry-handlers/report.md",
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "system-ingestor",
        "system.resources",
        json!({
            "cpu_percent": 17.5,
            "memory_percent": 42.0,
            "disk_percent": 63.5,
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "system-ingestor",
        "systemd.units_summary",
        json!({
            "active_units": 42,
            "cpu_percent": 15.0,
            "memory_percent": 40.0,
            "disk_percent": 60.0,
        }),
        None,
    )
    .await?;

    let params = json!({ "from": from, "to": to, "limit": 10 });

    let window_focus: TelemetryWindowFocusResponse =
        serde_json::from_value(handle_telemetry_window_focus(ctx.pool(), params.clone()).await?)?;
    let command_frequency: TelemetryCommandFrequencyResponse = serde_json::from_value(
        handle_telemetry_command_frequency(ctx.pool(), params.clone()).await?,
    )?;
    let file_activity: TelemetryFileActivityResponse =
        serde_json::from_value(handle_telemetry_file_activity(ctx.pool(), params.clone()).await?)?;
    let recent_activity: TelemetryRecentActivityResponse = serde_json::from_value(
        handle_telemetry_recent_activity(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let system_state: TelemetrySystemStateResponse = serde_json::from_value(
        handle_telemetry_system_state(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;

    assert_eq!(window_focus.buckets.len(), 1);
    let focus = &window_focus.buckets[0];
    assert_eq!(focus.workspace.as_deref(), Some("3"));
    assert_eq!(focus.window_class.as_deref(), Some("foot"));
    assert_eq!(focus.window_title.as_deref(), Some("cargo test"));
    assert_eq!(focus.window_id.as_deref(), Some("0xabc"));
    assert_eq!(focus.focus_event_count, 1);
    assert!(focus.last_focus_time.is_some());

    assert_eq!(command_frequency.entries.len(), 1);
    let command = &command_frequency.entries[0];
    assert_eq!(command.command, "cargo test");
    assert_eq!(command.shell.as_deref(), Some("atuin"));
    assert_eq!(command.total_executions, 1);
    assert_eq!(command.successful_executions, 1);
    assert_eq!(command.failed_executions, 0);
    assert_eq!(command.avg_duration_ms, Some(123.0));

    assert_eq!(file_activity.entries.len(), 1);
    let file = &file_activity.entries[0];
    assert_eq!(file.directory.as_deref(), Some("/tmp/telemetry-handlers"));
    assert_eq!(file.event_type, "file.created");
    assert_eq!(file.total_events, 1);
    assert_eq!(file.unique_files, 1);

    assert!(
        recent_activity
            .entries
            .iter()
            .any(|entry| entry.activity_type == "window_focus"
                && entry.context.as_deref() == Some("3")
                && entry.detail.as_deref() == Some("foot"))
    );
    assert!(recent_activity.entries.iter().any(
        |entry| entry.activity_type == "system_load" && entry.context.as_deref() == Some("cpu")
    ));
    assert!(
        recent_activity
            .entries
            .iter()
            .any(|entry| entry.activity_type == "command_execution"
                && entry.context.as_deref() == Some("atuin")
                && entry.detail.as_deref() == Some("cargo test"))
    );

    assert_eq!(system_state.buckets.len(), 1);
    let state = &system_state.buckets[0];
    assert_eq!(state.avg_cpu_percent, Some(16.25));
    assert_eq!(state.max_cpu_percent, Some(17.5));
    assert_eq!(state.avg_memory_percent, Some(41.0));
    assert_eq!(state.max_memory_percent, Some(42.0));
    assert_eq!(state.avg_disk_percent, Some(61.75));
    assert_eq!(state.current_active_units, Some(42));
    assert_eq!(state.sample_count, 2);

    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_bucket_activity_by_event_time(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(ctx.pool()).await?;

    let imported_at = time::OffsetDateTime::parse("2026-01-02T03:15:00Z", &Rfc3339)?;

    insert_event(
        &ctx,
        "wm.hyprland",
        "window.focused",
        json!({
            "workspace_id": 42,
            "window_class": "kitty",
            "window_title": "imported focus",
            "window_id": "0x42",
        }),
        Some(imported_at),
    )
    .await?;
    insert_event(
        &ctx,
        "shell.history.zsh",
        "command.executed",
        json!({
            "command": "git status",
            "exit_code": 0,
            "duration_ms": 15.0,
        }),
        Some(imported_at),
    )
    .await?;
    insert_event(
        &ctx,
        "fs-watcher",
        "file.modified",
        json!({
            "path": "/tmp/imported/session.log",
        }),
        Some(imported_at),
    )
    .await?;
    insert_event(
        &ctx,
        "system-ingestor",
        "system.resources",
        json!({
            "cpu_percent": 11.0,
            "memory_percent": 33.0,
            "disk_percent": 44.0,
        }),
        Some(imported_at),
    )
    .await?;
    insert_event(
        &ctx,
        "system-ingestor",
        "systemd.units_summary",
        json!({
            "active_units": 7,
            "cpu_percent": 9.0,
            "memory_percent": 31.0,
            "disk_percent": 40.0,
        }),
        Some(imported_at),
    )
    .await?;

    let params = json!({
        "from": "2026-01-02T00:00:00Z",
        "to": "2026-01-03T00:00:00Z",
        "limit": 10
    });

    let window_focus: TelemetryWindowFocusResponse =
        serde_json::from_value(handle_telemetry_window_focus(ctx.pool(), params.clone()).await?)?;
    let command_frequency: TelemetryCommandFrequencyResponse = serde_json::from_value(
        handle_telemetry_command_frequency(ctx.pool(), params.clone()).await?,
    )?;
    let file_activity: TelemetryFileActivityResponse =
        serde_json::from_value(handle_telemetry_file_activity(ctx.pool(), params.clone()).await?)?;
    let system_state: TelemetrySystemStateResponse =
        serde_json::from_value(handle_telemetry_system_state(ctx.pool(), params).await?)?;

    assert_eq!(window_focus.buckets.len(), 1);
    assert_eq!(window_focus.buckets[0].workspace.as_deref(), Some("42"));
    assert_eq!(
        window_focus.buckets[0].window_class.as_deref(),
        Some("kitty")
    );

    assert_eq!(command_frequency.entries.len(), 1);
    assert_eq!(command_frequency.entries[0].command, "git status");
    assert_eq!(command_frequency.entries[0].total_executions, 1);

    assert_eq!(file_activity.entries.len(), 1);
    assert_eq!(
        file_activity.entries[0].directory.as_deref(),
        Some("/tmp/imported")
    );
    assert_eq!(file_activity.entries[0].event_type, "file.modified");

    assert_eq!(system_state.buckets.len(), 1);
    assert_eq!(system_state.buckets[0].current_active_units, Some(7));
    assert_eq!(system_state.buckets[0].sample_count, 2);

    Ok(())
}

#[sinex_test]
async fn operator_telemetry_handlers_follow_read_model_schema(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(ctx.pool()).await?;

    let now = time::OffsetDateTime::now_utc();
    let from = (now - time::Duration::hours(1)).format(&Rfc3339)?;
    let to = (now + time::Duration::hours(1)).format(&Rfc3339)?;

    insert_event(
        &ctx,
        "sinex",
        "health.status",
        json!({
            "component": "gateway",
            "current_status": "healthy",
            "reason": "steady"
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "system-ingestor",
        "systemd.unit_changed",
        json!({
            "unit_name": "sinex-gateway.service",
            "unit_type": "service",
            "state": "active",
            "sub_state": "running"
        }),
        None,
    )
    .await?;
    refresh_current_device_state(&ctx).await?;
    insert_event(
        &ctx,
        "sinex.gateway.rpc",
        "request.stats",
        json!({
            "total_requests": 120,
            "rate_limited_requests": 7,
            "avg_latency_ms": 12.5,
            "p99_latency_ms": 25.0
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "sinex.ingestd",
        "stream.stats",
        json!({
            "stream": "events.raw",
            "messages": 640,
            "max_messages": 2000,
            "bytes": 0,
            "max_bytes": 0,
            "consumer_count": 1,
            "fill_pct": 32.5,
            "first_seq": 1,
            "last_seq": 640
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "sinex.ingestd",
        "assembly.stats",
        json!({
            "active_assemblies": 3,
            "total_started": 4,
            "total_completed": 2,
            "total_cancelled": 1,
            "total_failed": 0,
            "total_timed_out": 0,
            "avg_duration_ms": 18.0,
            "buffered_slices": 9
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "sinex.node",
        "processing.stats",
        json!({
            "node_type": "terminal-ingestor",
            "events_processed": 40,
            "events_dropped": 2,
            "avg_latency_ms": 5.5,
            "queue_depth": 4,
            "error_count": 1
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "sinex",
        "metric.counter",
        json!({
            "component": "sinex-gateway",
            "name": "requests.total",
            "value": 120,
            "labels": {}
        }),
        None,
    )
    .await?;
    insert_event(
        &ctx,
        "sinex.ingestd",
        "batch.stats",
        json!({
            "batch_size": 16,
            "fetch_to_ack_ms": 48,
            "events_deferred": 2,
            "events_failed": 1,
            "had_synthesis": false,
            "insert_path": "querybuilder",
            "validation_valid": 30,
            "validation_skipped": 1,
            "validation_no_schema": 2,
            "validation_schema_not_found": 0,
            "validation_invalid": 3,
            "validation_coverage_pct": 88.2,
            "suspicious_future_ts_orig": 0
        }),
        None,
    )
    .await?;

    let params = json!({ "from": from, "to": to, "limit": 10 });

    let current_health: TelemetryCurrentHealthResponse = serde_json::from_value(
        handle_telemetry_current_health(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let current_device_state: TelemetryCurrentDeviceStateResponse = serde_json::from_value(
        handle_telemetry_current_device_state(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let gateway_stats: TelemetryGatewayStatsResponse =
        serde_json::from_value(handle_telemetry_gateway_stats(ctx.pool(), params.clone()).await?)?;
    let stream_stats: TelemetryStreamStatsResponse =
        serde_json::from_value(handle_telemetry_stream_stats(ctx.pool(), params.clone()).await?)?;
    let assembly_stats: TelemetryAssemblyStatsResponse =
        serde_json::from_value(handle_telemetry_assembly_stats(ctx.pool(), params.clone()).await?)?;
    let node_stats: TelemetryNodeStatsResponse =
        serde_json::from_value(handle_telemetry_node_stats(ctx.pool(), params.clone()).await?)?;
    let metric_counters: TelemetryMetricCountersResponse = serde_json::from_value(
        handle_telemetry_metric_counters(ctx.pool(), params.clone()).await?,
    )?;
    let ingestd_batch_stats: TelemetryIngestdBatchStatsResponse =
        serde_json::from_value(handle_telemetry_ingestd_batch_stats(ctx.pool(), params).await?)?;

    assert_eq!(current_health.entries.len(), 1);
    assert_eq!(current_health.entries[0].source, "sinex");
    assert_eq!(
        current_health.entries[0].component.as_deref(),
        Some("gateway")
    );
    assert_eq!(current_health.entries[0].status.as_deref(), Some("healthy"));

    assert_eq!(current_device_state.entries.len(), 1);
    assert_eq!(
        current_device_state.entries[0].unit_name.as_deref(),
        Some("sinex-gateway.service")
    );
    assert_eq!(
        current_device_state.entries[0].state.as_deref(),
        Some("active")
    );

    assert_eq!(gateway_stats.buckets.len(), 1);
    assert_eq!(gateway_stats.buckets[0].source, "sinex.gateway.rpc");
    assert_eq!(gateway_stats.buckets[0].stat_events, 1);
    assert_eq!(gateway_stats.buckets[0].avg_total_requests, Some(120.0));
    assert_eq!(gateway_stats.buckets[0].total_rate_limited, Some(7));

    assert_eq!(stream_stats.buckets.len(), 1);
    assert_eq!(
        stream_stats.buckets[0].stream_name.as_deref(),
        Some("events.raw")
    );
    assert_eq!(stream_stats.buckets[0].avg_fill_pct, Some(32.5));
    assert_eq!(stream_stats.buckets[0].max_messages, Some(2000));

    assert_eq!(assembly_stats.buckets.len(), 1);
    assert_eq!(assembly_stats.buckets[0].max_active_assemblies, Some(3));
    assert_eq!(assembly_stats.buckets[0].total_completed, Some(2));
    assert_eq!(assembly_stats.buckets[0].avg_duration_ms, Some(18.0));

    assert_eq!(node_stats.buckets.len(), 1);
    assert_eq!(
        node_stats.buckets[0].node_type.as_deref(),
        Some("terminal-ingestor")
    );
    assert_eq!(node_stats.buckets[0].total_events_processed, Some(40));
    assert_eq!(node_stats.buckets[0].max_queue_depth, Some(4));

    assert_eq!(metric_counters.buckets.len(), 1);
    assert_eq!(
        metric_counters.buckets[0].component.as_deref(),
        Some("sinex-gateway")
    );
    assert_eq!(
        metric_counters.buckets[0].metric_name.as_deref(),
        Some("requests.total")
    );
    assert_eq!(metric_counters.buckets[0].total_value, Some(120));

    assert_eq!(ingestd_batch_stats.buckets.len(), 1);
    assert_eq!(ingestd_batch_stats.buckets[0].avg_batch_size, Some(16.0));
    assert_eq!(ingestd_batch_stats.buckets[0].max_latency_ms, Some(48.0));
    assert_eq!(ingestd_batch_stats.buckets[0].total_failed, Some(1));
    assert_eq!(ingestd_batch_stats.buckets[0].batch_count, 1);

    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_reject_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let error = handle_telemetry_recent_activity(ctx.pool(), json!({ "limit": 0 }))
        .await
        .expect_err("non-positive telemetry limits must be rejected");
    assert!(
        error
            .to_string()
            .contains("telemetry limit must be positive")
    );
    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_reject_inverted_time_ranges(ctx: TestContext) -> TestResult<()> {
    let error = handle_telemetry_window_focus(
        ctx.pool(),
        json!({
            "from": "2026-01-02T00:00:00Z",
            "to": "2026-01-01T00:00:00Z"
        }),
    )
    .await
    .expect_err("inverted telemetry time ranges must be rejected");
    assert!(error.to_string().contains("from' must be strictly earlier"));
    Ok(())
}

#[sinex_test]
async fn telemetry_ingestd_validation_returns_latest_snapshot(ctx: TestContext) -> TestResult<()> {
    let now = time::OffsetDateTime::parse("2026-03-28T03:45:00Z", &Rfc3339)?;
    insert_event(
        &ctx,
        "sinex.ingestd",
        "batch.stats",
        json!({
            "batch_size": 8,
            "fetch_to_ack_ms": 42,
            "events_deferred": 1,
            "events_failed": 0,
            "had_synthesis": true,
            "insert_path": "copy",
            "validation_valid": 20,
            "validation_skipped": 0,
            "validation_no_schema": 2,
            "validation_schema_not_found": 1,
            "validation_invalid": 3,
            "validation_coverage_pct": 87.5,
            "suspicious_future_ts_orig": 4
        }),
        Some(now),
    )
    .await?;

    let response: TelemetryIngestdValidationResponse =
        serde_json::from_value(handle_telemetry_ingestd_validation(ctx.pool(), json!({})).await?)?;
    let snapshot = response
        .snapshot
        .expect("expected latest validation snapshot");
    assert_eq!(snapshot.batch_size, 8);
    assert_eq!(snapshot.fetch_to_ack_ms, 42);
    assert_eq!(snapshot.events_deferred, 1);
    assert_eq!(snapshot.events_failed, 0);
    assert!(snapshot.had_synthesis);
    assert_eq!(snapshot.insert_path, "copy");
    assert_eq!(snapshot.validation_valid, 20);
    assert_eq!(snapshot.validation_no_schema, 2);
    assert_eq!(snapshot.validation_schema_not_found, 1);
    assert_eq!(snapshot.validation_invalid, 3);
    assert_eq!(snapshot.validation_coverage_pct, 87.5);
    assert_eq!(snapshot.suspicious_future_ts_orig, 4);
    Ok(())
}
