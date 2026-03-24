//! Regression coverage for telemetry RPC handlers against the live read-model schema.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{
    handle_telemetry_command_frequency, handle_telemetry_file_activity,
    handle_telemetry_recent_activity, handle_telemetry_system_state,
    handle_telemetry_window_focus,
};
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::telemetry::{
    TelemetryCommandFrequencyResponse, TelemetryFileActivityResponse,
    TelemetryRecentActivityResponse, TelemetrySystemStateResponse,
    TelemetryWindowFocusResponse,
};
use xtask::sandbox::prelude::*;

async fn insert_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<()> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    ctx.pool()
        .events()
        .insert(
            DynamicPayload::new(source, event_type, payload)
                .from_material(material_id)
                .build()?,
        )
        .await?;
    Ok(())
}

async fn refresh_telemetry_views(ctx: &TestContext) -> TestResult<()> {
    for view in [
        "sinex_telemetry.current_window_focus",
        "sinex_telemetry.command_frequency_hourly",
        "sinex_telemetry.file_activity_summary",
        "sinex_telemetry.current_system_state",
    ] {
        sqlx::query("CALL refresh_continuous_aggregate($1, NULL, NULL)")
            .bind(view)
            .execute(ctx.pool())
            .await?;
    }
    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_follow_current_read_model_schema(ctx: TestContext) -> TestResult<()> {
    insert_event(
        &ctx,
        "desktop.hyprland",
        "focus.window",
        json!({
            "workspace": "code",
            "window_class": "foot",
            "window_title": "cargo test",
            "window_id": "0xabc",
        }),
    )
    .await?;
    insert_event(
        &ctx,
        "terminal.zsh",
        "shell.command",
        json!({
            "command": "cargo test",
            "shell": "zsh",
            "exit_code": 0,
            "duration_ms": 123.0,
        }),
    )
    .await?;
    insert_event(
        &ctx,
        "fs-watcher",
        "file.created",
        json!({
            "path": "/tmp/telemetry-handlers/report.md",
        }),
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
    )
    .await?;

    refresh_telemetry_views(&ctx).await?;

    let window_focus: TelemetryWindowFocusResponse = serde_json::from_value(
        handle_telemetry_window_focus(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let command_frequency: TelemetryCommandFrequencyResponse = serde_json::from_value(
        handle_telemetry_command_frequency(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let file_activity: TelemetryFileActivityResponse = serde_json::from_value(
        handle_telemetry_file_activity(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let recent_activity: TelemetryRecentActivityResponse = serde_json::from_value(
        handle_telemetry_recent_activity(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;
    let system_state: TelemetrySystemStateResponse = serde_json::from_value(
        handle_telemetry_system_state(ctx.pool(), json!({ "limit": 10 })).await?,
    )?;

    assert_eq!(window_focus.buckets.len(), 1);
    let focus = &window_focus.buckets[0];
    assert_eq!(focus.workspace.as_deref(), Some("code"));
    assert_eq!(focus.window_class.as_deref(), Some("foot"));
    assert_eq!(focus.window_title.as_deref(), Some("cargo test"));
    assert_eq!(focus.window_id.as_deref(), Some("0xabc"));
    assert_eq!(focus.focus_event_count, 1);
    assert!(focus.last_focus_time.is_some());

    assert_eq!(command_frequency.entries.len(), 1);
    let command = &command_frequency.entries[0];
    assert_eq!(command.command, "cargo test");
    assert_eq!(command.shell.as_deref(), Some("zsh"));
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
                && entry.context.as_deref() == Some("code")
                && entry.detail.as_deref() == Some("foot"))
    );
    assert!(
        recent_activity
            .entries
            .iter()
            .any(|entry| entry.activity_type == "system_load"
                && entry.context.as_deref() == Some("cpu"))
    );
    assert!(
        recent_activity
            .entries
            .iter()
            .any(|entry| entry.activity_type == "command_execution"
                && entry.context.as_deref() == Some("zsh")
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
