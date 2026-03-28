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
    ctx.pool()
        .events()
        .insert(event)
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
        "desktop.hyprland",
        "focus.window",
        json!({
            "workspace": "code",
            "window_class": "foot",
            "window_title": "cargo test",
            "window_id": "0xabc",
        }),
        None,
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

    let window_focus: TelemetryWindowFocusResponse = serde_json::from_value(
        handle_telemetry_window_focus(ctx.pool(), params.clone()).await?,
    )?;
    let command_frequency: TelemetryCommandFrequencyResponse = serde_json::from_value(
        handle_telemetry_command_frequency(ctx.pool(), params.clone()).await?,
    )?;
    let file_activity: TelemetryFileActivityResponse = serde_json::from_value(
        handle_telemetry_file_activity(ctx.pool(), params.clone()).await?,
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

#[sinex_test]
async fn telemetry_handlers_bucket_activity_by_event_time(ctx: TestContext) -> TestResult<()> {
    sinex_schema::apply::apply(ctx.pool()).await?;

    let imported_at = time::OffsetDateTime::parse("2026-01-02T03:15:00Z", &Rfc3339)?;

    insert_event(
        &ctx,
        "desktop.hyprland",
        "focus.window",
        json!({
            "workspace": "retro",
            "window_class": "kitty",
            "window_title": "imported focus",
            "window_id": "0x42",
        }),
        Some(imported_at),
    )
    .await?;
    insert_event(
        &ctx,
        "terminal.zsh",
        "shell.command",
        json!({
            "command": "git status",
            "shell": "zsh",
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

    let window_focus: TelemetryWindowFocusResponse = serde_json::from_value(
        handle_telemetry_window_focus(ctx.pool(), params.clone()).await?,
    )?;
    let command_frequency: TelemetryCommandFrequencyResponse = serde_json::from_value(
        handle_telemetry_command_frequency(ctx.pool(), params.clone()).await?,
    )?;
    let file_activity: TelemetryFileActivityResponse = serde_json::from_value(
        handle_telemetry_file_activity(ctx.pool(), params.clone()).await?,
    )?;
    let system_state: TelemetrySystemStateResponse = serde_json::from_value(
        handle_telemetry_system_state(ctx.pool(), params).await?,
    )?;

    assert_eq!(window_focus.buckets.len(), 1);
    assert_eq!(window_focus.buckets[0].workspace.as_deref(), Some("retro"));
    assert_eq!(window_focus.buckets[0].window_class.as_deref(), Some("kitty"));

    assert_eq!(command_frequency.entries.len(), 1);
    assert_eq!(command_frequency.entries[0].command, "git status");
    assert_eq!(command_frequency.entries[0].total_executions, 1);

    assert_eq!(file_activity.entries.len(), 1);
    assert_eq!(file_activity.entries[0].directory.as_deref(), Some("/tmp/imported"));
    assert_eq!(file_activity.entries[0].event_type, "file.modified");

    assert_eq!(system_state.buckets.len(), 1);
    assert_eq!(system_state.buckets[0].current_active_units, Some(7));
    assert_eq!(system_state.buckets[0].sample_count, 2);

    Ok(())
}

#[sinex_test]
async fn telemetry_handlers_reject_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let error = handle_telemetry_recent_activity(ctx.pool(), json!({ "limit": 0 }))
        .await
        .expect_err("non-positive telemetry limits must be rejected");
    assert!(error.to_string().contains("telemetry limit must be positive"));
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
    assert!(error.to_string().contains("from' must be earlier"));
    Ok(())
}
