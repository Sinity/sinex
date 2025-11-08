#[path = "support/mod.rs"]
mod support;

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_satellite_sdk::replay::{ReplayFilters, ReplayMode, ReplayProgress, ReplayService};
use sinex_test_utils::{prelude::*, sinex_test};
use std::{collections::HashMap, time::Duration as StdDuration};
use support::runtime::TestRuntimeBuilder;
use tokio::time::timeout;

#[sinex_test]
async fn replay_emits_events_through_emitter(ctx: TestContext) -> color_eyre::Result<()> {
    let start_time = Utc::now();

    ctx.create_test_event(
        "terminal-history",
        "command.imported",
        json!({ "command": "echo 'hello world'" }),
    )
    .await?;

    ctx.create_test_event(
        "terminal-history",
        "command.imported",
        json!({ "command": "ls -la" }),
    )
    .await?;

    let support::runtime::TestRuntime {
        runtime,
        mut event_rx,
        nats,
    } = TestRuntimeBuilder::new(&ctx, "terminal-replay-test")
        .with_dry_run(false)
        .with_raw_config(HashMap::new())
        .build()
        .await?;
    let _ = nats.client_url();

    let mut replay_service = ReplayService::from_runtime(
        &runtime,
        ReplayMode::TimeRange {
            start_time: start_time - Duration::minutes(1),
            end_time: Some(start_time + Duration::minutes(1)),
        },
    )
    .with_batch_size(10);

    let replay_result = replay_service
        .replay_into_emitter(runtime.event_emitter(), Option::<fn(&ReplayProgress)>::None)
        .await?;

    assert_eq!(replay_result.total_processed, 2);
    assert!(replay_result.errors.is_empty());

    let first = timeout(StdDuration::from_secs(1), event_rx.recv())
        .await?
        .expect("first replay event");
    assert_eq!(first.event_type.as_str(), "command.imported");

    let second = timeout(StdDuration::from_secs(1), event_rx.recv())
        .await?
        .expect("second replay event");
    assert_eq!(second.event_type.as_str(), "command.imported");

    Ok(())
}

#[sinex_test]
async fn custom_filters_emit_only_matching_events(ctx: TestContext) -> color_eyre::Result<()> {
    let start_time = Utc::now();

    ctx.create_test_event(
        "terminal-history",
        "command.imported",
        json!({ "command": "git status" }),
    )
    .await?;

    ctx.create_test_event(
        "desktop",
        "window.focused",
        json!({ "application": "browser" }),
    )
    .await?;

    let support::runtime::TestRuntime {
        runtime,
        mut event_rx,
        nats,
    } = TestRuntimeBuilder::new(&ctx, "terminal-replay-custom")
        .with_dry_run(false)
        .with_raw_config(HashMap::new())
        .build()
        .await?;
    let _ = nats.client_url();

    let filters = ReplayFilters {
        sources: Some(vec!["terminal-history".to_string()]),
        event_types: Some(vec!["command.imported".to_string()]),
        hosts: None,
        start_time: Some(start_time - Duration::minutes(1)),
        end_time: Some(start_time + Duration::minutes(1)),
        limit: None,
        payload_filters: None,
    };

    let mut replay_service =
        ReplayService::from_runtime(&runtime, ReplayMode::Custom { filters }).with_batch_size(5);

    let replay_result = replay_service
        .replay_into_emitter(runtime.event_emitter(), Option::<fn(&ReplayProgress)>::None)
        .await?;

    assert_eq!(replay_result.total_processed, 1);
    assert!(replay_result.errors.is_empty());

    let event = timeout(StdDuration::from_secs(1), event_rx.recv())
        .await?
        .expect("filtered replay event");

    assert_eq!(event.event_type.as_str(), "command.imported");
    assert_eq!(event.source.as_str(), "terminal-history");

    // Ensure no extra events arrive
    assert!(
        timeout(StdDuration::from_millis(100), event_rx.recv())
            .await
            .ok()
            .flatten()
            .is_none(),
        "only matching events should be replayed"
    );

    Ok(())
}
