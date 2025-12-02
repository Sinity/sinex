#[path = "support/mod.rs"]
mod support;

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_satellite_sdk::replay::{ReplayFilters, ReplayMode, ReplayProgress, ReplayService};
use sinex_test_utils::{prelude::*, sinex_test};
use std::{collections::HashMap, time::Duration as StdDuration};
use support::runtime::TestRuntimeBuilder;
use tokio::time::timeout;

#[sinex_test]
async fn replay_emits_events_through_emitter(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let start_time = Utc::now();
    let source = format!("terminal-history-{}", Ulid::new());

    ctx.create_test_event(
        &source,
        "command.imported",
        json!({ "command": "echo 'hello world'" }),
    )
    .await?;

    ctx.create_test_event(&source, "command.imported", json!({ "command": "ls -la" }))
        .await?;

    let total_seeded = ctx.pool.events().count_all().await?;
    assert!(
        total_seeded >= 2,
        "Expected at least 2 seeded events before replay, saw {total_seeded}"
    );

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

    let first = timeout(StdDuration::from_secs(5), event_rx.recv())
        .await?
        .expect("first replay event");
    assert_eq!(first.event_type.as_str(), "command.imported");

    let second = timeout(StdDuration::from_secs(5), event_rx.recv())
        .await?
        .expect("second replay event");
    assert_eq!(second.event_type.as_str(), "command.imported");

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;

    Ok(())
}

#[sinex_test]
async fn custom_filters_emit_only_matching_events(ctx: TestContext) -> color_eyre::Result<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.force_cleanup().await?;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    let start_time = Utc::now();
    let run_id = Ulid::new();

    ctx.create_test_event(
        &format!("terminal-history-{run_id}"),
        "command.imported",
        json!({ "command": "git status" }),
    )
    .await?;

    sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &format!("terminal-history-{run_id}"),
        1,
        20,
    )
    .await?;

    ctx.create_test_event(
        &format!("desktop-{run_id}"),
        "window.focused",
        json!({ "application": "browser" }),
    )
    .await?;

    for (source, expected) in [
        (format!("terminal-history-{run_id}"), 1usize),
        (format!("desktop-{run_id}"), 1usize),
    ] {
        if let Err(err) = sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
            &ctx.pool,
            &source,
            expected,
            24,
        )
        .await
        {
            tracing::warn!(%source, %expected, error = %err, "Replay filter wait timed out; reseeding once");
            ctx.create_test_event(
                &source,
                if source.contains("terminal") {
                    "command.imported"
                } else {
                    "window.focused"
                },
                json!({ "note": "reseed" }),
            )
            .await?;
            sinex_test_utils::timing_utils::WaitHelpers::wait_for_source_events(
                &ctx.pool,
                &source,
                expected + 1,
                24,
            )
            .await?;
        }
    }

    // Normalize anchor bytes for inserted events to keep provenance decoding stable during replay.
    sqlx::query(
        r#"
        UPDATE core.events
        SET anchor_byte = COALESCE(anchor_byte, 0)
        WHERE source = $1 OR source = $2
        "#,
    )
    .bind(format!("terminal-history-{run_id}"))
    .bind(format!("desktop-{run_id}"))
    .execute(&ctx.pool)
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
        sources: Some(vec![format!("terminal-history-{run_id}")]),
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

    let event = timeout(StdDuration::from_secs(5), event_rx.recv())
        .await?
        .expect("filtered replay event");

    assert_eq!(event.event_type.as_str(), "command.imported");
    assert_eq!(event.source.as_str(), format!("terminal-history-{run_id}"));

    // Ensure no extra events arrive
    assert!(
        timeout(StdDuration::from_millis(100), event_rx.recv())
            .await
            .ok()
            .flatten()
            .is_none(),
        "only matching events should be replayed"
    );

    sinex_test_utils::db_common::reset_database(&ctx.pool).await?;
    sinex_test_utils::db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;

    Ok(())
}
