use chrono::{Duration as ChronoDuration, Utc};
use color_eyre::eyre::eyre;
use serde_json::{json, Value as JsonValue};
use sinex_core::db::DbPoolExt;
use sinex_core::types::events::DynamicPayload;
use sinex_core::types::Id;
use sinex_processor_runtime::replay::{ReplayMode, ReplayProgress, ReplayRuntimeExt};
use sinex_test_utils::{sinex_test, TestContext, TestRuntimeBuilder};
use tokio::time::{timeout, Duration};

#[sinex_test]
async fn replay_runtime_service_emits_events(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().await?;
    publish_event(
        &ctx,
        "runtime-replay",
        "file.created",
        json!({"path": "/tmp/replay.txt"}),
    )
    .await?;

    let test_runtime = TestRuntimeBuilder::new(&ctx, "runtime-replay")
        .build()
        .await?;
    let mut event_rx = test_runtime.event_rx;
    let runtime = test_runtime.runtime;

    let start = Utc::now() - ChronoDuration::hours(1);
    let end = Utc::now() + ChronoDuration::minutes(1);
    let mut replay_service = runtime.replay_service(ReplayMode::Source {
        source: "runtime-replay".to_string(),
        start_time: Some(start),
        end_time: Some(end),
    });

    let result = replay_service
        .replay_into_emitter(runtime.event_emitter(), Option::<fn(&ReplayProgress)>::None)
        .await?;
    assert!(result.total_processed >= 1);

    let replayed = timeout(Duration::from_secs(2), async {
        loop {
            match event_rx.recv().await {
                Some(event) if event.source.as_str() == "runtime-replay" => return Ok(event),
                Some(_) => continue,
                None => return Err(eyre!("No replay events received")),
            }
        }
    })
    .await??;

    assert_eq!(replayed.event_type.as_str(), "file.created");
    Ok(())
}

async fn publish_event(
    ctx: &TestContext,
    source: &str,
    event_type: &str,
    payload: JsonValue,
) -> color_eyre::Result<()> {
    ctx.publish(DynamicPayload::new(source, event_type, payload))
        .await?;
    Ok(())
}
