use serde_json::json;
use sinex_core::DynamicPayload;
use sinex_services::AnalyticsService;
use sinex_test_utils::prelude::*;
use sinex_test_utils::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};

#[sinex_test]
async fn pipeline_end_to_end(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let ctx = scope.ctx();

    let events = vec![
        json!({"line": "alpha", "file": "/tmp/e2e.log"}),
        json!({"line": "beta", "file": "/tmp/e2e.log"}),
        json!({"line": "gamma", "file": "/tmp/e2e.log"}),
    ];

    let mut event_ids = Vec::new();
    for payload in &events {
        let id = scope
            .publish(DynamicPayload::new(
                "integration-e2e",
                "log.line",
                payload.clone(),
            ))
            .await?;
        event_ids.push(id);
    }

    scope.wait_for_event_count(events.len()).await?;

    let analytics = AnalyticsService::new(ctx.pool.clone());
    let by_source = analytics.get_event_count_by_source(None, None).await?;
    assert!(
        by_source.values().sum::<i64>() >= events.len() as i64,
        "analytics should observe staged events"
    );

    let jetstream = ctx.jetstream().await?;
    let events_stream = scope.stream("SINEX_RAW_EVENTS");
    let expected = events.len() as u64;
    WaitHelpers::wait_for_condition(
        || {
            let jetstream = jetstream.clone();
            let events_stream = events_stream.clone();
            async move {
                let mut stream = jetstream
                    .get_stream(&events_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok(info.state.messages >= expected)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    for event_id in event_ids {
        scope.wait_for_event_id(event_id).await?;
    }

    scope.shutdown().await?;
    Ok(())
}
