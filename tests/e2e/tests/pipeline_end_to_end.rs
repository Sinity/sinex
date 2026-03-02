use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::DynamicPayload;
use sinex_primitives::query::{EventQuery, EventQueryResult, SortDirection};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{DEFAULT_WAIT_SECS, WaitHelpers};

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

    // Use composable query engine to verify events were ingested
    let query = EventQuery {
        sources: vec!["integration-e2e".into()],
        direction: SortDirection::Desc,
        ..Default::default()
    };
    let result = ctx.pool.events().query(query).await?;
    match result {
        EventQueryResult::Events { events: found, .. } => {
            assert!(
                found.len() >= events.len(),
                "composable query should observe staged events"
            );
        }
        _ => panic!("expected Events result variant"),
    }

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
                Ok::<bool, SinexError>(info.state.messages >= expected)
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
