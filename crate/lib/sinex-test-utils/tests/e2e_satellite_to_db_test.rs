use async_nats::jetstream;
use serde_json::json;
use sinex_ingestd::{
    validator::EventValidator, IngestdResult, JetStreamConsumer, JetStreamTopology,
};
use sinex_test_utils::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

async fn start_ingestd(
    ctx: &TestContext,
    suffix: &str,
) -> TestResult<(
    tokio::task::JoinHandle<IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats = ctx.nats_client();
    let js = jetstream::new(nats.clone());
    let env = ctx.env();
    let stream = env.nats_stream_name(&format!("SINEX_RAW_EVENTS_E2E_{suffix}"));
    let topology = JetStreamTopology::new(&env, stream, format!("ingestd-e2e-{suffix}"));

    let consumer = JetStreamConsumer::new(
        nats.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(EventValidator::new(false))),
        topology.clone(),
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    // Ensure streams exist before publishing.
    timeout(Duration::from_secs(5), async {
        loop {
            if js.get_stream(&topology.events_stream).await.is_ok()
                && js.get_stream(&topology.confirmations_stream).await.is_ok()
                && js.get_stream(&topology.dlq_stream).await.is_ok()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    Ok((handle, js, topology))
}

#[sinex_test]
async fn end_to_end_single_satellite_full_flow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("e2e-{}", uuid::Uuid::new_v4());
    let (handle, js, topology) = start_ingestd(&ctx, &suffix).await?;

    let mut confirmations = ctx
        .nats_client()
        .subscribe(ctx.env().nats_subject("events.confirmations.>"))
        .await?;

    let publisher = TestSatellitePublisher::new(ctx.nats_client(), format!("satellite.{suffix}"));
    let mut ids = Vec::new();
    for idx in 0..25u32 {
        let id = publisher
            .publish_event("e2e.event", json!({ "seq": idx, "note": "end-to-end" }))
            .await?;
        ids.push(id);
    }

    // Wait for persistence.
    timeout(Duration::from_secs(20), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source = $1")
                    .bind(format!("satellite.{suffix}"))
                    .fetch_one(&ctx.pool)
                    .await?;
            if count == 25 {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    // Confirmations observed for all events from the wildcard subscription.
    use std::collections::HashSet;
    let expected: HashSet<_> = ids.iter().map(|id| id.to_string()).collect();
    let mut seen: HashSet<String> = HashSet::new();
    timeout(Duration::from_secs(10), async {
        while seen.len() < expected.len() {
            if let Some(msg) = confirmations.next().await {
                let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
                if let Some(id) = payload.get("event_id").and_then(|v| v.as_str()) {
                    if expected.contains(id) {
                        seen.insert(id.to_string());
                    }
                }
            } else {
                bail!("confirmation stream closed unexpectedly");
            }
        }
        Ok::<_, color_eyre::Report>(())
    })
    .await??;

    // DLQ remains empty.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(
        dlq_state.messages, 0,
        "DLQ should be empty in e2e happy path"
    );

    handle.abort();
    Ok(())
}
