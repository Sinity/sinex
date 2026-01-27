use async_nats::jetstream;
use serde_json::json;
use sinex_ingestd::{
    validator::EventValidator, IngestdResult, JetStreamConsumer, JetStreamTopology,
};
use xtask::sandbox::timing::{Timeouts, WaitHelpers};
use xtask::sandbox::{prelude::*, TestNodePublisher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

async fn start_ingestd(
    ctx: &TestContext,
    suffix: &str,
) -> TestResult<(
    tokio::task::JoinHandle<IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let stream_base = format!("SINEX_RAW_EVENTS_E2E_{suffix}");
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let stream = ctx.pipeline_namespace().stream(&stream_base);
    let topology = JetStreamTopology::new(
        &env,
        stream,
        ctx.pipeline_namespace()
            .consumer_name(&format!("ingestd-e2e-{suffix}")),
        Some(&namespace),
    );

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        ctx.pool.clone(),
        Arc::new(RwLock::new(EventValidator::new(false))),
        topology.clone(),
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    // Ensure streams exist before publishing.
    let stream_timeout = Duration::from_secs(Timeouts::QUICK);
    timeout(stream_timeout, async {
        nats.wait_for_stream(&js, &topology.events_stream, stream_timeout)
            .await?;
        nats.wait_for_stream(&js, &topology.confirmations_stream, stream_timeout)
            .await?;
        nats.wait_for_stream(&js, &topology.dlq_stream, stream_timeout)
            .await
    })
    .await??;

    Ok((handle, js, topology))
}

#[sinex_test]
async fn end_to_end_single_node_full_flow(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let suffix = format!("e2e-{}", uuid::Uuid::new_v4());
    let (handle, js, topology) = start_ingestd(&ctx, &suffix).await?;

    let mut confirmation_sub = nats_client
        .subscribe(topology.confirmations_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        format!("node.{suffix}"),
        Some(namespace),
    );
    let mut ids = Vec::new();
    for idx in 0..25u32 {
        let id = publisher
            .publish("e2e.event", json!({ "seq": idx, "note": "end-to-end" }))
            .await?;
        ids.push(id);
    }

    // Wait for persistence.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let source = format!("node.{suffix}");
            async move {
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source = $1")
                        .bind(source)
                        .fetch_one(&pool)
                        .await?;
                Ok(count == 25)
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    // Confirmations observed for all events from the wildcard subscription.
    use std::collections::HashSet;
    let expected: HashSet<_> = ids.iter().map(|id| id.to_string()).collect();
    let mut seen: HashSet<String> = HashSet::new();
    timeout(Duration::from_secs(Timeouts::SHORT), async {
        while seen.len() < expected.len() {
            if let Some(msg) = confirmation_sub.next().await {
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