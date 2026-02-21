//! End-to-end `JetStream` integration tests using `PipelineScope`.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_node_sdk::{
    AutomatonEventHandler, JetStreamEventConsumer, JetStreamEventConsumerConfig, ProcessingModel,
};
use sinex_primitives::{error::SinexError, temporal};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

#[sinex_test(timeout = 60)]
async fn test_jetstream_e2e_event_flow(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    info!("🚀 Starting E2E JetStream test");

    let sandbox = scope.ctx();
    let env = sandbox.env().clone();
    let namespace = scope.namespace().prefix().to_string();
    let nats_client = sandbox.nats_client();

    let automaton_handler = Arc::new(AutomatonEventHandler::new());
    let automaton_config = JetStreamEventConsumerConfig {
        processing_model: ProcessingModel::StatelessWorker,
        batch_size: 100,
        confirmation_timeout: Duration::from_secs(30),
        consumer_name: format!("test-automaton-{namespace}"),
        enable_provisional_processing: false,
        ..Default::default()
    };
    // Wait for the confirmations stream to exist before starting the automaton consumer.
    // ingestd (started by PipelineScope) creates this stream on startup; the automaton
    // consumer's run() immediately calls js.get_stream() which fails if it doesn't exist.
    let js = async_nats::jetstream::new(nats_client.clone());
    let confirmations_stream = format!(
        "{}_CONFIRMATIONS",
        env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS")
    );
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let stream = confirmations_stream.clone();
            async move { Ok::<bool, SinexError>(js.get_stream(&stream).await.is_ok()) }
        },
        Timeouts::STANDARD,
    )
    .await?;

    // Publish the event FIRST and wait for DB persistence.
    // The automaton consumer uses DeliverPolicy::All, so starting it after the event
    // is already in the stream guarantees it will receive the event on startup.
    // If the consumer starts before any events arrive, its messages() call returns
    // None immediately (no-wait pull semantics) and the consumer task exits.
    let event_id = scope
        .publish(DynamicPayload::new(
            "test-node",
            "test.event",
            json!({
                "message": "E2E JetStream test event",
                "timestamp": temporal::now().format_rfc3339(),
            }),
        ))
        .await?;
    info!(event_id = %event_id, "✅ Event published to JetStream via PipelineScope");

    let automaton_consumer = JetStreamEventConsumer::new_with_namespace(
        nats_client.clone(),
        env.clone(),
        automaton_config,
        automaton_handler.clone(),
        None,
        Some(namespace.clone()),
    );
    let automaton_handle = tokio::spawn(async move { automaton_consumer.run().await });

    WaitHelpers::wait_for_condition(
        || {
            let handler = automaton_handler.clone();
            async move {
                let processed_ids = handler.processed_event_ids().await;
                Ok::<bool, SinexError>(processed_ids.contains(&event_id))
            }
        },
        30,
    )
    .await?;

    let event_from_db = sandbox
        .pool
        .events()
        .get_by_id(event_id)
        .await?
        .expect("event should be persisted");
    assert_eq!(event_from_db.source.as_str(), "test-node");
    assert_eq!(event_from_db.event_type.as_str(), "test.event");

    info!("🎉 E2E JetStream test PASSED");
    info!("   ✓ Node → JetStream (events.raw)");
    info!("   ✓ ingestd → Database persistence");
    info!("   ✓ ingestd → JetStream (events.confirmations)");
    info!("   ✓ Automaton → Confirmed event consumption");

    automaton_handle.abort();
    let _ = automaton_handle.await;
    scope.shutdown().await?;
    Ok(())
}

#[sinex_test]
async fn test_jetstream_idempotency(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    info!("🚀 Starting JetStream idempotency test");

    let sandbox = scope.ctx();

    // Publish twice with the same ID using overrides
    let event_id = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    for i in 1..=2 {
        scope
            .publish_with_overrides(
                DynamicPayload::new(
                    "idempotency-test",
                    "test.duplicate",
                    json!({"test": "idempotency"}),
                ),
                overrides.clone(),
            )
            .await?;
        info!(iteration = i, event_id = %event_id, "Published event");
    }

    scope.wait_for_event_id(event_id.into()).await?;

    let event_count = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid::ulid",
        event_id.as_uuid()
    )
    .fetch_one(&sandbox.pool)
    .await?;
    assert_eq!(
        event_count.count.unwrap_or(0),
        1,
        "Idempotency should yield a single event"
    );

    scope.shutdown().await?;
    Ok(())
}
