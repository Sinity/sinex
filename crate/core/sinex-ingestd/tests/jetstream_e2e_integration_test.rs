//! End-to-end JetStream integration tests using PipelineScope.

use serde_json::json;
use sinex_core::DbPoolExt;
use sinex_node_sdk::{
    AutomatonEventHandler, JetStreamEventConsumer, JetStreamEventConsumerConfig, ProcessingModel,
};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{WaitHelpers, DEFAULT_WAIT_SECS};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[sinex_test(timeout = 60)]
async fn test_jetstream_e2e_event_flow(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    info!("🚀 Starting E2E JetStream test");

    let ctx = scope.ctx();
    let env = ctx.env().clone();
    let namespace = scope.namespace().prefix().to_string();
    let nats_client = ctx.nats_client();

    let automaton_handler = Arc::new(AutomatonEventHandler::new());
    let automaton_config = JetStreamEventConsumerConfig {
        processing_model: ProcessingModel::StatelessWorker,
        batch_size: 100,
        confirmation_timeout: Duration::from_secs(30),
        consumer_name: format!("test-automaton-{namespace}"),
        enable_provisional_processing: false,
        ..Default::default()
    };
    let automaton_consumer = JetStreamEventConsumer::new_with_namespace(
        nats_client.clone(),
        env.clone(),
        automaton_config,
        automaton_handler.clone(),
        None,
        Some(namespace.clone()),
    );
    let automaton_handle = tokio::spawn(async move { automaton_consumer.run().await });

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test-node",
        Some(namespace.clone()),
    );
    let event_id = publisher
        .publish(
            "test.event",
            json!({
                "message": "E2E JetStream test event",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        )
        .await?;
    info!(event_id = %event_id, "✅ Event published to JetStream via TestNodePublisher");

    scope.wait_for_event_id(event_id.into()).await?;

    WaitHelpers::wait_for_condition(
        || {
            let handler = automaton_handler.clone();
            let event_id = event_id;
            async move {
                let processed_ids = handler.processed_event_ids().await;
                Ok(processed_ids.contains(&event_id.into()))
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    let event_from_db = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
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

    let ctx = scope.ctx();
    let namespace = scope.namespace().prefix().to_string();
    let nats_client = ctx.nats_client();

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "idempotency-test",
        Some(namespace.clone()),
    );
    let event_id = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    for i in 1..=2 {
        publisher
            .publish_with_overrides(
                "test.duplicate",
                json!({"test": "idempotency"}),
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
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(
        event_count.count.unwrap_or(0),
        1,
        "Idempotency should yield a single event"
    );

    scope.shutdown().await?;
    Ok(())
}
