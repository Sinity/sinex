//! End-to-end `JetStream` integration tests using `PipelineScope`.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::JsonValue;
use sinex_primitives::events::builder::EventId;
use sinex_primitives::events::Event;
use sinex_primitives::{error::SinexError, temporal};
use sinexd::runtime::{
    ConfirmedEventCompletion, ConfirmedEventHandler, JetStreamEventConsumer,
    JetStreamEventConsumerConfig, RuntimeResult,
    prelude::async_trait,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::timeout;
use tracing::info;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

#[derive(Default)]
struct TrackingConfirmedEventHandler {
    processed_event_ids: RwLock<Vec<EventId>>,
}

impl TrackingConfirmedEventHandler {
    fn new() -> Self {
        Self::default()
    }

    async fn processed_event_ids(&self) -> Vec<EventId> {
        self.processed_event_ids.read().await.clone()
    }
}

#[async_trait]
impl ConfirmedEventHandler for TrackingConfirmedEventHandler {
    async fn handle_confirmed(
        &self,
        event: &Event<JsonValue>,
        completion: tokio::sync::oneshot::Sender<ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        if let Some(event_id) = event.id {
            self.processed_event_ids.write().await.push(event_id);
        }
        let _ = completion.send(ConfirmedEventCompletion::Safe);
        Ok(())
    }
}

struct DeferredCompletionHandler {
    deliveries: mpsc::Sender<(EventId, oneshot::Sender<ConfirmedEventCompletion>)>,
}

#[async_trait]
impl ConfirmedEventHandler for DeferredCompletionHandler {
    async fn handle_confirmed(
        &self,
        event: &Event<JsonValue>,
        completion: oneshot::Sender<ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        let event_id = event.id.ok_or_else(|| {
            SinexError::validation("confirmed test event unexpectedly had no event id")
        })?;
        self.deliveries
            .send((event_id, completion))
            .await
            .map_err(|_| SinexError::lifecycle("ACK-barrier test receiver dropped"))
    }
}

#[sinex_test(timeout = 60)]
async fn test_jetstream_e2e_event_flow(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    info!("🚀 Starting E2E JetStream test");

    let sandbox = scope.ctx();
    let env = sandbox.env().clone();
    let namespace = scope.namespace().prefix().to_string();
    let nats_client = sandbox.nats_client();

    let automaton_handler = Arc::new(TrackingConfirmedEventHandler::new());
    let automaton_config = JetStreamEventConsumerConfig {
        batch_size: 100,
        consumer_name: format!("test-automaton-{namespace}"),
        liveness_observer: None,
        ..Default::default()
    };
    // Wait for the confirmed-events stream to exist before starting the automaton consumer.
    // event_engine (started by PipelineScope) creates this stream on startup; the automaton
    // consumer's run() immediately calls js.get_stream() which fails if it doesn't exist.
    let js = async_nats::jetstream::new(nats_client.clone());
    let confirmed_events_stream = format!(
        "{}_CONFIRMED",
        env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS")
    );
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let stream = confirmed_events_stream.clone();
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
            "test-source",
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
        Some(namespace.clone()),
    );
    let mut automaton_handle = tokio::spawn(async move { automaton_consumer.run().await });

    // Verify the consumer didn't exit immediately (which would indicate a startup
    // error such as a consumer config mismatch). We race a short timeout against
    // the task handle — if the handle resolves first, it exited prematurely.
    tokio::select! {
        result = &mut automaton_handle => {
            bail!("Automaton consumer task exited unexpectedly: {:?}", result);
        }
        () = tokio::time::sleep(Duration::from_millis(500)) => {
            // Consumer is still running after 500ms — good, proceed.
        }
    }

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
    assert_eq!(event_from_db.source.as_str(), "test-source");
    assert_eq!(event_from_db.event_type.as_str(), "test.event");

    info!("🎉 E2E JetStream test PASSED");
    info!("   ✓ RuntimeModule → JetStream (events.raw)");
    info!("   ✓ event_engine → Database persistence");
    info!("   ✓ event_engine → JetStream (events.confirmed)");
    info!("   ✓ Automaton → Confirmed event consumption");

    automaton_handle.abort();
    let _ = automaton_handle.await;
    scope.shutdown().await?;
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn confirmed_event_ack_waits_for_automaton_completion(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;
    let sandbox = scope.ctx();
    let env = sandbox.env().clone();
    let namespace = scope.namespace().prefix().to_string();
    let nats_client = sandbox.nats_client();
    let consumer_name = format!("ack-barrier-{namespace}");
    let (delivery_tx, mut delivery_rx) = mpsc::channel(1);

    let first_event_id = scope
        .publish(DynamicPayload::new(
            "ack-barrier-source",
            "ack.barrier",
            json!({"message": "safe prefix waits for automaton completion"}),
        ))
        .await?;
    let retry_event_id = scope
        .publish(DynamicPayload::new(
            "ack-barrier-source",
            "ack.barrier",
            json!({"message": "retry suffix must redeliver"}),
        ))
        .await?;

    let handler = Arc::new(DeferredCompletionHandler {
        deliveries: delivery_tx,
    });
    let consumer = JetStreamEventConsumer::new_with_namespace(
        nats_client.clone(),
        env.clone(),
        JetStreamEventConsumerConfig {
            consumer_name: consumer_name.clone(),
            batch_size: 2,
            ..Default::default()
        },
        handler,
        Some(namespace.clone()),
    );
    let mut consumer_handle = tokio::spawn(async move { consumer.run().await });

    let (first_delivered_id, first_completion) = timeout(
        Duration::from_secs(Timeouts::STANDARD),
        delivery_rx.recv(),
    )
    .await?
    .ok_or_else(|| eyre!("confirmed consumer stopped before dispatching the first test event"))?;
    let (second_delivered_id, second_completion) = timeout(
        Duration::from_secs(Timeouts::STANDARD),
        delivery_rx.recv(),
    )
    .await?
    .ok_or_else(|| eyre!("confirmed consumer stopped before dispatching the retry test event"))?;
    assert_eq!(first_delivered_id, first_event_id);
    assert_eq!(second_delivered_id, retry_event_id);

    let confirmed_stream = format!(
        "{}_CONFIRMED",
        env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS")
    );
    WaitHelpers::wait_for_condition(
        || {
            let js = async_nats::jetstream::new(nats_client.clone());
            let stream_name = confirmed_stream.clone();
            let consumer_name = consumer_name.clone();
            async move {
                let stream = js.get_stream(&stream_name).await?;
                let mut consumer = stream
                    .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::service(format!("get consumer: {error}")))?;
                Ok::<bool, SinexError>(consumer.info().await?.num_ack_pending == 2)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    first_completion
        .send(ConfirmedEventCompletion::Safe)
        .map_err(|_| eyre!("confirmed consumer dropped the completion receipt"))?;
    second_completion
        .send(ConfirmedEventCompletion::Retry)
        .map_err(|_| eyre!("confirmed consumer dropped the retry receipt"))?;

    WaitHelpers::wait_for_condition(
        || {
            let js = async_nats::jetstream::new(nats_client.clone());
            let stream_name = confirmed_stream.clone();
            let consumer_name = consumer_name.clone();
            async move {
                let stream = js.get_stream(&stream_name).await?;
                let mut consumer = stream
                    .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::service(format!("get consumer: {error}")))?;
                let info = consumer.info().await?;
                Ok::<bool, SinexError>(info.num_ack_pending == 1 && info.num_redelivered >= 1)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    let (redelivered_id, redelivery_completion) = timeout(
        Duration::from_secs(Timeouts::STANDARD),
        delivery_rx.recv(),
    )
    .await?
    .ok_or_else(|| eyre!("retrying confirmed event was not redelivered"))?;
    assert_eq!(redelivered_id, retry_event_id);
    redelivery_completion
        .send(ConfirmedEventCompletion::Safe)
        .map_err(|_| eyre!("confirmed consumer dropped the redelivery receipt"))?;

    WaitHelpers::wait_for_condition(
        || {
            let js = async_nats::jetstream::new(nats_client.clone());
            let stream_name = confirmed_stream.clone();
            let consumer_name = consumer_name.clone();
            async move {
                let stream = js.get_stream(&stream_name).await?;
                let mut consumer = stream
                    .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::service(format!("get consumer: {error}")))?;
                Ok::<bool, SinexError>(consumer.info().await?.num_ack_pending == 0)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;

    consumer_handle.abort();
    let _ = consumer_handle.await;
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
    let event_id = Uuid::now_v7();
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
        "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid",
        event_id
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
