//! End-to-end `JetStream` integration tests using `PipelineScope`.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::JsonValue;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::EventId;
use sinex_primitives::{error::SinexError, temporal};
use sinexd::runtime::{
    ConfirmedEventCompletion, ConfirmedEventHandler, JetStreamEventConsumer,
    JetStreamEventConsumerConfig, RuntimeResult, prelude::async_trait,
};
use std::os::unix::process::ExitStatusExt;
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

const PIPELINE_CRASH_NATS: &str = "SINEX_PIPELINE_CRASH_NATS";
const PIPELINE_CRASH_NAMESPACE: &str = "SINEX_PIPELINE_CRASH_NAMESPACE";
const PIPELINE_CRASH_CONSUMER: &str = "SINEX_PIPELINE_CRASH_CONSUMER";
const PIPELINE_CRASH_EVENT: &str = "SINEX_PIPELINE_CRASH_EVENT";
const PIPELINE_CRASH_MARKER: &str = "SINEX_PIPELINE_CRASH_MARKER";

/// Admission and database persistence precede the real consumer-process kill.
/// Unlike the direct confirmed-stream control, this uses the normal raw pipeline.
#[sinex_test(timeout = 90)]
async fn confirmed_event_redelivers_after_consumer_process_death() -> Result<()> {
    if let Ok(nats_url) = std::env::var(PIPELINE_CRASH_NATS) {
        let namespace = std::env::var(PIPELINE_CRASH_NAMESPACE)?;
        let consumer_name = std::env::var(PIPELINE_CRASH_CONSUMER)?;
        let expected = std::env::var(PIPELINE_CRASH_EVENT)?;
        let marker = std::env::var(PIPELINE_CRASH_MARKER)?;
        let client = async_nats::connect(&nats_url).await?;
        let env = SinexEnvironment::new("dev")?;
        let stream_name = format!(
            "{}_CONFIRMED",
            env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS")
        );
        let js = async_nats::jetstream::new(client.clone());
        let (delivery_tx, mut delivery_rx) = mpsc::channel(1);
        let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
            client,
            env,
            JetStreamEventConsumerConfig {
                consumer_name: consumer_name.clone(),
                batch_size: 1,
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                liveness_check_interval: Duration::from_secs(60),
                ..Default::default()
            },
            Arc::new(DeferredCompletionHandler {
                deliveries: delivery_tx,
            }),
            Some(namespace),
        ));
        let (ready_tx, ready_rx) = oneshot::channel();
        let runner = consumer.clone();
        let _runner =
            tokio::spawn(async move { runner.run_with_ready_signal(Some(ready_tx)).await });
        timeout(Duration::from_secs(10), ready_rx).await??;
        let (_event_id, _unsettled_receipt) = timeout(Duration::from_secs(10), async {
            loop {
                let (id, receipt) = delivery_rx
                    .recv()
                    .await
                    .ok_or_else(|| eyre!("child delivery channel closed"))?;
                if id.to_string() == expected {
                    return Ok::<_, color_eyre::eyre::Report>((id, receipt));
                }
                receipt
                    .send(ConfirmedEventCompletion::Safe)
                    .map_err(|_| eyre!("child consumer dropped unrelated completion"))?;
            }
        })
        .await??;
        let stream = js.get_stream(&stream_name).await?;
        let mut durable = stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
            .await
            .map_err(|error| eyre!("child durable lookup failed: {error}"))?;
        timeout(Duration::from_secs(5), async {
            loop {
                if durable.info().await?.num_ack_pending == 1 {
                    return Ok::<_, color_eyre::eyre::Report>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        tokio::fs::write(marker, expected.as_bytes()).await?;
        // Keep the exact delivery and completion receipt alive until the parent
        // kills this process. The child does not construct a database sandbox.
        std::future::pending::<()>().await;
        return Ok(());
    }

    let ctx = TestContext::with_name("confirmed_event_redelivers_after_consumer_process_death")
        .await?
        .with_nats()
        .shared()
        .await?;
    let scope = ctx.pipeline().await?;
    let sandbox = scope.ctx();
    let env = sandbox.env().clone();
    let namespace = scope.namespace().prefix().to_string();
    let consumer_name = format!("pipeline-process-death-{namespace}");
    let event_id = scope
        .publish(DynamicPayload::new(
            "pipeline-process-death-source",
            "pipeline.process_death",
            json!({"test": "persisted-unsettled-delivery"}),
        ))
        .await?;
    let persisted = sandbox
        .pool()
        .events()
        .get_by_id(event_id)
        .await?
        .ok_or_else(|| eyre!("admitted event was not persisted before consumer death"))?;
    assert_eq!(persisted.id, Some(event_id));
    assert!(persisted.provenance.source_material_id().is_some());

    let marker_dir = tempfile::tempdir()?;
    let marker = marker_dir.path().join("unsettled-delivery");
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let test_name = format!("{module}::confirmed_event_redelivers_after_consumer_process_death");
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(PIPELINE_CRASH_NATS, sandbox.nats_handle()?.client_url())
        .env(PIPELINE_CRASH_NAMESPACE, &namespace)
        .env(PIPELINE_CRASH_CONSUMER, &consumer_name)
        .env(PIPELINE_CRASH_EVENT, event_id.to_string())
        .env(PIPELINE_CRASH_MARKER, &marker)
        .kill_on_drop(true)
        .spawn()?;
    let held = timeout(Duration::from_secs(25), async {
        loop {
            if tokio::fs::try_exists(&marker).await? {
                return Ok::<_, std::io::Error>(());
            }
            if let Some(status) = child.try_wait()? {
                return Err(std::io::Error::other(format!(
                    "child exited before holding delivery: {status}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if !matches!(held, Ok(Ok(()))) {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(eyre!("child did not reach unsettled delivery: {held:?}"));
    }
    assert_eq!(
        tokio::fs::read_to_string(&marker).await?,
        event_id.to_string()
    );
    child.kill().await?;
    assert_eq!(
        child.wait().await?.signal(),
        Some(9),
        "consumer must die from the parent's SIGKILL"
    );

    let client = sandbox.nats_client();
    let stream_name = format!(
        "{}_CONFIRMED",
        env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS")
    );
    let (delivery_tx, mut delivery_rx) = mpsc::channel(1);
    let restarted = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env,
        JetStreamEventConsumerConfig {
            consumer_name: consumer_name.clone(),
            batch_size: 1,
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
            liveness_check_interval: Duration::from_secs(60),
            ..Default::default()
        },
        Arc::new(DeferredCompletionHandler {
            deliveries: delivery_tx,
        }),
        Some(namespace),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let runner = restarted.clone();
    let runner_task =
        tokio::spawn(async move { runner.run_with_ready_signal(Some(ready_tx)).await });
    timeout(Duration::from_secs(10), ready_rx).await??;
    // The production consumer uses its unchanged 30-second ACK deadline.
    let (redelivered, receipt) = timeout(Duration::from_secs(40), delivery_rx.recv())
        .await?
        .ok_or_else(|| eyre!("restarted consumer stopped before redelivery"))?;
    assert_eq!(redelivered, event_id);
    let js = async_nats::jetstream::new(client);
    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| eyre!("restarted durable lookup failed: {error}"))?;
    assert!(durable.info().await?.num_redelivered >= 1);
    receipt
        .send(ConfirmedEventCompletion::Safe)
        .map_err(|_| eyre!("restarted consumer dropped completion receipt"))?;
    timeout(Duration::from_secs(5), async {
        loop {
            if durable.info().await?.num_ack_pending == 0 {
                return Ok::<_, color_eyre::eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let after = sandbox
        .pool()
        .events()
        .get_by_id(event_id)
        .await?
        .ok_or_else(|| eyre!("persisted interpretation vanished after consumer recovery"))?;
    assert_eq!(after.id, persisted.id);
    assert_eq!(after.provenance, persisted.provenance);
    assert_eq!(after.payload, persisted.payload);
    restarted.stop().await;
    runner_task.await??;
    scope.shutdown().await?;
    Ok(())
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

    let (first_delivered_id, first_completion) =
        timeout(Duration::from_secs(Timeouts::STANDARD), delivery_rx.recv())
            .await?
            .ok_or_else(|| {
                eyre!("confirmed consumer stopped before dispatching the first test event")
            })?;
    let (second_delivered_id, second_completion) =
        timeout(Duration::from_secs(Timeouts::STANDARD), delivery_rx.recv())
            .await?
            .ok_or_else(|| {
                eyre!("confirmed consumer stopped before dispatching the retry test event")
            })?;
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

    let (redelivered_id, redelivery_completion) =
        timeout(Duration::from_secs(Timeouts::STANDARD), delivery_rx.recv())
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
