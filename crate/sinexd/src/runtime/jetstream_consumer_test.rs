use super::{
    ConfirmedConsumerRetirementAction, JetStreamEventConsumerConfig, confirmed_filter_subject_for,
};
use crate::runtime::automaton::traits::InputProvenanceFilter;
use crate::runtime::{ConfirmedEventHandler, JetStreamEventConsumer, RuntimeResult, SelfObserver};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_trait::async_trait;
use serde_json::json;
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::events::payload::DynamicPayload;
use sinex_primitives::events::{Event, SourceMaterial};
use sinex_primitives::{Id, JsonValue, Uuid};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use xtask::sandbox::sinex_test;

struct BlockingConfirmedHandler {
    first_started: Notify,
    release_first: Notify,
    calls: Mutex<usize>,
}

impl BlockingConfirmedHandler {
    fn new() -> Self {
        Self {
            first_started: Notify::new(),
            release_first: Notify::new(),
            calls: Mutex::new(0),
        }
    }
}

#[async_trait]
impl ConfirmedEventHandler for BlockingConfirmedHandler {
    async fn handle_confirmed(
        &self,
        _event: &Event<JsonValue>,
        completion: oneshot::Sender<crate::runtime::ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        let call = {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            *calls
        };
        if call == 1 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        completion
            .send(crate::runtime::ConfirmedEventCompletion::Safe)
            .map_err(|_| crate::runtime::SinexError::lifecycle("completion receiver dropped"))?;
        Ok(())
    }
}

struct DeferredConfirmedHandler {
    completions: mpsc::Sender<oneshot::Sender<crate::runtime::ConfirmedEventCompletion>>,
}

struct IdentifiedDeferredConfirmedHandler {
    completions: mpsc::Sender<(
        Option<Id<Event>>,
        oneshot::Sender<crate::runtime::ConfirmedEventCompletion>,
    )>,
}

#[async_trait]
impl ConfirmedEventHandler for DeferredConfirmedHandler {
    async fn handle_confirmed(
        &self,
        _event: &Event<JsonValue>,
        completion: oneshot::Sender<crate::runtime::ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        self.completions
            .send(completion)
            .await
            .map_err(|_| crate::runtime::SinexError::lifecycle("test completion receiver dropped"))
    }
}

#[async_trait]
impl ConfirmedEventHandler for IdentifiedDeferredConfirmedHandler {
    async fn handle_confirmed(
        &self,
        event: &Event<JsonValue>,
        completion: oneshot::Sender<crate::runtime::ConfirmedEventCompletion>,
    ) -> RuntimeResult<()> {
        self.completions
            .send((event.id.clone(), completion))
            .await
            .map_err(|_| crate::runtime::SinexError::lifecycle("test completion receiver dropped"))
    }
}

#[sinex_test]
async fn default_consumer_config_targets_confirmed_firehose() -> xtask::sandbox::TestResult<()> {
    let config = JetStreamEventConsumerConfig::default();
    assert!(config.event_type_filters.is_empty());
    assert_eq!(config.deliver_policy, DeliverPolicy::All);
    Ok(())
}

#[sinex_test]
async fn confirmed_filter_subject_composes_provenance_and_type() -> xtask::sandbox::TestResult<()> {
    let env = SinexEnvironment::new("dev")?;

    assert_eq!(
        confirmed_filter_subject_for(&env, None, InputProvenanceFilter::Any, None),
        "dev.events.confirmed.>"
    );
    assert_eq!(
        confirmed_filter_subject_for(&env, None, InputProvenanceFilter::MaterialOnly, None),
        "dev.events.confirmed.material.>"
    );
    assert_eq!(
        confirmed_filter_subject_for(
            &env,
            None,
            InputProvenanceFilter::SynthesizedOnly,
            Some("entity.resolved")
        ),
        "dev.events.confirmed.synthesized.*.entity_d_resolved"
    );
    assert_eq!(
        confirmed_filter_subject_for(
            &env,
            Some("agent"),
            InputProvenanceFilter::Any,
            Some("command.executed")
        ),
        "dev.agent.events.confirmed.*.*.command_d_executed"
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_filter_subjects_compose_multiple_event_types() -> xtask::sandbox::TestResult<()>
{
    let env = SinexEnvironment::new("dev")?;
    let filters = super::confirmed_filter_subjects_for(
        &env,
        None,
        InputProvenanceFilter::MaterialOnly,
        &[
            "command.executed".to_string(),
            "command.canonical".to_string(),
        ],
    );

    assert_eq!(
        filters,
        vec![
            "dev.events.confirmed.material.*.command_d_executed",
            "dev.events.confirmed.material.*.command_d_canonical",
        ]
    );
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn confirmed_consumer_stops_on_real_retention_gap(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-gap-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        discard: async_nats::jetstream::stream::DiscardPolicy::Old,
        max_messages: 2,
        max_age: Duration::from_millis(100),
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let handler = Arc::new(BlockingConfirmedHandler::new());
    let consumer = JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env.clone(),
        super::JetStreamEventConsumerConfig {
            batch_size: 1,
            consumer_name: format!("gap-consumer-{}", Uuid::now_v7()),
            deliver_policy: DeliverPolicy::All,
            liveness_check_interval: Duration::from_millis(20),
            liveness_observer: Some(Arc::new(SelfObserver::disabled())),
            ..Default::default()
        },
        handler.clone(),
        Some(namespace.clone()),
    );
    let (ready_tx, ready_rx) = oneshot::channel();
    let consumer_task =
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await });
    let ready_result = tokio::time::timeout(Duration::from_secs(3), ready_rx).await?;
    if ready_result.is_err() {
        let startup_result = consumer_task.await?;
        panic!("consumer failed before ready: {startup_result:?}");
    }

    let event = DynamicPayload::new(
        "confirmed-gap-test",
        "confirmed.gap",
        json!({"test": "retention-gap"}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;
    let payload = serde_json::to_vec(&event)?;
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-gap-test.confirmed.gap",
    );
    js.publish(publish_subject.clone(), payload.clone().into())
        .await?
        .await?;
    tokio::time::timeout(Duration::from_secs(3), handler.first_started.notified()).await?;
    // Let the delivered-but-unacked first message age out, then publish enough
    // new messages to advance the retained first sequence past the gap.
    tokio::time::sleep(Duration::from_millis(300)).await;
    for _ in 0..3 {
        js.publish(publish_subject.clone(), payload.clone().into())
            .await?
            .await?;
    }

    handler.release_first.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(5), consumer_task)
        .await??
        .expect_err("consumer must stop so the supervisor can run historical catch-up");
    assert!(error.to_string().contains("retention gap"));

    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn confirmed_consumer_waits_for_runtime_completion_before_ack(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-ack-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let consumer_name = format!("ack-barrier-{}", Uuid::now_v7());
    let (completion_tx, mut completion_rx) = mpsc::channel(1);
    let handler = Arc::new(DeferredConfirmedHandler {
        completions: completion_tx,
    });
    let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env.clone(),
        super::JetStreamEventConsumerConfig {
            batch_size: 1,
            consumer_name: consumer_name.clone(),
            deliver_policy: DeliverPolicy::All,
            liveness_check_interval: Duration::from_secs(60),
            liveness_observer: Some(Arc::new(SelfObserver::disabled())),
            ..Default::default()
        },
        handler,
        Some(namespace.clone()),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let consumer_task = {
        let consumer = consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await })
    };
    let ready_result = tokio::time::timeout(Duration::from_secs(3), ready_rx).await?;
    if ready_result.is_err() {
        let startup_result = consumer_task.await?;
        panic!("consumer failed before ready: {startup_result:?}");
    }

    let event = DynamicPayload::new(
        "confirmed-ack-test",
        "confirmed.ack_barrier",
        json!({"test": "completion-before-ack"}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;
    let payload = serde_json::to_vec(&event)?;
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-ack-test.confirmed.ack_barrier",
    );
    js.publish(publish_subject, payload.into()).await?.await?;

    let completion = tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
        .await?
        .expect("handler must forward the completion sender");
    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| crate::runtime::SinexError::lifecycle(error.to_string()))?;
    let pending = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 1 {
                return info.num_ack_pending;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(
        pending, 1,
        "enqueue alone must leave the delivery ack-pending"
    );

    completion
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting on the completion receipt");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    consumer.stop().await;
    consumer_task.await??;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn confirmed_consumer_retries_only_the_suffix_after_a_retry_hole(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-prefix-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let consumer_name = format!("prefix-consumer-{}", Uuid::now_v7());
    let (completion_tx, mut completion_rx) = mpsc::channel(8);
    let handler = Arc::new(IdentifiedDeferredConfirmedHandler {
        completions: completion_tx,
    });
    let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env.clone(),
        super::JetStreamEventConsumerConfig {
            batch_size: 3,
            consumer_name: consumer_name.clone(),
            deliver_policy: DeliverPolicy::All,
            liveness_check_interval: Duration::from_secs(60),
            liveness_observer: Some(Arc::new(SelfObserver::disabled())),
            ..Default::default()
        },
        handler,
        Some(namespace.clone()),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let consumer_task = {
        let consumer = consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await })
    };
    let ready_result = tokio::time::timeout(Duration::from_secs(3), ready_rx).await?;
    if ready_result.is_err() {
        let startup_result = consumer_task.await?;
        panic!("consumer failed before ready: {startup_result:?}");
    }

    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-prefix-test.confirmed.prefix_retry",
    );
    let mut event_ids = Vec::with_capacity(3);
    for index in 0..3 {
        let mut event = DynamicPayload::new(
            "confirmed-prefix-test",
            "confirmed.prefix_retry",
            json!({"index": index}),
        )
        .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
        .build()?;
        let id = Id::<Event>::from_uuid(Uuid::now_v7());
        event.id = Some(id.clone());
        event_ids.push(id);
        js.publish(publish_subject.clone(), serde_json::to_vec(&event)?.into())
            .await?
            .await?;
    }

    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| crate::runtime::SinexError::lifecycle(error.to_string()))?;
    let (first_id, first) = tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
        .await?
        .expect("handler channel must remain open");
    let (second_id, second) = tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
        .await?
        .expect("handler channel must remain open");
    let (third_id, third) = tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
        .await?
        .expect("handler channel must remain open");
    assert_eq!(
        [first_id, second_id, third_id],
        [
            Some(event_ids[0].clone()),
            Some(event_ids[1].clone()),
            Some(event_ids[2].clone())
        ]
    );

    first
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting for first completion");
    second
        .send(crate::runtime::ConfirmedEventCompletion::Retry)
        .expect("consumer must still be waiting for second completion");
    third
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting for third completion");

    // NAK uses the production five-second delay. The retry hole and its later
    // safe suffix should be the only deliveries sent back through the handler.
    let (redelivered_second_id, redelivered_second) =
        tokio::time::timeout(Duration::from_secs(20), completion_rx.recv())
            .await?
            .expect("handler channel must remain open");
    let (redelivered_third_id, redelivered_third) =
        tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
            .await?
            .expect("handler channel must remain open");
    assert_eq!(redelivered_second_id, Some(event_ids[1].clone()));
    assert_eq!(redelivered_third_id, Some(event_ids[2].clone()));
    redelivered_second
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting for retried completion");
    redelivered_third
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting for suffix completion");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        completion_rx.try_recv().is_err(),
        "the already-acked prefix must not redeliver"
    );

    consumer.stop().await;
    consumer_task.await??;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn confirmed_consumer_retries_when_completion_receipt_is_dropped(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-drop-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let consumer_name = format!("drop-consumer-{}", Uuid::now_v7());
    let (completion_tx, mut completion_rx) = mpsc::channel(4);
    let handler = Arc::new(IdentifiedDeferredConfirmedHandler {
        completions: completion_tx,
    });
    let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env.clone(),
        super::JetStreamEventConsumerConfig {
            batch_size: 1,
            consumer_name: consumer_name.clone(),
            deliver_policy: DeliverPolicy::All,
            liveness_check_interval: Duration::from_secs(60),
            liveness_observer: Some(Arc::new(SelfObserver::disabled())),
            ..Default::default()
        },
        handler,
        Some(namespace.clone()),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let consumer_task = {
        let consumer = consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await })
    };
    let ready_result = tokio::time::timeout(Duration::from_secs(3), ready_rx).await?;
    if ready_result.is_err() {
        let startup_result = consumer_task.await?;
        panic!("consumer failed before ready: {startup_result:?}");
    }

    let mut event = DynamicPayload::new(
        "confirmed-drop-test",
        "confirmed.dropped_receipt",
        json!({"attempt": 1}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;
    let event_id = Id::<Event>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id.clone());
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-drop-test.confirmed.dropped_receipt",
    );
    js.publish(publish_subject, serde_json::to_vec(&event)?.into())
        .await?
        .await?;

    let (first_id, first_receipt) =
        tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
            .await?
            .expect("handler channel must remain open");
    assert_eq!(first_id, Some(event_id.clone()));
    drop(first_receipt);

    let (redelivered_id, redelivered_receipt) =
        tokio::time::timeout(Duration::from_secs(20), completion_rx.recv())
            .await?
            .expect("handler channel must remain open");
    assert_eq!(redelivered_id, Some(event_id));
    redelivered_receipt
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("consumer must still be waiting for redelivered completion");

    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| crate::runtime::SinexError::lifecycle(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 0 {
                assert_eq!(info.num_redelivered, 1);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    consumer.stop().await;
    consumer_task.await??;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_test(timeout = 60)]
async fn confirmed_consumer_redelivers_unsettled_delivery_after_task_restart(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-restart-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let consumer_name = format!("restart-consumer-{}", Uuid::now_v7());
    let (completion_tx, mut completion_rx) = mpsc::channel(2);
    let make_consumer = || {
        Arc::new(JetStreamEventConsumer::new_with_namespace(
            client.clone(),
            env.clone(),
            super::JetStreamEventConsumerConfig {
                batch_size: 1,
                consumer_name: consumer_name.clone(),
                deliver_policy: DeliverPolicy::All,
                liveness_check_interval: Duration::from_secs(60),
                liveness_observer: Some(Arc::new(SelfObserver::disabled())),
                ..Default::default()
            },
            Arc::new(IdentifiedDeferredConfirmedHandler {
                completions: completion_tx.clone(),
            }),
            Some(namespace.clone()),
        ))
    };

    let first_consumer = make_consumer();
    let (first_ready_tx, first_ready_rx) = oneshot::channel();
    let first_task = {
        let consumer = first_consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(first_ready_tx)).await })
    };
    let first_ready = tokio::time::timeout(Duration::from_secs(3), first_ready_rx).await?;
    if first_ready.is_err() {
        let startup_result = first_task.await?;
        panic!("first consumer failed before ready: {startup_result:?}");
    }

    let mut event = DynamicPayload::new(
        "confirmed-restart-test",
        "confirmed.task_restart",
        json!({"test": "unsettled-durable-redelivery"}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;
    let event_id = Id::<Event>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id.clone());
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-restart-test.confirmed.task_restart",
    );
    js.publish(publish_subject, serde_json::to_vec(&event)?.into())
        .await?
        .await?;

    let (first_id, first_receipt) =
        tokio::time::timeout(Duration::from_secs(3), completion_rx.recv())
            .await?
            .expect("first handler channel must remain open");
    assert_eq!(first_id, Some(event_id.clone()));

    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| crate::runtime::SinexError::lifecycle(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    // Model abrupt in-process loss: abort the runner while its completion
    // receipt is unresolved. This is not an OS process-kill test.
    first_task.abort();
    let first_exit = first_task
        .await
        .expect_err("aborted consumer task must report cancellation");
    assert!(first_exit.is_cancelled());
    drop(first_receipt);
    drop(first_consumer);

    let restarted_consumer = make_consumer();
    let (restart_ready_tx, restart_ready_rx) = oneshot::channel();
    let restarted_task = {
        let consumer = restarted_consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(restart_ready_tx)).await })
    };
    let restart_ready = tokio::time::timeout(Duration::from_secs(3), restart_ready_rx).await?;
    if restart_ready.is_err() {
        let startup_result = restarted_task.await?;
        panic!("restarted consumer failed before ready: {startup_result:?}");
    }

    let (redelivered_id, redelivered_receipt) =
        tokio::time::timeout(Duration::from_secs(35), completion_rx.recv())
            .await?
            .expect("restarted handler channel must remain open");
    assert_eq!(redelivered_id, Some(event_id));
    redelivered_receipt
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("restarted consumer must await the redelivered completion");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let info = durable
                .info()
                .await
                .expect("consumer info must be readable");
            if info.num_ack_pending == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    restarted_consumer.stop().await;
    restarted_task.await??;
    stream.delete_consumer(&consumer_name).await?;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

const PROCESS_DEATH_NATS_URL_ENV: &str = "SINEX_CONFIRMED_PROCESS_DEATH_NATS_URL";
const PROCESS_DEATH_NAMESPACE_ENV: &str = "SINEX_CONFIRMED_PROCESS_DEATH_NAMESPACE";
const PROCESS_DEATH_CONSUMER_ENV: &str = "SINEX_CONFIRMED_PROCESS_DEATH_CONSUMER";
const PROCESS_DEATH_MARKER_ENV: &str = "SINEX_CONFIRMED_PROCESS_DEATH_MARKER";

#[sinex_test(timeout = 90)]
async fn confirmed_consumer_redelivers_unsettled_delivery_after_process_death(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    if let Ok(nats_url) = std::env::var(PROCESS_DEATH_NATS_URL_ENV) {
        let namespace = std::env::var(PROCESS_DEATH_NAMESPACE_ENV)?;
        let consumer_name = std::env::var(PROCESS_DEATH_CONSUMER_ENV)?;
        let marker = std::env::var(PROCESS_DEATH_MARKER_ENV)?;
        let client = async_nats::connect(&nats_url).await?;
        let js = async_nats::jetstream::new(client.clone());
        let env = SinexEnvironment::new("dev")?;
        let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
        let stream_name = format!("{raw_stream}_CONFIRMED");
        let (completion_tx, mut completion_rx) = mpsc::channel(2);
        let consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
            client,
            env,
            JetStreamEventConsumerConfig {
                batch_size: 1,
                consumer_name: consumer_name.clone(),
                deliver_policy: DeliverPolicy::All,
                liveness_check_interval: Duration::from_secs(60),
                liveness_observer: Some(Arc::new(SelfObserver::disabled())),
                ..Default::default()
            },
            Arc::new(IdentifiedDeferredConfirmedHandler {
                completions: completion_tx,
            }),
            Some(namespace),
        ));
        let (ready_tx, ready_rx) = oneshot::channel();
        let consumer_task = {
            let consumer = consumer.clone();
            tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await })
        };
        tokio::time::timeout(Duration::from_secs(10), ready_rx)
            .await?
            .map_err(|error| {
                color_eyre::eyre::eyre!("child consumer failed before ready: {error}")
            })?;

        let (_event_id, _receipt) =
            tokio::time::timeout(Duration::from_secs(10), completion_rx.recv())
                .await?
                .ok_or_else(|| color_eyre::eyre::eyre!("child completion channel closed"))?;
        let stream = js.get_stream(&stream_name).await?;
        let mut durable = stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
            .await
            .map_err(|error| color_eyre::eyre::eyre!("child durable lookup failed: {error}"))?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let info = durable
                    .info()
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                if info.num_ack_pending == 1 {
                    return Ok::<(), std::io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        tokio::fs::write(marker, b"delivery-unsettled").await?;

        // The parent kills this process while the confirmed completion is
        // still unresolved. Keep the runner alive until that OS-level kill.
        std::future::pending::<()>().await;
        consumer.stop().await;
        consumer_task.await??;
        return Ok(());
    }

    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let env = SinexEnvironment::new("dev")?;
    let namespace = format!("confirmed-process-death-{}", Uuid::now_v7());
    let raw_stream = env.nats_stream_name_with_namespace(Some(&namespace), "SINEX_RAW_EVENTS");
    let stream_name = format!("{raw_stream}_CONFIRMED");
    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.confirmed.>");
    let js = async_nats::jetstream::new(client.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![subject],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    let consumer_name = format!("process-death-consumer-{}", Uuid::now_v7());
    let mut event = DynamicPayload::new(
        "confirmed-process-death-test",
        "confirmed.process_death",
        json!({"test": "unsettled-durable-redelivery"}),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()))
    .build()?;
    let event_id = Id::<Event>::from_uuid(Uuid::now_v7());
    event.id = Some(event_id.clone());
    let publish_subject = env.nats_subject_with_namespace(
        Some(&namespace),
        "events.confirmed.material.confirmed-process-death-test.confirmed.process_death",
    );
    js.publish(publish_subject, serde_json::to_vec(&event)?.into())
        .await?
        .await?;

    let marker_dir = tempfile::tempdir()?;
    let marker = marker_dir.path().join("delivery-unsettled");
    let exe = std::env::current_exe()?;
    let module_path_without_crate = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let qualified_name = format!(
        "{module_path_without_crate}::confirmed_consumer_redelivers_unsettled_delivery_after_process_death"
    );
    let mut child = tokio::process::Command::new(exe)
        .arg(&qualified_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(PROCESS_DEATH_NATS_URL_ENV, &nats_url)
        .env(PROCESS_DEATH_NAMESPACE_ENV, &namespace)
        .env(PROCESS_DEATH_CONSUMER_ENV, &consumer_name)
        .env(PROCESS_DEATH_MARKER_ENV, &marker)
        .kill_on_drop(true)
        .spawn()?;

    let marker_result = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if tokio::fs::try_exists(&marker).await? {
                return Ok::<(), std::io::Error>(());
            }
            if let Some(status) = child.try_wait()? {
                return Err(std::io::Error::other(format!(
                    "child exited before holding a confirmed delivery: {status}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match marker_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(color_eyre::eyre::eyre!(
                "process-death child exited before holding the confirmed delivery: {error}"
            ));
        }
        Err(error) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(color_eyre::eyre::eyre!(
                "child did not reach the unsettled delivery boundary before timeout: {error}"
            ));
        }
    }

    child.kill().await?;
    let child_status = child.wait().await?;
    assert!(
        !child_status.success(),
        "parent must kill the child process"
    );

    let (completion_tx, mut completion_rx) = mpsc::channel(2);
    let restarted_consumer = Arc::new(JetStreamEventConsumer::new_with_namespace(
        client.clone(),
        env,
        JetStreamEventConsumerConfig {
            batch_size: 1,
            consumer_name: consumer_name.clone(),
            deliver_policy: DeliverPolicy::All,
            liveness_check_interval: Duration::from_secs(60),
            liveness_observer: Some(Arc::new(SelfObserver::disabled())),
            ..Default::default()
        },
        Arc::new(IdentifiedDeferredConfirmedHandler {
            completions: completion_tx,
        }),
        Some(namespace),
    ));
    let (ready_tx, ready_rx) = oneshot::channel();
    let restarted_task = {
        let consumer = restarted_consumer.clone();
        tokio::spawn(async move { consumer.run_with_ready_signal(Some(ready_tx)).await })
    };
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await?
        .map_err(|error| {
            color_eyre::eyre::eyre!("restarted consumer failed before ready: {error}")
        })?;

    let (redelivered_id, redelivered_receipt) =
        tokio::time::timeout(Duration::from_secs(40), completion_rx.recv())
            .await?
            .ok_or_else(|| color_eyre::eyre::eyre!("restarted completion channel closed"))?;
    assert_eq!(redelivered_id, Some(event_id));
    redelivered_receipt
        .send(crate::runtime::ConfirmedEventCompletion::Safe)
        .expect("restarted consumer must await the redelivered completion");

    let stream = js.get_stream(&stream_name).await?;
    let mut durable = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&consumer_name)
        .await
        .map_err(|error| color_eyre::eyre::eyre!("restarted durable lookup failed: {error}"))?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let info = durable
                .info()
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            if info.num_ack_pending == 0 {
                return Ok::<(), std::io::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    restarted_consumer.stop().await;
    restarted_task.await??;
    stream.delete_consumer(&consumer_name).await?;
    js.delete_stream(&stream_name).await?;
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_deletes_same_service_stale_filters()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events-filter-window_d_focused"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_interval-lift-confirmed-events-filter-window_d_focused_or_window_d_active_or_afk_d_changed_or_unit_d_started_or_unit_d_stopped",
            "sinex_interval-lift-confirmed-events"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_keeps_current_and_unrelated()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "sinex_analytics-confirmed-events-material-filter-command_d_executed"
        ),
        ConfirmedConsumerRetirementAction::KeepCurrent
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "sinex-tag-applier-confirmed-events-material"
        ),
        ConfirmedConsumerRetirementAction::IgnoreUnrelated
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex_analytics-confirmed-events-material-filter-command_d_executed",
            "event-engine-dev"
        ),
        ConfirmedConsumerRetirementAction::IgnoreUnrelated
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_consumer_retirement_deletes_old_provenance_shape()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex-tag-applier-confirmed-events-material",
            "sinex-tag-applier-confirmed-events"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    assert_eq!(
        super::confirmed_consumer_retirement_action(
            "sinex-tag-applier-confirmed-events",
            "sinex-tag-applier-confirmed-events-synthesized"
        ),
        ConfirmedConsumerRetirementAction::DeleteStaleSameService
    );
    Ok(())
}
