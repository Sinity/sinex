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
use tokio::sync::{Mutex, Notify, oneshot};
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
