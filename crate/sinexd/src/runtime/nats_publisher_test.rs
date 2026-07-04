use super::{
    DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY, NatsPublisher, RAW_STREAM_BACKPRESSURE_HIGH_PENDING,
    RAW_STREAM_BACKPRESSURE_LOW_PENDING, build_publish_payload, destructure_provenance,
    wait_for_publish_ack,
};
use crate::runtime::nats_payload::NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES;
use sinex_primitives::{
    DynamicPayload, Id, Uuid,
    domain::{AutomatonModel, HostName, SyntheticTemporalPolicy},
    events::Event,
    events::admission::EventIntent,
    transport,
};
use std::{future, io, time::Duration};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn publish_ack_timeout_is_reported() -> TestResult<()> {
    let result =
        wait_for_publish_ack::<(), io::Error, _>(future::pending(), Duration::from_millis(10))
            .await;
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn publish_with_headers_rejects_oversized_payload_before_nats(
    ctx: xtask::sandbox::TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let publisher = NatsPublisher::new(ctx.nats_client());
    let error = publisher
        .publish_with_headers(
            "oversized.test".to_string(),
            async_nats::HeaderMap::new(),
            vec![0; NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES + 1],
            "oversized test publish",
        )
        .await
        .expect_err("oversized payload should fail before NATS publish");

    let error_text = error.to_string();
    assert!(error_text.contains("NATS payload exceeds configured hard limit"));
    assert!(error_text.contains("oversized test publish"));
    Ok(())
}

#[sinex_test]
async fn raw_stream_backpressure_uses_ordered_pending_hysteresis() -> TestResult<()> {
    assert!(RAW_STREAM_BACKPRESSURE_LOW_PENDING < RAW_STREAM_BACKPRESSURE_HIGH_PENDING);
    Ok(())
}

#[sinex_test]
async fn publish_payload_serializes_json_once() -> TestResult<()> {
    let mut event = DynamicPayload::new(
        "publisher.test",
        "payload.check",
        serde_json::json!({"nested": {"a": 1}}),
    )
    .from_parents([Id::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    let (event_id, payload) =
        build_publish_payload(&event, None, None, None, None, None, None)?;
    let value: serde_json::Value = serde_json::from_slice(&payload)?;

    assert_eq!(value["id"], event_id);
    assert!(value["payload"].is_object());
    assert_eq!(value["payload"]["nested"]["a"], 1);
    Ok(())
}

#[sinex_test]
async fn publish_payload_preserves_replay_and_synthetic_metadata() -> TestResult<()> {
    let source_material_id = Id::from_uuid(Uuid::now_v7());
    let mut event = DynamicPayload::new(
        "publisher.test",
        "payload.replay",
        serde_json::json!({"path": "/tmp/replay.txt"}),
    )
    .from_material(source_material_id)
    .build()
    .expect("infallible: test provenance set");
    let operation_id = Uuid::now_v7();
    event.id = Some(Id::from_uuid(Uuid::now_v7()));
    event.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
    event.semantics_version = Some("2026-04-13".to_string());
    event.scope_key = Some("scope:publisher".to_string());
    event.equivalence_key = Some("publisher-slot".to_string());
    event.created_by_operation_id = Some(operation_id);
    event.automaton_model = Some(AutomatonModel::Windowed);

    let prov = destructure_provenance(event.provenance());
    let (_, payload) = build_publish_payload(
        &event,
        prov.source_material_id,
        prov.anchor_byte,
        prov.offset_start,
        prov.offset_end,
        prov.offset_kind,
        prov.source_event_ids,
    )?;
    let decoded: Event<serde_json::Value> = serde_json::from_slice(&payload)?;

    assert_eq!(
        decoded.temporal_policy,
        Some(SyntheticTemporalPolicy::LatestInput)
    );
    assert_eq!(decoded.semantics_version.as_deref(), Some("2026-04-13"));
    assert_eq!(decoded.scope_key.as_deref(), Some("scope:publisher"));
    assert_eq!(decoded.equivalence_key.as_deref(), Some("publisher-slot"));
    assert_eq!(decoded.created_by_operation_id, Some(operation_id));
    assert_eq!(decoded.automaton_model, Some(AutomatonModel::Windowed));
    Ok(())
}

#[sinex_test]
async fn invalid_publish_concurrency_override_falls_back_to_default(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let previous = std::env::var("SINEX_PUBLISH_CONCURRENCY").ok();
    unsafe { std::env::set_var("SINEX_PUBLISH_CONCURRENCY", "bogus") };

    let publisher = NatsPublisher::new(ctx.nats_client());

    unsafe {
        match previous {
            Some(value) => std::env::set_var("SINEX_PUBLISH_CONCURRENCY", value),
            None => std::env::remove_var("SINEX_PUBLISH_CONCURRENCY"),
        }
    }

    assert_eq!(
        publisher.semaphores.raw_event.available_permits(),
        DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY
    );
    Ok(())
}

#[sinex_test]
async fn publish_intent_bootstraps_raw_events_stream(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut event = DynamicPayload::new(
        "publisher.test",
        "stream.bootstrap",
        serde_json::json!({"ok": true}),
    )
    .from_parents([Id::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    let intent = EventIntent::new(
        "publisher.test".to_string(),
        "publisher-test",
        "1.0.0",
        vec![event],
        HostName::from_static("test-host"),
    );
    let publisher = NatsPublisher::new(ctx.nats_client());

    publisher
        .publish_intent(&intent, transport::Class::Critical)
        .await?;

    let stream_name = sinex_primitives::environment::environment()
        .nats_stream_name_with_namespace(None, "SINEX_RAW_EVENTS");
    let mut stream = async_nats::jetstream::new(ctx.nats_client())
        .get_stream(&stream_name)
        .await?;
    assert_eq!(stream.info().await?.state.messages, 1);
    Ok(())
}

#[sinex_test]
async fn publish_telemetry_bootstraps_reflection_events_stream(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let mut event = DynamicPayload::new(
        "sinexd.event_engine",
        "metric.gauge",
        serde_json::json!({"name": "event_engine.consumer.lag.pending", "value": 0}),
    )
    .from_parents([Id::from_uuid(Uuid::now_v7())])?
    .build()
    .expect("infallible: test provenance set");
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    let publisher = NatsPublisher::new(ctx.nats_client());
    publisher
        .publish_telemetry(&event, transport::Class::Telemetry)
        .await?;

    let env = sinex_primitives::environment::environment();
    let reflection_stream_name =
        env.nats_stream_name_with_namespace(None, "SINEX_REFLECTION_EVENTS");
    let mut reflection_stream = async_nats::jetstream::new(ctx.nats_client())
        .get_stream(&reflection_stream_name)
        .await?;
    assert_eq!(reflection_stream.info().await?.state.messages, 1);

    let raw_stream_name = env.nats_stream_name_with_namespace(None, "SINEX_RAW_EVENTS");
    let raw_stream = async_nats::jetstream::new(ctx.nats_client())
        .get_stream(&raw_stream_name)
        .await;
    assert!(
        raw_stream.is_err(),
        "telemetry publish should not create the activity raw stream"
    );
    Ok(())
}
