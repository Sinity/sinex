use super::{
    DEFAULT_RAW_EVENT_PUBLISH_CONCURRENCY, NatsPublisher, build_publish_payload,
    destructure_provenance, wait_for_publish_ack,
};
use sinex_primitives::{
    DynamicPayload, Id, Uuid,
    domain::{AutomatonModel, SyntheticTemporalPolicy},
    events::Event,
};
use std::{future, io, time::Duration};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn publish_ack_timeout_is_reported() -> TestResult<()> {
    let result =
        wait_for_publish_ack::<(), io::Error, _>(future::pending(), Duration::from_millis(10))
            .await;
    assert!(result.is_err());
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
