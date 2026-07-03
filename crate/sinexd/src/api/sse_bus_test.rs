use super::{
    CLIENT_CHANNEL_CAPACITY, DeliveryOutcome, SseMessage, SubscriptionBus, SubscriptionSlot,
};
use serde_json::json;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::{Id, Uuid};
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn parse_confirmed_event_round_trips_full_event() -> TestResult<()> {
    let event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 7}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    let bytes = serde_json::to_vec(&event)?;

    let parsed = SubscriptionBus::parse_confirmed_event(&bytes)
        .expect("a serialized Event must round-trip through parse_confirmed_event");
    assert_eq!(parsed.id, event.id);
    assert_eq!(parsed.event_type.as_str(), "sse.event");
    assert_eq!(parsed.payload, json!({"value": 7}));
    Ok(())
}

#[sinex_test]
async fn parse_confirmed_event_reports_invalid_json() -> TestResult<()> {
    let error = SubscriptionBus::parse_confirmed_event(br#"{"not":"an event"#)
        .expect_err("invalid JSON should be reported");
    assert!(error.contains("failed to parse confirmed event JSON"));
    Ok(())
}

#[sinex_test]
async fn payload_fingerprint_does_not_disclose_raw_log_bytes() -> TestResult<()> {
    let payload = b"malformed-confirmation-secret";
    let fingerprint = SubscriptionBus::payload_fingerprint(payload);

    assert!(fingerprint.contains("len=29"));
    assert!(fingerprint.contains("blake3="));
    assert!(!fingerprint.contains("malformed"));
    assert!(!fingerprint.contains("secret"));
    Ok(())
}

#[sinex_test]
async fn fan_out_event_delivers_full_event_to_matching_subscription() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");

    let mut event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    event.id = Some(Id::from_uuid(Uuid::now_v7()));
    let expected_id = event.id;

    bus.fan_out_event(Arc::new(event));

    let message = timeout(Duration::from_secs(1), rx.recv())
        .await?
        .expect("subscription should receive the confirmed event directly");
    match message {
        SseMessage::Event { event, .. } => assert_eq!(event.id, expected_id),
        other => panic!("expected event payload, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn deliver_suppresses_recently_redelivered_events() -> TestResult<()> {
    let (slot, mut rx) =
        SubscriptionSlot::new(SubscriptionFilter::default(), None, CLIENT_CHANNEL_CAPACITY);
    let mut event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    event.id = Some(Id::from_uuid(Uuid::now_v7()));
    let event = Arc::new(event);

    assert!(matches!(slot.deliver(&event), DeliveryOutcome::Delivered));
    assert!(matches!(slot.deliver(&event), DeliveryOutcome::Delivered));

    let message = timeout(Duration::from_secs(1), rx.recv())
        .await?
        .expect("first delivery should reach the receiver");
    match message {
        SseMessage::Event {
            event: delivered, ..
        } => assert_eq!(delivered.id, event.id),
        other => panic!("expected first SSE event, got {other:?}"),
    }

    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "duplicate delivery should be suppressed for recently delivered events"
    );
    Ok(())
}

#[sinex_test]
async fn register_seeds_recent_event_id_for_reconnect_deduplication() -> TestResult<()> {
    let event_id = Id::from_uuid(Uuid::now_v7());
    let mut event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    event.id = Some(event_id);
    let event = Arc::new(event);

    let bus = SubscriptionBus::new();
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), Some(event_id))
        .expect("test subscription should register");
    let slot = bus
        .subscriptions
        .iter()
        .next()
        .map(|entry| Arc::clone(entry.value()))
        .expect("subscription slot should exist");

    assert!(matches!(slot.deliver(&event), DeliveryOutcome::Delivered));
    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "the last delivered event id should not be replayed immediately on reconnect"
    );
    Ok(())
}

#[sinex_test]
async fn delivery_scope_does_not_evaluate_payload_predicates() -> TestResult<()> {
    use sinex_primitives::query::PayloadFilter;

    let filter = SubscriptionFilter {
        payload: Some(PayloadFilter::HasKey {
            key: "missing_after_disclosure".to_string(),
        }),
        ..Default::default()
    };
    let (slot, _) = SubscriptionSlot::new(filter, None, CLIENT_CHANNEL_CAPACITY);
    let event = DynamicPayload::new("sse-test", "sse.event", json!({"public": true}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;

    assert!(
        slot.matches_delivery_scope(&event),
        "SSE bus must only prefilter on source/type/host; payload predicates run after disclosure"
    );
    Ok(())
}
