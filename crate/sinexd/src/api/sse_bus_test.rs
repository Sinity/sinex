use super::{
    CLIENT_CHANNEL_CAPACITY, CONFIRMATION_RETRY_MAX_ATTEMPTS, DeliveryOutcome, SseMessage,
    SubscriptionBus, SubscriptionSlot,
};
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::events::{DynamicPayload, Event};
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::{Id, JsonValue, SinexError, Uuid};
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::*;

// Inline because these exercise private confirmation parsing/reporting helpers.
#[sinex_test]
async fn parse_confirmation_accepts_persisted_event() -> TestResult<()> {
    let event_id = Id::from_uuid(Uuid::now_v7());
    let payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "persisted": true,
    });

    let parsed = SubscriptionBus::parse_confirmation(&serde_json::to_vec(&payload)?)
        .map_err(SinexError::validation)?;
    assert_eq!(parsed, Some(event_id));
    Ok(())
}

#[sinex_test]
async fn parse_confirmation_ignores_unpersisted_event() -> TestResult<()> {
    let payload = serde_json::json!({
        "event_id": Uuid::now_v7().to_string(),
        "persisted": false,
    });

    let parsed = SubscriptionBus::parse_confirmation(&serde_json::to_vec(&payload)?)
        .map_err(SinexError::validation)?;
    assert!(parsed.is_none());
    Ok(())
}

#[sinex_test]
async fn parse_confirmation_reports_invalid_json() -> TestResult<()> {
    let error = SubscriptionBus::parse_confirmation(br#"{"event_id":"oops""#)
        .expect_err("invalid JSON should be reported");
    assert!(error.contains("failed to parse confirmation JSON"));
    Ok(())
}

#[sinex_test]
async fn parse_confirmation_reports_invalid_event_id() -> TestResult<()> {
    let payload = serde_json::json!({
        "event_id": "not-a-uuid",
        "persisted": true,
    });

    let error = SubscriptionBus::parse_confirmation(&serde_json::to_vec(&payload)?)
        .expect_err("invalid event_id should be reported");
    assert!(error.contains("failed to parse confirmation event_id"));
    assert!(error.contains("len=10"));
    assert!(error.contains("blake3="));
    assert!(
        !error.contains("not-a-uuid"),
        "invalid confirmation ids are operator-supplied text and must not be copied into log-bound errors"
    );
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
async fn index_events_by_id_reports_missing_and_duplicate_ids() -> TestResult<()> {
    let mut missing_id = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    missing_id.id = None;

    let duplicate_id = Id::from_uuid(Uuid::now_v7());
    let mut duplicate = DynamicPayload::new("sse-test", "sse.event", json!({"value": 2}))
        .from_material(Id::from_uuid(Uuid::now_v7()))
        .build()?;
    duplicate.id = Some(duplicate_id);
    let duplicate_again = duplicate.clone();

    let indexed =
        SubscriptionBus::index_events_by_id(vec![missing_id, duplicate, duplicate_again]);

    assert_eq!(indexed.missing_id_count, 1);
    assert_eq!(indexed.duplicate_id_count, 1);
    assert_eq!(indexed.events_by_id.len(), 1);
    Ok(())
}

#[sinex_test]
async fn flush_batch_preserves_ids_when_db_fetch_fails(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    pool.close().await;

    let bus = SubscriptionBus::new();
    let event_id = Id::from_uuid(Uuid::now_v7());
    let mut id_buffer = vec![event_id];

    bus.flush_batch(&mut id_buffer, &pool).await;

    assert_eq!(
        id_buffer,
        vec![event_id],
        "SSE confirmation batches should stay buffered when DB fan-out fetch fails"
    );
    assert_eq!(bus.health_snapshot().db_fetch_failures_total, 1);
    Ok(())
}

#[sinex_test]
async fn health_snapshot_reports_retry_and_drop_counters() -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let event_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());

    let mut id_buffer = Vec::new();
    bus.filter_retry_ids(vec![event_id], &mut id_buffer);
    let snapshot = bus.health_snapshot();
    assert_eq!(snapshot.pending_retry_confirmations, 1);
    assert_eq!(snapshot.dropped_confirmations_total, 0);

    for _ in 1..CONFIRMATION_RETRY_MAX_ATTEMPTS {
        id_buffer.clear();
        bus.filter_retry_ids(vec![event_id], &mut id_buffer);
    }

    let snapshot = bus.health_snapshot();
    assert_eq!(snapshot.pending_retry_confirmations, 0);
    assert_eq!(snapshot.dropped_confirmations_total, 1);
    Ok(())
}

#[sinex_test]
async fn flush_batch_preserves_missing_confirmations_for_retry(
    ctx: TestContext,
) -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");

    let event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(ctx.create_source_material(Some("sse-batch-miss")).await?)
        .build()?;
    let event = ctx.pool().events().insert(event).await?;
    let event_id = event.id.expect("inserted event must have id");
    let missing_id = Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7());

    let mut id_buffer = vec![event_id, missing_id];
    bus.flush_batch(&mut id_buffer, ctx.pool()).await;

    let message = timeout(Duration::from_secs(1), rx.recv())
        .await?
        .expect("subscription should receive the found event");
    match message {
        SseMessage::Event { event, .. } => assert_eq!(event.id, Some(event_id)),
        other => panic!("expected event payload, got {other:?}"),
    }

    assert_eq!(
        id_buffer,
        vec![missing_id],
        "missing confirmed event ids must remain buffered for retry"
    );
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
        } => {
            assert_eq!(delivered.id, event.id);
        }
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

#[sinex_test]
async fn flush_batch_dedupes_duplicate_confirmation_ids(ctx: TestContext) -> TestResult<()> {
    let bus = SubscriptionBus::new();
    let (_, mut rx) = bus
        .register(SubscriptionFilter::default(), None)
        .expect("test subscription should register");

    let event = DynamicPayload::new("sse-test", "sse.event", json!({"value": 1}))
        .from_material(
            ctx.create_source_material(Some("sse-duplicate-confirmations"))
                .await?,
        )
        .build()?;
    let event = ctx.pool().events().insert(event).await?;
    let event_id = event.id.expect("inserted event must have id");

    let mut id_buffer = vec![event_id, event_id];
    bus.flush_batch(&mut id_buffer, ctx.pool()).await;

    let message = timeout(Duration::from_secs(1), rx.recv())
        .await?
        .expect("subscription should receive the deduped event");
    match message {
        SseMessage::Event { event, .. } => assert_eq!(event.id, Some(event_id)),
        other => panic!("expected deduped event payload, got {other:?}"),
    }

    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "duplicate confirmation ids should not trigger a second delivery"
    );
    assert!(
        id_buffer.is_empty(),
        "duplicate confirmations should not be re-buffered after a successful flush"
    );
    Ok(())
}
