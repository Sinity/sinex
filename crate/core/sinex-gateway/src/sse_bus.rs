//! Subscription bus: fans out confirmed events to SSE clients via in-memory filters.
//!
//! Subscribes to NATS `events.confirmations.>`, batches event IDs over a 20ms window,
//! fetches full events from Postgres (buffer-cache hot), evaluates each client's
//! [`SubscriptionFilter`], and pushes matches into bounded per-client channels.

use dashmap::DashMap;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::{Id, JsonValue, Timestamp};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Batch window: accumulate confirmation IDs for this duration before DB fetch.
const BATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(20);
const SUBSCRIBE_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const SUBSCRIBE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Maximum IDs to accumulate before forcing a fetch (even within the batch window).
const BATCH_MAX_IDS: usize = 32;

/// Per-client channel capacity. Slow consumers get gap notifications.
const CLIENT_CHANNEL_CAPACITY: usize = 256;
const RECENT_DELIVERED_EVENT_IDS: usize = 1024;

/// Hard cap on concurrent SSE subscriptions. Each one owns a buffered channel,
/// so leaving this unbounded makes memory exhaustion trivial.
pub const MAX_ACTIVE_SUBSCRIPTIONS: usize = 512;

/// Heartbeat interval for keepalive messages.
pub const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

// ─────────────────────────────────────────────────────────────────────
// Message types
// ─────────────────────────────────────────────────────────────────────

/// Messages sent to SSE clients.
#[derive(Debug, Clone)]
pub enum SseMessage {
    /// A matching event arrived.
    Event {
        seq: u64,
        event: Arc<Event<JsonValue>>,
    },
    /// Slow consumer — events were dropped.
    Gap {
        from_seq: u64,
        to_seq: u64,
        dropped: u64,
    },
    /// Keepalive.
    Heartbeat { ts: Timestamp },
}

/// Wire format for the SSE `event` type.
#[derive(Serialize)]
pub(crate) struct SseEventPayload<'a> {
    pub event: &'a Event<JsonValue>,
}

/// Wire format for the SSE `gap` type.
#[derive(Serialize)]
pub(crate) struct SseGapPayload {
    pub from_seq: u64,
    pub to_seq: u64,
    pub dropped: u64,
}

/// Wire format for the SSE `heartbeat` type.
#[derive(Serialize)]
pub(crate) struct SseHeartbeatPayload {
    pub ts: Timestamp,
}

/// Wire format for the SSE `error` type.
#[derive(Serialize)]
#[allow(dead_code)]
pub(crate) struct SseErrorPayload {
    pub code: String,
    pub message: String,
}

// ─────────────────────────────────────────────────────────────────────
// Core bus
// ─────────────────────────────────────────────────────────────────────

struct SubscriptionSlot {
    filter: SubscriptionFilter,
    tx: mpsc::Sender<SseMessage>,
    state: Mutex<SubscriptionState>,
}

struct SubscriptionState {
    next_seq: u64,
    /// Running gap counter — when we fail to send, track how many events were dropped.
    gap_start: Option<u64>,
    gap_count: u64,
    recent_delivered_event_ids: VecDeque<Id<Event<JsonValue>>>,
    recent_delivered_event_id_set: HashSet<Id<Event<JsonValue>>>,
}

enum DeliveryOutcome {
    Delivered,
    Closed,
}

struct IndexedEvents {
    events_by_id: HashMap<Id<Event<JsonValue>>, Arc<Event<JsonValue>>>,
    missing_id_count: usize,
    duplicate_id_count: usize,
}

fn remember_recent_event_id(state: &mut SubscriptionState, event_id: Id<Event<JsonValue>>) {
    if !state.recent_delivered_event_id_set.insert(event_id) {
        return;
    }

    state.recent_delivered_event_ids.push_back(event_id);
    while state.recent_delivered_event_ids.len() > RECENT_DELIVERED_EVENT_IDS {
        if let Some(evicted) = state.recent_delivered_event_ids.pop_front() {
            state.recent_delivered_event_id_set.remove(&evicted);
        }
    }
}

impl SubscriptionSlot {
    fn new(
        filter: SubscriptionFilter,
        resume_from: Option<Id<Event<JsonValue>>>,
    ) -> (Arc<Self>, mpsc::Receiver<SseMessage>) {
        let (tx, rx) = mpsc::channel(CLIENT_CHANNEL_CAPACITY);
        let mut state = SubscriptionState {
            next_seq: 1,
            gap_start: None,
            gap_count: 0,
            recent_delivered_event_ids: VecDeque::with_capacity(RECENT_DELIVERED_EVENT_IDS),
            recent_delivered_event_id_set: HashSet::with_capacity(RECENT_DELIVERED_EVENT_IDS),
        };
        if let Some(event_id) = resume_from {
            remember_recent_event_id(&mut state, event_id);
        }
        let slot = Arc::new(Self {
            filter,
            tx,
            state: Mutex::new(state),
        });
        (slot, rx)
    }

    fn matches(&self, event: &Event<JsonValue>) -> bool {
        self.filter.matches(event)
    }

    fn deliver(&self, event: &Arc<Event<JsonValue>>) -> DeliveryOutcome {
        let mut state = self.state.lock();
        if let Some(event_id) = event.id
            && state.recent_delivered_event_id_set.contains(&event_id)
        {
            return DeliveryOutcome::Delivered;
        }

        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);

        if let Some(from_seq) = state.gap_start {
            let gap = SseMessage::Gap {
                from_seq,
                to_seq: seq.saturating_sub(1),
                dropped: state.gap_count,
            };

            match self.tx.try_send(gap) {
                Ok(()) => {
                    state.gap_start = None;
                    state.gap_count = 0;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    state.gap_count += 1;
                    return DeliveryOutcome::Delivered;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return DeliveryOutcome::Closed;
                }
            }
        }

        let msg = SseMessage::Event {
            seq,
            event: Arc::clone(event),
        };

        match self.tx.try_send(msg) {
            Ok(()) => {
                if let Some(event_id) = event.id {
                    remember_recent_event_id(&mut state, event_id);
                }
                DeliveryOutcome::Delivered
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if state.gap_start.is_none() {
                    state.gap_start = Some(seq);
                }
                state.gap_count += 1;
                DeliveryOutcome::Delivered
            }
            Err(mpsc::error::TrySendError::Closed(_)) => DeliveryOutcome::Closed,
        }
    }
}

/// Fan-out bus from NATS confirmations → per-client SSE streams.
pub struct SubscriptionBus {
    subscriptions: DashMap<u64, Arc<SubscriptionSlot>>,
    next_sub_id: AtomicU64,
    active_subscriptions: AtomicUsize,
}

impl SubscriptionBus {
    /// Create a new subscription bus.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
            active_subscriptions: AtomicUsize::new(0),
        }
    }

    /// Register a new subscription. Returns `(sub_id, receiver)`.
    pub fn register(
        &self,
        filter: SubscriptionFilter,
        resume_from: Option<Id<Event<JsonValue>>>,
    ) -> Option<(u64, mpsc::Receiver<SseMessage>)> {
        if self
            .active_subscriptions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < MAX_ACTIVE_SUBSCRIPTIONS).then_some(current + 1)
            })
            .is_err()
        {
            return None;
        }

        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (slot, rx) = SubscriptionSlot::new(filter, resume_from);
        self.subscriptions.insert(id, slot);
        debug!(sub_id = id, "SSE subscription registered");
        Some((id, rx))
    }

    /// Unregister a subscription (client disconnected).
    pub fn unregister(&self, sub_id: u64) {
        if self.subscriptions.remove(&sub_id).is_some() {
            self.active_subscriptions.fetch_sub(1, Ordering::AcqRel);
            debug!(sub_id, "SSE subscription unregistered");
        }
    }

    /// Number of active subscriptions.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active_subscriptions.load(Ordering::Acquire)
    }

    /// Run the bus loop. Blocks until the shutdown signal fires.
    ///
    /// This subscribes to NATS confirmations, batches IDs, fetches events from DB,
    /// and fans out to all matching client channels.
    ///
    /// If `ready` is provided, it will be notified once the NATS subscription is active.
    pub async fn run(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        pool: sqlx::PgPool,
        env: sinex_primitives::environment::SinexEnvironment,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        self.run_with_ready(nats_client, pool, env, shutdown, None)
            .await;
    }

    /// Like [`run`](Self::run), but notifies `ready` once the NATS subscription is active.
    /// Useful in tests to avoid racing between subscribe and publish.
    pub async fn run_with_ready(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        pool: sqlx::PgPool,
        env: sinex_primitives::environment::SinexEnvironment,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        ready: Option<Arc<tokio::sync::Notify>>,
    ) {
        let subject = env.nats_subject("events.confirmations.>");
        let mut id_buffer: Vec<Id<Event<JsonValue>>> = Vec::with_capacity(BATCH_MAX_IDS);
        let mut batch_timer = tokio::time::interval(BATCH_WINDOW);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ready = ready;
        let mut ready_notified = false;

        'outer: loop {
            let mut sub = loop {
                let subscribe_result =
                    tokio::time::timeout(SUBSCRIBE_ATTEMPT_TIMEOUT, nats_client.subscribe(subject.clone())).await;
                match subscribe_result {
                    Ok(Ok(sub)) => {
                        if ready_notified {
                            info!(subject, "SSE subscription bus reconnected");
                        } else {
                            info!(subject, "SSE subscription bus started");
                            if let Some(notify) = ready.take() {
                                notify.notify_one();
                            }
                            ready_notified = true;
                        }
                        break sub;
                    }
                    Ok(Err(error)) => {
                        warn!(
                            ?error,
                            subject,
                            retry_delay_ms = SUBSCRIBE_RETRY_DELAY.as_millis(),
                            "Failed to subscribe to confirmations — retrying"
                        );
                    }
                    Err(_) => {
                        warn!(
                            subject,
                            subscribe_timeout_ms = SUBSCRIBE_ATTEMPT_TIMEOUT.as_millis(),
                            retry_delay_ms = SUBSCRIBE_RETRY_DELAY.as_millis(),
                            "Timed out subscribing to confirmations — retrying"
                        );
                    }
                }
                tokio::select! {
                    shutdown_result = shutdown.changed() => {
                        if shutdown_result.is_err() {
                            warn!("SSE bus shutdown channel dropped before explicit shutdown");
                        }
                        if shutdown_result.is_err() || *shutdown.borrow() {
                            if !id_buffer.is_empty() {
                                self.flush_batch(&mut id_buffer, &pool).await;
                            }
                            info!("SSE bus shutting down");
                            break 'outer;
                        }
                    }
                    _ = tokio::time::sleep(SUBSCRIBE_RETRY_DELAY) => {}
                }
            };

            loop {
                tokio::select! {
                    biased;

                    shutdown_result = shutdown.changed() => {
                        if shutdown_result.is_err() {
                            warn!("SSE bus shutdown channel dropped before explicit shutdown");
                        }
                        if shutdown_result.is_err() || *shutdown.borrow() {
                            if !id_buffer.is_empty() {
                                self.flush_batch(&mut id_buffer, &pool).await;
                            }
                            info!("SSE bus shutting down");
                            break 'outer;
                        }
                    }

                    msg = sub.next() => {
                        let Some(msg) = msg else {
                            warn!(
                                retry_delay_ms = SUBSCRIBE_RETRY_DELAY.as_millis(),
                                "NATS subscription closed — SSE bus reconnecting"
                            );
                            if !id_buffer.is_empty() {
                                self.flush_batch(&mut id_buffer, &pool).await;
                            }
                            continue 'outer;
                        };

                        // Parse confirmation payload: { "event_id": "...", "persisted": true, "ts_ingest": "..." }
                        match Self::parse_confirmation(&msg.payload) {
                            Ok(Some(event_id)) => {
                                id_buffer.push(event_id);

                                // Flush immediately if buffer full
                                if id_buffer.len() >= BATCH_MAX_IDS {
                                    self.flush_batch(&mut id_buffer, &pool).await;
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    payload_len = msg.payload.len(),
                                    payload_preview = %Self::payload_preview(&msg.payload),
                                    "Ignoring malformed SSE confirmation payload"
                                );
                            }
                        }
                    }

                    _ = batch_timer.tick() => {
                        if !id_buffer.is_empty() {
                            self.flush_batch(&mut id_buffer, &pool).await;
                        }
                    }
                }
            }
        }

        info!("SSE subscription bus stopped");
    }

    /// Parse an event ID from a NATS confirmation message payload.
    fn parse_confirmation(payload: &[u8]) -> Result<Option<Id<Event<JsonValue>>>, String> {
        #[derive(serde::Deserialize)]
        struct Confirmation {
            event_id: String,
            persisted: bool,
        }

        let conf: Confirmation = serde_json::from_slice(payload)
            .map_err(|error| format!("failed to parse confirmation JSON: {error}"))?;
        if !conf.persisted {
            return Ok(None);
        }
        conf.event_id
            .parse()
            .map(Some)
            .map_err(|error| format!("failed to parse confirmation event_id '{}': {error}", conf.event_id))
    }

    fn payload_preview(payload: &[u8]) -> String {
        const MAX_PREVIEW_CHARS: usize = 160;
        let preview = String::from_utf8_lossy(payload);
        let mut truncated = preview.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
        if preview.chars().count() > MAX_PREVIEW_CHARS {
            truncated.push('…');
        }
        truncated
    }

    fn index_events_by_id(events: Vec<Event<JsonValue>>) -> IndexedEvents {
        let mut events_by_id = HashMap::with_capacity(events.len());
        let mut missing_id_count = 0usize;
        let mut duplicate_id_count = 0usize;

        for event in events {
            let Some(id) = event.id else {
                missing_id_count = missing_id_count.saturating_add(1);
                continue;
            };
            if events_by_id.insert(id, Arc::new(event)).is_some() {
                duplicate_id_count = duplicate_id_count.saturating_add(1);
            }
        }

        IndexedEvents {
            events_by_id,
            missing_id_count,
            duplicate_id_count,
        }
    }

    fn snapshot_subscriptions(&self) -> Vec<(u64, Arc<SubscriptionSlot>)> {
        self.subscriptions
            .iter()
            .map(|slot| (*slot.key(), Arc::clone(slot.value())))
            .collect()
    }

    /// Fetch events from DB and fan out to all matching subscriptions.
    async fn flush_batch(&self, id_buffer: &mut Vec<Id<Event<JsonValue>>>, pool: &sqlx::PgPool) {
        let ids: Vec<_> = std::mem::take(id_buffer);
        if ids.is_empty() {
            return;
        }

        let mut duplicate_confirmation_count = 0usize;
        let mut seen_ids = HashSet::with_capacity(ids.len());
        let mut unique_ids = Vec::with_capacity(ids.len());
        for id in ids {
            if seen_ids.insert(id) {
                unique_ids.push(id);
            } else {
                duplicate_confirmation_count = duplicate_confirmation_count.saturating_add(1);
            }
        }

        // Batch-fetch from DB (buffer-cache hot — events were JUST written)
        let events = match pool.events().get_by_ids(&unique_ids).await {
            Ok(events) => events,
            Err(e) => {
                warn!(
                    ?e,
                    count = unique_ids.len(),
                    "Failed to fetch events for SSE fan-out; preserving batch for retry"
                );
                *id_buffer = unique_ids;
                return;
            }
        };

        let IndexedEvents {
            mut events_by_id,
            missing_id_count,
            duplicate_id_count,
        } = Self::index_events_by_id(events);
        if missing_id_count > 0 || duplicate_id_count > 0 || duplicate_confirmation_count > 0 {
            warn!(
                events_without_id = missing_id_count,
                duplicate_event_ids = duplicate_id_count,
                duplicate_confirmation_ids = duplicate_confirmation_count,
                "SSE fan-out fetch returned malformed events; preserving unresolved confirmations for retry"
            );
        }
        let mut events = Vec::new();
        let mut missing_ids = Vec::new();
        for id in unique_ids {
            if let Some(event) = events_by_id.remove(&id) {
                events.push(event);
            } else {
                missing_ids.push(id);
            }
        }

        if !missing_ids.is_empty() {
            warn!(
                missing = missing_ids.len(),
                "SSE fan-out fetch missed confirmed events; preserving IDs for retry"
            );
            id_buffer.extend(missing_ids);
        }

        // Snapshot handles so DashMap entry locks are not held during filter evaluation or send.
        let subscriptions = self.snapshot_subscriptions();
        let mut to_remove = Vec::new();

        for (sub_id, slot) in subscriptions {
            for event in &events {
                if !slot.matches(event) {
                    continue;
                }

                if matches!(slot.deliver(event), DeliveryOutcome::Closed) {
                    to_remove.push(sub_id);
                    break;
                }
            }
        }

        // Clean up disconnected clients
        for sub_id in to_remove {
            self.unregister(sub_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeliveryOutcome, SseMessage, SubscriptionBus, SubscriptionSlot};
    use serde_json::json;
    use sinex_db::DbPoolExt;
    use sinex_primitives::events::{DynamicPayload, Event};
    use sinex_primitives::query::SubscriptionFilter;
    use sinex_primitives::{Id, JsonValue, Uuid};
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
            .map_err(|error| color_eyre::eyre::eyre!(error))?;
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
            .map_err(|error| color_eyre::eyre::eyre!(error))?;
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
        Ok(())
    }

    #[sinex_test]
    async fn payload_preview_truncates_long_payloads() -> TestResult<()> {
        let preview = SubscriptionBus::payload_preview(&vec![b'a'; 200]);
        assert!(preview.ends_with('…'));
        assert_eq!(preview.chars().count(), 161);
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

        let indexed = SubscriptionBus::index_events_by_id(vec![missing_id, duplicate, duplicate_again]);

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
        Ok(())
    }

    #[sinex_test]
    async fn flush_batch_preserves_missing_confirmations_for_retry(ctx: TestContext) -> TestResult<()> {
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
        let (slot, mut rx) = SubscriptionSlot::new(SubscriptionFilter::default(), None);
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
            SseMessage::Event { event: delivered, .. } => {
                assert_eq!(delivered.id, event.id);
            }
            other => panic!("expected first SSE event, got {other:?}"),
        }

        assert!(
            timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
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
            timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
            "the last delivered event id should not be replayed immediately on reconnect"
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
            .from_material(ctx.create_source_material(Some("sse-duplicate-confirmations")).await?)
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
            timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
            "duplicate confirmation ids should not trigger a second delivery"
        );
        assert!(
            id_buffer.is_empty(),
            "duplicate confirmations should not be re-buffered after a successful flush"
        );
        Ok(())
    }
}
