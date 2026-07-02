//! Subscription bus: fans out confirmed events to SSE clients via in-memory filters.
//!
//! Subscribes to NATS `events.confirmations.>`, batches event IDs over a 20ms window,
//! fetches full events from Postgres (buffer-cache hot), evaluates each client's
//! source/type/host subscription scope, and pushes candidates into bounded
//! per-client channels. Payload predicates are evaluated after view disclosure
//! in the SSE handler so raw payload fields cannot be inferred by filtering.

use dashmap::DashMap;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::views::CaveatView;
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

/// Maximum retry attempts for a single confirmed event ID before it is dropped.
/// Prevents infinite retry loops when the DB persistently misses a confirmed event.
const CONFIRMATION_RETRY_MAX_ATTEMPTS: u8 = 10;

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
    /// Structured stream error.
    Error { code: String, message: String },
}

/// Wire format for the SSE `event` type.
#[derive(Serialize)]
pub(crate) struct SseEventPayload<'a> {
    pub event: &'a Event<JsonValue>,
    pub privacy_caveats: &'a [CaveatView],
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

/// Operator-facing health snapshot for the confirmation fan-out path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SseBusHealthSnapshot {
    pub active_subscriptions: usize,
    pub pending_retry_confirmations: usize,
    pub dropped_confirmations_total: u64,
    pub db_fetch_failures_total: u64,
    pub malformed_confirmations_total: u64,
    pub subscription_reconnects_total: u64,
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
        channel_capacity: usize,
    ) -> (Arc<Self>, mpsc::Receiver<SseMessage>) {
        let (tx, rx) = mpsc::channel(channel_capacity);
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

    fn matches_delivery_scope(&self, event: &Event<JsonValue>) -> bool {
        if !self.filter.sources.is_empty() && !self.filter.sources.contains(&event.source) {
            return false;
        }
        if !self.filter.event_types.is_empty()
            && !self.filter.event_types.contains(&event.event_type)
        {
            return false;
        }
        if !self.filter.hosts.is_empty() && !self.filter.hosts.contains(&event.host) {
            return false;
        }
        true
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
    channel_capacity: usize,
    subscriptions: DashMap<u64, Arc<SubscriptionSlot>>,
    next_sub_id: AtomicU64,
    active_subscriptions: AtomicUsize,
    /// Tracks retry counts for confirmed event IDs that were not found in the DB.
    /// After `CONFIRMATION_RETRY_MAX_ATTEMPTS`, the ID is dropped with a warning.
    confirmation_retry_counts: Mutex<HashMap<Id<Event<JsonValue>>, u8>>,
    dropped_confirmations_total: AtomicU64,
    db_fetch_failures_total: AtomicU64,
    malformed_confirmations_total: AtomicU64,
    subscription_reconnects_total: AtomicU64,
}

impl Default for SubscriptionBus {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionBus {
    /// Create a new subscription bus.
    #[must_use]
    pub fn new() -> Self {
        Self::with_channel_capacity(CLIENT_CHANNEL_CAPACITY)
    }

    /// Create a bus with a specific per-client buffer size.
    #[must_use]
    pub fn with_channel_capacity(channel_capacity: usize) -> Self {
        assert!(
            channel_capacity > 0,
            "SSE subscription channel capacity must be positive"
        );
        Self {
            channel_capacity,
            subscriptions: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
            active_subscriptions: AtomicUsize::new(0),
            confirmation_retry_counts: Mutex::new(HashMap::new()),
            dropped_confirmations_total: AtomicU64::new(0),
            db_fetch_failures_total: AtomicU64::new(0),
            malformed_confirmations_total: AtomicU64::new(0),
            subscription_reconnects_total: AtomicU64::new(0),
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
        let (slot, rx) = SubscriptionSlot::new(filter, resume_from, self.channel_capacity);
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

    /// Snapshot confirmation fan-out health counters for operator status.
    #[must_use]
    pub fn health_snapshot(&self) -> SseBusHealthSnapshot {
        SseBusHealthSnapshot {
            active_subscriptions: self.active_count(),
            pending_retry_confirmations: self.confirmation_retry_counts.lock().len(),
            dropped_confirmations_total: self.dropped_confirmations_total.load(Ordering::Acquire),
            db_fetch_failures_total: self.db_fetch_failures_total.load(Ordering::Acquire),
            malformed_confirmations_total: self
                .malformed_confirmations_total
                .load(Ordering::Acquire),
            subscription_reconnects_total: self
                .subscription_reconnects_total
                .load(Ordering::Acquire),
        }
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
        namespace: Option<String>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        self.run_with_ready(nats_client, pool, env, namespace, shutdown, None)
            .await;
    }

    /// Like [`run`](Self::run), but notifies `ready` once the NATS subscription is active.
    /// Useful in tests to avoid racing between subscribe and publish.
    ///
    /// `namespace` MUST match the namespace the paired event_engine publishes
    /// confirmations under (`SINEX_NAMESPACE`): NATS subjects are
    /// namespace-prefixed, so a mismatched (or absent) namespace makes the bus
    /// subscribe to `{default}.events.confirmations.>` while a namespaced
    /// event_engine publishes to `{namespace}.events.confirmations.*`, and SSE
    /// delivery silently never completes.
    pub async fn run_with_ready(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        pool: sqlx::PgPool,
        env: sinex_primitives::environment::SinexEnvironment,
        namespace: Option<String>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        ready: Option<Arc<tokio::sync::Notify>>,
    ) {
        let subject =
            env.nats_subject_with_namespace(namespace.as_deref(), "events.confirmations.>");
        let mut id_buffer: Vec<Id<Event<JsonValue>>> = Vec::with_capacity(BATCH_MAX_IDS);
        let mut batch_timer = tokio::time::interval(BATCH_WINDOW);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ready = ready;
        let mut ready_notified = false;

        'outer: loop {
            let mut sub = loop {
                let subscribe_result = tokio::time::timeout(
                    SUBSCRIBE_ATTEMPT_TIMEOUT,
                    nats_client.subscribe(subject.clone()),
                )
                .await;
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
                    () = tokio::time::sleep(SUBSCRIBE_RETRY_DELAY) => {}
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
                            self.subscription_reconnects_total
                                .fetch_add(1, Ordering::Relaxed);
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
                                self.malformed_confirmations_total
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    error = %error,
                                    payload_len = msg.payload.len(),
                                    payload_fingerprint = %Self::payload_fingerprint(&msg.payload),
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
        conf.event_id.parse().map(Some).map_err(|error| {
            let fingerprint = Self::payload_fingerprint(conf.event_id.as_bytes());
            format!("failed to parse confirmation event_id ({fingerprint}): {error}")
        })
    }

    fn payload_fingerprint(payload: &[u8]) -> String {
        let hash = blake3::hash(payload).to_hex();
        format!("len={} blake3={}", payload.len(), &hash[..16])
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

    /// Apply retry counting to IDs that were not found in the DB.
    /// IDs that exceed the max retry limit are dropped with a warning
    /// instead of being retried indefinitely.
    fn filter_retry_ids(
        &self,
        ids: Vec<Id<Event<JsonValue>>>,
        id_buffer: &mut Vec<Id<Event<JsonValue>>>,
    ) {
        let mut retry_counts = self.confirmation_retry_counts.lock();
        for id in ids {
            let entry = retry_counts.entry(id).or_insert(0);
            *entry += 1;
            if *entry >= CONFIRMATION_RETRY_MAX_ATTEMPTS {
                warn!(
                    event_id = %id,
                    retries = *entry,
                    "Dropping missed confirmation ID after max retries"
                );
                self.dropped_confirmations_total
                    .fetch_add(1, Ordering::Relaxed);
                retry_counts.remove(&id);
            } else {
                id_buffer.push(id);
            }
        }
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
                self.db_fetch_failures_total.fetch_add(1, Ordering::Relaxed);
                warn!(
                    ?e,
                    count = unique_ids.len(),
                    "Failed to fetch events for SSE fan-out; preserving batch for retry"
                );
                self.filter_retry_ids(unique_ids, id_buffer);
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
            self.filter_retry_ids(missing_ids, id_buffer);
        }

        // Snapshot handles so DashMap entry locks are not held during filter evaluation or send.
        let subscriptions = self.snapshot_subscriptions();
        let mut to_remove = Vec::new();

        for (sub_id, slot) in subscriptions {
            for event in &events {
                if !slot.matches_delivery_scope(event) {
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
#[path = "sse_bus_test.rs"]
mod tests;
