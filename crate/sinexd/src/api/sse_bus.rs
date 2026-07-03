//! Subscription bus: fans out confirmed events to SSE clients via in-memory filters.
//!
//! Subscribes to NATS `events.confirmed.>` and receives the full post-redaction
//! `Event<JsonValue>` directly from the confirmed-events stream — no Postgres
//! refetch, no commit/confirmation visibility race (the #2187 / #2202
//! confirmed-delivery redesign). Tombstone-suppression is preserved upstream:
//! the event engine never publishes a confirmed event for a tombstoned id, so
//! tombstoned events never reach this bus. Each confirmed event is evaluated
//! against every client's source/type/host subscription scope and pushed into
//! bounded per-client channels. Payload predicates are evaluated after view
//! disclosure in the SSE handler so raw payload fields cannot be inferred by
//! filtering.

use dashmap::DashMap;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use sinex_primitives::events::Event;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::views::CaveatView;
use sinex_primitives::{Id, JsonValue, Timestamp};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const SUBSCRIBE_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const SUBSCRIBE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// Per-client channel capacity. Slow consumers get gap notifications.
const CLIENT_CHANNEL_CAPACITY: usize = 256;
const RECENT_DELIVERED_EVENT_IDS: usize = 1024;

/// Hard cap on concurrent SSE subscriptions. Each one owns a buffered channel,
/// so leaving this unbounded makes memory exhaustion trivial.
pub const MAX_ACTIVE_SUBSCRIPTIONS: usize = 512;

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
    dropped_confirmations_total: AtomicU64,
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
            dropped_confirmations_total: AtomicU64::new(0),
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
            // Direct confirmed-event delivery has no DB-refetch fallback path.
            pending_retry_confirmations: 0,
            dropped_confirmations_total: self.dropped_confirmations_total.load(Ordering::Acquire),
            db_fetch_failures_total: 0,
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
    /// This subscribes to NATS `events.confirmed.>`, deserializes each full
    /// confirmed event, and fans out to all matching client channels.
    ///
    /// If `ready` is provided, it will be notified once the NATS subscription is active.
    pub async fn run(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        env: sinex_primitives::environment::SinexEnvironment,
        namespace: Option<String>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        self.run_with_ready(nats_client, env, namespace, shutdown, None)
            .await;
    }

    /// Like [`run`](Self::run), but notifies `ready` once the NATS subscription is active.
    /// Useful in tests to avoid racing between subscribe and publish.
    ///
    /// `namespace` MUST match the namespace the paired event_engine publishes
    /// confirmed events under (`SINEX_NAMESPACE`): NATS subjects are
    /// namespace-prefixed, so a mismatched (or absent) namespace makes the bus
    /// subscribe to `{default}.events.confirmed.>` while a namespaced event_engine
    /// publishes to `{namespace}.events.confirmed.*`, and SSE delivery silently
    /// never completes.
    pub async fn run_with_ready(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        env: sinex_primitives::environment::SinexEnvironment,
        namespace: Option<String>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        ready: Option<Arc<tokio::sync::Notify>>,
    ) {
        let subject = env.nats_subject_with_namespace(namespace.as_deref(), "events.confirmed.>");
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
                            "Failed to subscribe to confirmed events — retrying"
                        );
                    }
                    Err(_) => {
                        warn!(
                            subject,
                            subscribe_timeout_ms = SUBSCRIBE_ATTEMPT_TIMEOUT.as_millis(),
                            retry_delay_ms = SUBSCRIBE_RETRY_DELAY.as_millis(),
                            "Timed out subscribing to confirmed events — retrying"
                        );
                    }
                }
                tokio::select! {
                    shutdown_result = shutdown.changed() => {
                        if shutdown_result.is_err() {
                            warn!("SSE bus shutdown channel dropped before explicit shutdown");
                        }
                        if shutdown_result.is_err() || *shutdown.borrow() {
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
                            continue 'outer;
                        };

                        match Self::parse_confirmed_event(&msg.payload) {
                            Ok(event) => self.fan_out_event(Arc::new(event)),
                            Err(error) => {
                                self.malformed_confirmations_total
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    error = %error,
                                    payload_len = msg.payload.len(),
                                    payload_fingerprint = %Self::payload_fingerprint(&msg.payload),
                                    "Ignoring malformed SSE confirmed-event payload"
                                );
                            }
                        }
                    }
                }
            }
        }

        info!("SSE subscription bus stopped");
    }

    /// Parse a full confirmed `Event<JsonValue>` from a confirmed-events payload.
    fn parse_confirmed_event(payload: &[u8]) -> Result<Event<JsonValue>, String> {
        serde_json::from_slice(payload)
            .map_err(|error| format!("failed to parse confirmed event JSON: {error}"))
    }

    fn payload_fingerprint(payload: &[u8]) -> String {
        let hash = blake3::hash(payload).to_hex();
        format!("len={} blake3={}", payload.len(), &hash[..16])
    }

    fn snapshot_subscriptions(&self) -> Vec<(u64, Arc<SubscriptionSlot>)> {
        self.subscriptions
            .iter()
            .map(|slot| (*slot.key(), Arc::clone(slot.value())))
            .collect()
    }

    /// Fan out a single confirmed event to all matching subscriptions.
    ///
    /// The event arrives fully materialized from the confirmed-events stream, so
    /// there is no DB fetch, dedup-by-id, or retry — just scope evaluation and
    /// per-client delivery.
    fn fan_out_event(&self, event: Arc<Event<JsonValue>>) {
        // Snapshot handles so DashMap entry locks are not held during filter evaluation or send.
        let subscriptions = self.snapshot_subscriptions();
        let mut to_remove = Vec::new();

        for (sub_id, slot) in subscriptions {
            if !slot.matches_delivery_scope(&event) {
                continue;
            }
            if matches!(slot.deliver(&event), DeliveryOutcome::Closed) {
                to_remove.push(sub_id);
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
