//! Subscription bus: fans out confirmed events to SSE clients via in-memory filters.
//!
//! Subscribes to NATS `events.confirmations.>`, batches event IDs over a 20ms window,
//! fetches full events from Postgres (buffer-cache hot), evaluates each client's
//! [`SubscriptionFilter`], and pushes matches into bounded per-client channels.

use dashmap::DashMap;
use futures::StreamExt;
use serde::Serialize;
use sinex_db::DbPoolExt;
use sinex_primitives::events::Event;
use sinex_primitives::query::SubscriptionFilter;
use sinex_primitives::{Id, JsonValue, Timestamp};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Batch window: accumulate confirmation IDs for this duration before DB fetch.
const BATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(20);

/// Maximum IDs to accumulate before forcing a fetch (even within the batch window).
const BATCH_MAX_IDS: usize = 32;

/// Per-client channel capacity. Slow consumers get gap notifications.
const CLIENT_CHANNEL_CAPACITY: usize = 256;

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
    /// Running gap counter — when we fail to send, track how many events were dropped.
    gap_start: Option<u64>,
    gap_count: u64,
}

/// Fan-out bus from NATS confirmations → per-client SSE streams.
pub struct SubscriptionBus {
    subscriptions: Arc<DashMap<u64, SubscriptionSlot>>,
    next_sub_id: AtomicU64,
    next_seq: AtomicU64,
}

impl SubscriptionBus {
    /// Create a new subscription bus.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(DashMap::new()),
            next_sub_id: AtomicU64::new(1),
            next_seq: AtomicU64::new(1),
        }
    }

    /// Register a new subscription. Returns `(sub_id, receiver)`.
    pub fn register(&self, filter: SubscriptionFilter) -> (u64, mpsc::Receiver<SseMessage>) {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(CLIENT_CHANNEL_CAPACITY);
        self.subscriptions.insert(
            id,
            SubscriptionSlot {
                filter,
                tx,
                gap_start: None,
                gap_count: 0,
            },
        );
        debug!(sub_id = id, "SSE subscription registered");
        (id, rx)
    }

    /// Unregister a subscription (client disconnected).
    pub fn unregister(&self, sub_id: u64) {
        if self.subscriptions.remove(&sub_id).is_some() {
            debug!(sub_id, "SSE subscription unregistered");
        }
    }

    /// Number of active subscriptions.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Run the bus loop. Blocks until the shutdown signal fires.
    ///
    /// This subscribes to NATS confirmations, batches IDs, fetches events from DB,
    /// and fans out to all matching client channels.
    pub async fn run(
        self: Arc<Self>,
        nats_client: async_nats::Client,
        pool: sqlx::PgPool,
        env: sinex_primitives::environment::SinexEnvironment,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let subject = env.nats_subject("events.confirmations.>");
        let mut sub = match nats_client.subscribe(subject.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                error!(?e, subject, "Failed to subscribe to confirmations — SSE bus disabled");
                return;
            }
        };

        info!(subject, "SSE subscription bus started");

        let mut id_buffer: Vec<Id<Event<JsonValue>>> = Vec::with_capacity(BATCH_MAX_IDS);
        let mut batch_timer = tokio::time::interval(BATCH_WINDOW);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("SSE bus shutting down");
                        break;
                    }
                }

                msg = sub.next() => {
                    let Some(msg) = msg else {
                        warn!("NATS subscription closed — SSE bus stopping");
                        break;
                    };

                    // Parse confirmation payload: { "event_id": "...", "persisted": true, "ts_ingest": "..." }
                    if let Some(event_id) = Self::parse_confirmation(&msg.payload) {
                        id_buffer.push(event_id);

                        // Flush immediately if buffer full
                        if id_buffer.len() >= BATCH_MAX_IDS {
                            self.flush_batch(&mut id_buffer, &pool).await;
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

        info!("SSE subscription bus stopped");
    }

    /// Parse an event ID from a NATS confirmation message payload.
    fn parse_confirmation(payload: &[u8]) -> Option<Id<Event<JsonValue>>> {
        #[derive(serde::Deserialize)]
        struct Confirmation {
            event_id: String,
            persisted: bool,
        }

        let conf: Confirmation = serde_json::from_slice(payload).ok()?;
        if !conf.persisted {
            return None;
        }
        conf.event_id.parse().ok()
    }

    /// Fetch events from DB and fan out to all matching subscriptions.
    async fn flush_batch(
        &self,
        id_buffer: &mut Vec<Id<Event<JsonValue>>>,
        pool: &sqlx::PgPool,
    ) {
        let ids: Vec<_> = id_buffer.drain(..).collect();
        if ids.is_empty() {
            return;
        }

        // Batch-fetch from DB (buffer-cache hot — events were JUST written)
        let events = match pool.events().get_by_ids(&ids).await {
            Ok(events) => events,
            Err(e) => {
                warn!(?e, count = ids.len(), "Failed to fetch events for SSE fan-out");
                return;
            }
        };

        // Wrap in Arc for zero-copy fan-out to multiple clients
        let events: Vec<Arc<Event<JsonValue>>> = events.into_iter().map(Arc::new).collect();

        // Fan out to all subscriptions
        let mut to_remove = Vec::new();

        for mut slot_ref in self.subscriptions.iter_mut() {
            let sub_id = *slot_ref.key();
            let slot = slot_ref.value_mut();

            for event in &events {
                if !slot.filter.matches(event) {
                    continue;
                }

                let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
                let msg = SseMessage::Event {
                    seq,
                    event: Arc::clone(event),
                };

                match slot.tx.try_send(msg) {
                    Ok(()) => {
                        // If we were in a gap, send the gap notification first
                        if let Some(from_seq) = slot.gap_start.take() {
                            let gap_count = slot.gap_count;
                            slot.gap_count = 0;
                            let gap = SseMessage::Gap {
                                from_seq,
                                to_seq: seq.saturating_sub(1),
                                dropped: gap_count,
                            };
                            // Best-effort gap notification
                            let _ = slot.tx.try_send(gap);
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Slow consumer — track gap
                        if slot.gap_start.is_none() {
                            slot.gap_start = Some(seq);
                        }
                        slot.gap_count += 1;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Client disconnected
                        to_remove.push(sub_id);
                        break;
                    }
                }
            }
        }

        // Clean up disconnected clients
        for sub_id in to_remove {
            self.unregister(sub_id);
        }
    }
}
