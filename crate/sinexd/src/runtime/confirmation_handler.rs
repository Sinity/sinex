//! Confirmation-aware event consumption primitives
//!
//! This module provides the infrastructure for consuming provisional events
//! and processing them after confirmation, with optional immediate provisional processing.

use crate::runtime::RuntimeResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::constants::buffers::DEFAULT_CONFIRMATION_BUFFER_CAPACITY;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::EventId;
use sinex_primitives::JsonValue;
use sinex_primitives::runtime_pressure::RuntimePressureAction;
use sinex_primitives::source_contracts::ResourceBudgetSpec;
use sinex_primitives::units::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::RwLock;

const DEFAULT_CONFIRMATION_BUFFER_PENDING_BYTES: Bytes = Bytes::from_mebibytes(512);
const CONFIRMATION_BUFFER_WARNING_FILL_PCT: usize = 80;
const PAYLOAD_BYTES_REJECTION_REDELIVERY_DELAY: std::time::Duration =
    std::time::Duration::from_secs(2);
const EVENT_CAPACITY_REJECTION_REDELIVERY_DELAY: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Processing model for automata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingModel {
    /// Leader/standby with a single active runtime module.
    /// Uses NATS KV leases for coordination
    LeaderStandby,
    /// Stateless workers processing confirmed events
    /// Multiple instances can run in parallel
    StatelessWorker,
}

/// Provisional event data waiting for confirmation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionalEvent {
    pub event_id: EventId,
    pub source: EventSource,
    pub event_type: EventType,
    pub payload: serde_json::Value,
    pub ts_orig: sinex_primitives::temporal::Timestamp,
    pub received_at: sinex_primitives::temporal::Timestamp,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    event: ProvisionalEvent,
    timed_out_at: Option<sinex_primitives::temporal::Timestamp>,
    payload_bytes: usize,
}

/// Point-in-time diagnostics for the confirmation buffer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmationBufferSnapshot {
    pub pending_count: usize,
    pub timed_out_retained_count: usize,
    pub rejected_count: u64,
    pub late_confirmation_count: u64,
    pub pressure_level: ConfirmationBufferPressureLevel,
    pub runtime_action: RuntimePressureAction,
    pub retained_payload_bytes: usize,
    pub max_payload_bytes: usize,
    pub approximate_payload_bytes: usize,
    pub active_payload_bytes: usize,
    pub timed_out_retained_payload_bytes: usize,
    pub approximate_payload_bytes_by_kind: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationBufferPressureLevel {
    Nominal,
    Warning,
    Critical,
}

impl ConfirmationBufferPressureLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nominal => "nominal",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmationBufferRejectionReason {
    EventCapacity,
    PayloadBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmationBufferInsertDecision {
    pub accepted: bool,
    pub rejection_reason: Option<ConfirmationBufferRejectionReason>,
    pub pressure_level: ConfirmationBufferPressureLevel,
    pub pending_count: usize,
    pub max_capacity: usize,
    pub retained_payload_bytes: usize,
    pub max_payload_bytes: usize,
    pub attempted_payload_bytes: usize,
    pub projected_payload_bytes: usize,
}

impl ConfirmationBufferInsertDecision {
    /// Runtime action represented by this insertion decision.
    ///
    /// This intentionally mirrors the package budget action vocabulary without
    /// introducing a scheduler layer here: callers can log and act on the
    /// decision already made by the confirmation buffer.
    #[must_use]
    pub const fn runtime_action(&self) -> RuntimePressureAction {
        if !self.accepted {
            return RuntimePressureAction::Throttle;
        }
        match self.pressure_level {
            ConfirmationBufferPressureLevel::Nominal => RuntimePressureAction::Admit,
            ConfirmationBufferPressureLevel::Warning
            | ConfirmationBufferPressureLevel::Critical => RuntimePressureAction::AdmitWithPressure,
        }
    }

    /// Redelivery pacing for a rejected provisional message.
    ///
    /// The delay is intentionally finite and deterministic so resource-pressure
    /// response is visible in tests and operator logs instead of being hidden
    /// behind a generic immediate retry.
    #[must_use]
    pub fn rejected_redelivery_delay(&self) -> Option<std::time::Duration> {
        if self.accepted {
            return None;
        }
        let delay = match self.rejection_reason {
            Some(ConfirmationBufferRejectionReason::PayloadBytes) => {
                PAYLOAD_BYTES_REJECTION_REDELIVERY_DELAY
            }
            Some(ConfirmationBufferRejectionReason::EventCapacity) | None => {
                EVENT_CAPACITY_REJECTION_REDELIVERY_DELAY
            }
        };
        Some(delay)
    }

    /// Redelivery delay in milliseconds for diagnostics and structured logs.
    #[must_use]
    pub fn rejected_redelivery_delay_ms(&self) -> Option<u64> {
        self.rejected_redelivery_delay()
            .map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
    }
}

static CONFIRMATION_BUFFER_REGISTRY: OnceLock<Mutex<Vec<Weak<ConfirmationBuffer>>>> =
    OnceLock::new();

/// Register a confirmation buffer for operator health/diagnostics surfaces.
///
/// The registry stores weak refs so diagnostics never extend a runtime buffer's
/// lifetime. Duplicate registrations of the same `Arc` are ignored.
pub fn register_confirmation_buffer(buffer: &Arc<ConfirmationBuffer>) {
    let weak = Arc::downgrade(buffer);
    let registry = CONFIRMATION_BUFFER_REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.retain(|existing| existing.upgrade().is_some());
    if !guard.iter().any(|existing| existing.ptr_eq(&weak)) {
        guard.push(weak);
    }
}

/// Snapshot all live registered confirmation buffers.
///
/// Dead weak refs are discarded before snapshots are taken. Snapshotting occurs
/// outside the registry lock, and each buffer snapshot already keeps its own
/// pending-map lock section short.
pub async fn registered_confirmation_buffer_snapshots() -> Vec<ConfirmationBufferSnapshot> {
    let Some(registry) = CONFIRMATION_BUFFER_REGISTRY.get() else {
        return Vec::new();
    };
    let buffers = {
        let mut guard = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut buffers = Vec::new();
        guard.retain(|weak| {
            if let Some(buffer) = weak.upgrade() {
                buffers.push(buffer);
                true
            } else {
                false
            }
        });
        buffers
    };

    let mut snapshots = Vec::with_capacity(buffers.len());
    for buffer in buffers {
        snapshots.push(buffer.snapshot().await);
    }
    snapshots
}

/// Per-kind confirmation watermark from event_engine. Per #1306: a single message
/// per `(source, event_type)` tells downstream "events of this kind with
/// id ≤ `event_id` are confirmed". Subjects use the kind as the leaf
/// (`<prefix>.<source>.<event_type>`), so `max_messages_per_subject = 1` on the
/// stream actually compacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfirmation {
    /// High-watermark event id for this kind.
    pub event_id: EventId,
    /// Source of the kind (matches `event.source`).
    pub source: String,
    /// Event type of the kind (matches `event.event_type`).
    pub event_type: String,
    pub persisted: bool,
    pub ts_ingest: sinex_primitives::temporal::Timestamp,
}

/// Optional trait for handling provisional events before confirmation
///
/// Automata can implement this to react to events immediately with the understanding
/// that processing may need to be rolled back if confirmation fails or event goes to DLQ.
#[async_trait]
pub trait ProvisionalEventHandler: Send + Sync {
    /// Process a provisional event before confirmation
    ///
    /// This is called as soon as the event arrives on the raw stream.
    /// Implementation should be idempotent and prepared for rollback.
    async fn handle_provisional(&self, event: &ProvisionalEvent) -> RuntimeResult<()>;

    /// Rollback provisional processing if event is not confirmed
    ///
    /// Called when an event goes to DLQ or confirmation timeout occurs.
    async fn rollback_provisional(&self, event_id: EventId) -> RuntimeResult<()>;
}

/// Handler for confirmed events (required)
#[async_trait]
pub trait ConfirmedEventHandler: Send + Sync {
    /// Process a confirmed event
    ///
    /// This is called after the event has been successfully persisted to the database
    /// and confirmation published to `JetStream`.
    async fn handle_confirmed(&self, event: &Event<JsonValue>) -> RuntimeResult<()>;
}

/// Buffer for provisional events awaiting confirmation.
///
/// Locking contract:
/// - the lock protects only the in-memory pending-event map
/// - lock-held sections stay CPU-only (`insert`, `remove`, timeout scan)
/// - NATS, database, and handler callbacks happen after the lock is released
/// - slow acquisition warnings are part of the regression signal and should stay intact
pub struct ConfirmationBuffer {
    /// Provisional events indexed by `event_id`
    pending: Arc<RwLock<HashMap<EventId, PendingEntry>>>,
    /// Per-kind confirmation high-watermark seen on the confirmations stream.
    /// Per #1306: when a provisional event is added whose `(source, event_type)`
    /// already has a watermark `>=` its `event_id`, it is implicitly confirmed
    /// immediately (the confirmation message arrived before the provisional —
    /// would otherwise sit in the buffer until timeout).
    kind_watermarks: Arc<RwLock<HashMap<(String, String), EventId>>>,
    /// Maximum time to wait for confirmation before treating as failure
    timeout: std::time::Duration,
    /// Additional grace period to retain timed-out events so delayed confirmations
    /// can still be matched after temporary confirmation-path failures.
    grace_period: std::time::Duration,
    /// Maximum number of pending events (prevents unbounded memory growth)
    max_capacity: usize,
    /// Maximum retained provisional payload bytes. This prevents a small number
    /// of large provisional events from bypassing the event-count budget.
    max_payload_bytes: usize,
    /// Retained provisional payload bytes across pending entries.
    retained_payload_bytes: AtomicU64,
    /// Counter for rejected events due to capacity limits
    rejected_count: AtomicU64,
    /// Counter for confirmations accepted after timeout while still inside grace.
    late_confirmation_count: AtomicU64,
}

impl ConfirmationBuffer {
    #[must_use]
    pub fn new(timeout: std::time::Duration) -> Self {
        Self::with_capacity(timeout, DEFAULT_CONFIRMATION_BUFFER_CAPACITY)
    }

    #[must_use]
    pub fn with_capacity(timeout: std::time::Duration, max_capacity: usize) -> Self {
        Self::with_capacity_and_grace(timeout, max_capacity, timeout)
    }

    #[must_use]
    pub fn with_capacity_and_grace(
        timeout: std::time::Duration,
        max_capacity: usize,
        grace_period: std::time::Duration,
    ) -> Self {
        Self::with_capacity_grace_and_payload_budget(
            timeout,
            max_capacity,
            grace_period,
            DEFAULT_CONFIRMATION_BUFFER_PENDING_BYTES.as_usize(),
        )
    }

    #[must_use]
    pub fn with_resource_budget(timeout: std::time::Duration, budget: ResourceBudgetSpec) -> Self {
        Self::with_capacity_grace_and_payload_budget(
            timeout,
            resource_budget_pending_candidates(budget),
            timeout,
            resource_budget_pending_payload_bytes(budget),
        )
    }

    #[must_use]
    pub fn with_capacity_grace_and_payload_budget(
        timeout: std::time::Duration,
        max_capacity: usize,
        grace_period: std::time::Duration,
        max_payload_bytes: usize,
    ) -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::with_capacity(
                max_capacity.min(1000), // Pre-allocate reasonably
            ))),
            kind_watermarks: Arc::new(RwLock::new(HashMap::new())),
            timeout,
            grace_period,
            max_capacity,
            max_payload_bytes,
            retained_payload_bytes: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
            late_confirmation_count: AtomicU64::new(0),
        }
    }

    /// Add a provisional event to the buffer
    ///
    /// Returns `false` if the buffer is at capacity and the event was rejected.
    /// Callers should handle this by applying backpressure or logging.
    ///
    /// Per #1306: callers that want to handle the late-confirmation race
    /// (confirmation watermark arrived before the provisional event was added)
    /// should call `try_implicit_confirm_on_add` BEFORE `add_provisional` and
    /// dispatch the confirmed handler synchronously when it returns true.
    #[tracing::instrument(skip(self, event), fields(event_id = %event.event_id, buffer_size))]
    pub async fn add_provisional(&self, event: ProvisionalEvent) -> bool {
        self.add_provisional_with_pressure(event).await.accepted
    }

    /// Add a provisional event and return the resource-pressure decision that
    /// admitted or rejected it.
    #[tracing::instrument(skip(self, event), fields(event_id = %event.event_id, buffer_size))]
    pub async fn add_provisional_with_pressure(
        &self,
        event: ProvisionalEvent,
    ) -> ConfirmationBufferInsertDecision {
        let payload_bytes = serde_json::to_vec(&event.payload).map_or(0, |bytes| bytes.len());
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in add_provisional");
        }

        let existing_payload_bytes = pending
            .get(&event.event_id)
            .map_or(0, |entry| entry.payload_bytes);
        let existing_timed_out_at = pending
            .get(&event.event_id)
            .and_then(|entry| entry.timed_out_at);
        let pending_count = pending.len() + usize::from(!pending.contains_key(&event.event_id));
        let retained_payload_bytes = self.retained_payload_bytes();
        let projected_payload_bytes = retained_payload_bytes
            .saturating_sub(existing_payload_bytes)
            .saturating_add(payload_bytes);
        let decision = self.insert_decision(
            pending_count,
            retained_payload_bytes,
            payload_bytes,
            projected_payload_bytes,
        );

        if let Some(reason) = decision.rejection_reason {
            let rejected = self.rejected_count.fetch_add(1, Ordering::Relaxed);

            // Log periodically to avoid log spam
            if rejected.is_multiple_of(100) {
                tracing::error!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_buffer_rejections_total",
                    max_capacity = self.max_capacity,
                    max_payload_bytes = self.max_payload_bytes,
                    retained_payload_bytes,
                    attempted_payload_bytes = payload_bytes,
                    projected_payload_bytes,
                    reason = ?reason,
                    rejected_total = rejected + 1,
                    event_id = %event.event_id,
                    "ConfirmationBuffer over budget - event rejected (memory protection)"
                );
            }
            return decision;
        }

        // Warn when approaching capacity
        let current_len = pending.len();
        if current_len > 0
            && current_len % 1000 == 0
            && decision.pressure_level != ConfirmationBufferPressureLevel::Nominal
        {
            tracing::warn!(
                current = current_len,
                max = self.max_capacity,
                retained_payload_bytes,
                max_payload_bytes = self.max_payload_bytes,
                "ConfirmationBuffer approaching capacity limit"
            );
        }

        pending.insert(
            event.event_id,
            PendingEntry {
                event,
                timed_out_at: existing_timed_out_at,
                payload_bytes,
            },
        );
        self.set_retained_payload_bytes(projected_payload_bytes);
        tracing::Span::current().record("buffer_size", pending.len());
        decision
    }

    /// Retrieve and remove an event upon confirmation
    #[tracing::instrument(skip(self), fields(buffer_size))]
    pub async fn confirm(&self, event_id: EventId) -> Option<ProvisionalEvent> {
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in confirm");
        }
        let result = pending.remove(&event_id);
        if let Some(entry) = &result {
            self.subtract_retained_payload_bytes(entry.payload_bytes);
        }
        if result
            .as_ref()
            .is_some_and(|entry| entry.timed_out_at.is_some())
        {
            self.record_late_confirmation(pending.len(), None);
        }
        tracing::Span::current().record("buffer_size", pending.len());
        result.map(|entry| entry.event)
    }

    /// Returns Some(event) iff the provisional event's kind already has a
    /// watermark `>=` its `event_id` — i.e. event_engine already confirmed it but the
    /// confirmation arrived before this provisional was buffered. Caller should
    /// treat the returned event as already confirmed.
    pub async fn try_implicit_confirm_on_add(&self, event: &ProvisionalEvent) -> bool {
        let watermarks = self.kind_watermarks.read().await;
        let key = (
            event.source.as_str().to_string(),
            event.event_type.as_str().to_string(),
        );
        watermarks
            .get(&key)
            .is_some_and(|wm| wm.as_uuid() >= event.event_id.as_uuid())
    }

    /// Per-kind watermark confirm. Per #1306: remove and return every pending
    /// event of `(source, event_type)` whose `event_id <= watermark`. This is
    /// the consumer side of event_engine's per-kind watermark compaction — one
    /// message on `events.confirmations.<source>.<event_type>` implicitly
    /// confirms every prior event of that kind. Also advances the per-kind
    /// watermark so future late-arriving provisional events with `event_id ≤
    /// watermark` are recognized as already-confirmed at add time.
    #[tracing::instrument(skip(self), fields(buffer_size, kind_source = %source, kind_event_type = %event_type, confirmed))]
    pub async fn confirm_kind_up_to(
        &self,
        source: &str,
        event_type: &str,
        watermark: EventId,
    ) -> Vec<ProvisionalEvent> {
        // Advance the per-kind watermark first so late-arriving provisional
        // events of the same kind with id <= watermark are recognized as
        // already-confirmed by `try_implicit_confirm_on_add`.
        {
            let mut watermarks = self.kind_watermarks.write().await;
            let key = (source.to_string(), event_type.to_string());
            let advance = watermarks
                .get(&key)
                .is_none_or(|prev| watermark.as_uuid() > prev.as_uuid());
            if advance {
                watermarks.insert(key, watermark);
            }
        }
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in confirm_kind_up_to");
        }
        let matching_ids: Vec<EventId> = pending
            .iter()
            .filter_map(|(event_id, entry)| {
                if event_id.as_uuid() <= watermark.as_uuid()
                    && entry.event.source.as_str() == source
                    && entry.event.event_type.as_str() == event_type
                {
                    Some(*event_id)
                } else {
                    None
                }
            })
            .collect();
        let removed: Vec<PendingEntry> = matching_ids
            .into_iter()
            .filter_map(|id| pending.remove(&id))
            .collect();
        let removed_payload_bytes = removed.iter().map(|entry| entry.payload_bytes).sum();
        self.subtract_retained_payload_bytes(removed_payload_bytes);
        let pending_after_remove = pending.len();
        let confirmed: Vec<ProvisionalEvent> = removed
            .into_iter()
            .map(|entry| {
                let was_timed_out = entry.timed_out_at.is_some();
                let kind = was_timed_out.then(|| {
                    (
                        entry.event.source.as_str().to_string(),
                        entry.event.event_type.as_str().to_string(),
                    )
                });
                if let Some(kind) = kind {
                    self.record_late_confirmation(pending_after_remove, Some(kind));
                }
                entry.event
            })
            .collect();
        tracing::Span::current().record("buffer_size", pending.len());
        tracing::Span::current().record("confirmed", confirmed.len());
        confirmed
    }

    /// Identify newly timed-out events and retain them for the grace window.
    #[tracing::instrument(skip(self), fields(checked_count, timed_out_count))]
    pub async fn check_timeouts(&self) -> Vec<EventId> {
        let mut timed_out = Vec::new();
        let now = sinex_primitives::temporal::now();
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in check_timeouts");
        }
        tracing::Span::current().record("checked_count", pending.len());

        for (event_id, entry) in pending.iter_mut() {
            if entry.timed_out_at.is_some() {
                continue;
            }
            let age = now - entry.event.received_at;
            // Explicitly handle clock skew with a warning.
            match std::time::Duration::try_from(age) {
                Ok(age_std) if age_std > self.timeout => {
                    entry.timed_out_at = Some(now);
                    timed_out.push(*event_id);
                }
                Err(_) => {
                    // Negative duration indicates clock skew
                    tracing::warn!(
                        event_id = %event_id,
                        received_at = %entry.event.received_at,
                        now = %now,
                        "Clock skew detected: event received_at is in the future"
                    );
                    // Don't timeout events with clock skew - they might be valid
                }
                _ => {} // Within timeout window
            }
        }

        tracing::Span::current().record("timed_out_count", timed_out.len());
        timed_out
    }

    /// Remove timed-out events whose grace period has elapsed.
    #[tracing::instrument(skip(self), fields(purged_count))]
    pub async fn purge_expired(&self) -> Vec<ProvisionalEvent> {
        let now = sinex_primitives::temporal::now();
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in purge_expired");
        }

        let expired_ids: Vec<_> = pending
            .iter()
            .filter_map(|(event_id, entry)| {
                let timed_out_at = entry.timed_out_at?;
                let age = now - timed_out_at;
                match std::time::Duration::try_from(age) {
                    Ok(age_std) if age_std >= self.grace_period => Some(*event_id),
                    Err(_) => {
                        tracing::warn!(
                            event_id = %event_id,
                            timed_out_at = %timed_out_at,
                            now = %now,
                            "Clock skew detected while purging timed-out provisional events"
                        );
                        None
                    }
                    _ => None,
                }
            })
            .collect();
        tracing::Span::current().record("purged_count", expired_ids.len());

        expired_ids
            .into_iter()
            .filter_map(|event_id| {
                pending.remove(&event_id).map(|entry| {
                    self.subtract_retained_payload_bytes(entry.payload_bytes);
                    entry.event
                })
            })
            .collect()
    }

    /// Remove timed-out events
    #[tracing::instrument(skip(self, event_ids), fields(remove_count = event_ids.len()))]
    pub async fn remove_timed_out(&self, event_ids: &[EventId]) -> Vec<ProvisionalEvent> {
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in remove_timed_out");
        }
        event_ids
            .iter()
            .filter_map(|id| {
                pending.remove(id).map(|entry| {
                    self.subtract_retained_payload_bytes(entry.payload_bytes);
                    entry.event
                })
            })
            .collect()
    }

    /// Get current buffer size
    pub async fn len(&self) -> usize {
        self.pending.read().await.len()
    }

    /// Check if buffer is empty
    pub async fn is_empty(&self) -> bool {
        self.pending.read().await.is_empty()
    }

    /// Get the number of events rejected due to capacity limits
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(Ordering::Relaxed)
    }

    /// Get the number of late confirmations accepted during the grace period.
    pub fn late_confirmation_count(&self) -> u64 {
        self.late_confirmation_count.load(Ordering::Relaxed)
    }

    /// Get retained payload bytes across pending provisional events.
    pub fn retained_payload_bytes(&self) -> usize {
        self.retained_payload_bytes.load(Ordering::Relaxed) as usize
    }

    /// Get the maximum capacity
    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// Get the maximum retained payload-byte budget.
    pub fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    /// Snapshot confirmation-buffer diagnostics without exposing retained events.
    pub async fn snapshot(&self) -> ConfirmationBufferSnapshot {
        let retained_payload_bytes = self.retained_payload_bytes();
        let (pending_count, rows) = {
            let pending = self.pending.read().await;
            let rows = pending
                .values()
                .map(|entry| {
                    (
                        entry.event.source.as_str().to_string(),
                        entry.event.event_type.as_str().to_string(),
                        entry.payload_bytes,
                        entry.timed_out_at.is_some(),
                    )
                })
                .collect::<Vec<_>>();
            (pending.len(), rows)
        };

        let mut approximate_payload_bytes_by_kind = BTreeMap::new();
        let mut approximate_payload_bytes = 0;
        let mut active_payload_bytes = 0;
        let mut timed_out_retained_payload_bytes = 0;
        let mut timed_out_retained_count = 0;
        for (source, event_type, payload_bytes, timed_out) in rows {
            if timed_out {
                timed_out_retained_count += 1;
                timed_out_retained_payload_bytes += payload_bytes;
            } else {
                active_payload_bytes += payload_bytes;
            }
            approximate_payload_bytes += payload_bytes;
            let key = format!("{source}:{event_type}");
            *approximate_payload_bytes_by_kind.entry(key).or_insert(0) += payload_bytes;
        }
        let mut pressure_level = self
            .insert_decision(
                pending_count,
                retained_payload_bytes,
                0,
                retained_payload_bytes,
            )
            .pressure_level;
        if timed_out_retained_count > 0
            && pressure_level == ConfirmationBufferPressureLevel::Nominal
        {
            pressure_level = ConfirmationBufferPressureLevel::Warning;
        }
        let snapshot_rejection_reason = if pending_count >= self.max_capacity {
            Some(ConfirmationBufferRejectionReason::EventCapacity)
        } else if retained_payload_bytes >= self.max_payload_bytes {
            Some(ConfirmationBufferRejectionReason::PayloadBytes)
        } else {
            None
        };
        let pressure = ConfirmationBufferInsertDecision {
            accepted: snapshot_rejection_reason.is_none(),
            rejection_reason: snapshot_rejection_reason,
            pressure_level,
            pending_count,
            max_capacity: self.max_capacity,
            retained_payload_bytes,
            max_payload_bytes: self.max_payload_bytes,
            attempted_payload_bytes: 0,
            projected_payload_bytes: retained_payload_bytes,
        };

        ConfirmationBufferSnapshot {
            pending_count,
            timed_out_retained_count,
            rejected_count: self.rejected_count(),
            late_confirmation_count: self.late_confirmation_count(),
            pressure_level: pressure.pressure_level,
            runtime_action: pressure.runtime_action(),
            retained_payload_bytes,
            max_payload_bytes: self.max_payload_bytes,
            approximate_payload_bytes,
            active_payload_bytes,
            timed_out_retained_payload_bytes,
            approximate_payload_bytes_by_kind,
        }
    }

    fn insert_decision(
        &self,
        pending_count: usize,
        retained_payload_bytes: usize,
        attempted_payload_bytes: usize,
        projected_payload_bytes: usize,
    ) -> ConfirmationBufferInsertDecision {
        let rejection_reason = if pending_count > self.max_capacity {
            Some(ConfirmationBufferRejectionReason::EventCapacity)
        } else if projected_payload_bytes > self.max_payload_bytes {
            Some(ConfirmationBufferRejectionReason::PayloadBytes)
        } else {
            None
        };
        let pressure_level = if rejection_reason.is_some()
            || pending_count == self.max_capacity
            || projected_payload_bytes == self.max_payload_bytes
        {
            ConfirmationBufferPressureLevel::Critical
        } else if pending_count >= warning_fill(self.max_capacity)
            || projected_payload_bytes >= warning_fill(self.max_payload_bytes)
        {
            ConfirmationBufferPressureLevel::Warning
        } else {
            ConfirmationBufferPressureLevel::Nominal
        };

        ConfirmationBufferInsertDecision {
            accepted: rejection_reason.is_none(),
            rejection_reason,
            pressure_level,
            pending_count,
            max_capacity: self.max_capacity,
            retained_payload_bytes,
            max_payload_bytes: self.max_payload_bytes,
            attempted_payload_bytes,
            projected_payload_bytes,
        }
    }

    fn set_retained_payload_bytes(&self, retained_payload_bytes: usize) {
        self.retained_payload_bytes
            .store(retained_payload_bytes as u64, Ordering::Relaxed);
    }

    fn subtract_retained_payload_bytes(&self, payload_bytes: usize) {
        self.retained_payload_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(payload_bytes as u64))
            })
            .ok();
    }

    fn record_late_confirmation(
        &self,
        pending_after_remove: usize,
        kind: Option<(String, String)>,
    ) {
        let late_total = self.late_confirmation_count.fetch_add(1, Ordering::Relaxed) + 1;
        if should_log_late_confirmation_aggregate(late_total) {
            match kind {
                Some((source, event_type)) => tracing::warn!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_late_total",
                    late_total,
                    pending_after_remove,
                    source,
                    event_type,
                    "Late confirmations accepted after timeout; aggregated during grace period"
                ),
                None => tracing::warn!(
                    target: "sinex_metrics",
                    metric = "runtime.confirmation_late_total",
                    late_total,
                    pending_after_remove,
                    "Late confirmations accepted after timeout; aggregated during grace period"
                ),
            }
        }
    }
}

fn resource_budget_pending_candidates(budget: ResourceBudgetSpec) -> usize {
    usize::try_from(budget.max_pending_candidates)
        .unwrap_or(usize::MAX)
        .max(1)
}

fn resource_budget_pending_payload_bytes(budget: ResourceBudgetSpec) -> usize {
    usize::try_from(budget.max_pending_material_bytes)
        .unwrap_or(usize::MAX)
        .max(1)
}

fn warning_fill(limit: usize) -> usize {
    if limit == 0 {
        return usize::MAX;
    }
    limit
        .saturating_mul(CONFIRMATION_BUFFER_WARNING_FILL_PCT)
        .saturating_add(99)
        / 100
}

fn should_log_late_confirmation_aggregate(late_total: u64) -> bool {
    late_total == 1 || late_total.is_power_of_two() || late_total.is_multiple_of(10_000)
}

#[cfg(test)]
#[path = "confirmation_handler_test.rs"]
mod tests;
