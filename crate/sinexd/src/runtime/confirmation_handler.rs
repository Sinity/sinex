//! Confirmation-aware event consumption primitives
//!
//! This module provides the infrastructure for consuming provisional events
//! and processing them after confirmation, with optional immediate provisional processing.

use crate::runtime::RuntimeResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::constants::buffers::DEFAULT_CONFIRMATION_BUFFER_CAPACITY;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use sinex_primitives::source_contracts::ResourceBudgetSpec;
use sinex_primitives::units::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tokio::sync::RwLock;

const DEFAULT_CONFIRMATION_BUFFER_PENDING_BYTES: Bytes = Bytes::from_mebibytes(512);
const CONFIRMATION_BUFFER_WARNING_FILL_PCT: usize = 80;

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
    pub runtime_action: String,
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
    pub const fn runtime_action(&self) -> &'static str {
        if !self.accepted {
            return "throttle";
        }
        match self.pressure_level {
            ConfirmationBufferPressureLevel::Nominal => "admit",
            ConfirmationBufferPressureLevel::Warning
            | ConfirmationBufferPressureLevel::Critical => "admit_with_pressure",
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
                std::time::Duration::from_secs(2)
            }
            Some(ConfirmationBufferRejectionReason::EventCapacity) | None => {
                std::time::Duration::from_millis(500)
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
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> RuntimeResult<()>;
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
            runtime_action: pressure.runtime_action().to_string(),
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
mod tests {
    use super::*;
    use crate::runtime::parser::{MaterialParser, records_from_journal_lines};
    use crate::sources::source_contracts::system::journald::JournaldParser;
    use serde_json::Value;
    use serde_json::json;
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceId};
    use sinex_primitives::primitives::Uuid;
    use sinex_primitives::source_contracts::{BudgetPressureAction, WorkClass};
    use sinex_primitives::temporal::Timestamp;
    use sinex_primitives::{Event, JsonValue};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::fmt::MakeWriter;
    use xtask::sandbox::{TestResult, sinex_test};

    static TEST_PRESSURE_ACTIONS: &[BudgetPressureAction] = &[BudgetPressureAction::Inspect];

    fn test_budget(
        max_pending_candidates: u32,
        max_pending_material_bytes: u64,
    ) -> ResourceBudgetSpec {
        ResourceBudgetSpec {
            work_class: WorkClass::ProjectionHot,
            steady_memory_mib: 1,
            burst_memory_mib: 1,
            cpu_weight: 100,
            max_input_bytes_per_sec: None,
            max_input_events_per_sec: None,
            max_pending_material_bytes,
            max_pending_candidates,
            max_unacked_transport_messages: None,
            batch_size: None,
            flush_interval_ms: None,
            checkpoint_interval_ms: None,
            expected_disk_write_bytes_per_min: None,
            expected_wal_write_bytes_per_min: None,
            pressure_actions: TEST_PRESSURE_ACTIONS,
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn output(&self) -> String {
            let bytes = self.bytes.lock().expect("captured log mutex poisoned");
            String::from_utf8(bytes.clone()).expect("tracing output should be UTF-8")
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                bytes: Arc::clone(&self.bytes),
            }
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.bytes
                .lock()
                .expect("captured log mutex poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn event_id() -> EventId {
        Id::<Event<JsonValue>>::new()
    }

    fn provisional(
        source: &str,
        event_type: &str,
        received_at: Timestamp,
        payload: serde_json::Value,
    ) -> ProvisionalEvent {
        ProvisionalEvent {
            event_id: event_id(),
            source: EventSource::new(source).expect("test source must be valid"),
            event_type: EventType::new(event_type).expect("test event type must be valid"),
            payload,
            ts_orig: received_at,
            received_at,
        }
    }

    fn journal_parser_ctx(mid: Id<SourceMaterial>) -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("system.journald"),
            source_material_id: mid,
            record_anchor: MaterialAnchor::Line {
                byte_start: 0,
                line: 1,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn payload_bytes(payload: &Value) -> TestResult<usize> {
        Ok(serde_json::to_vec(payload)?.len())
    }

    #[sinex_test]
    async fn payload_budget_admits_at_limit_rejects_over_limit_and_recovers() -> TestResult<()> {
        let now = Timestamp::now();
        let at_limit_payload = json!({ "MESSAGE": "fits exactly at the byte budget" });
        let over_limit_payload = json!({ "MESSAGE": "this would exceed the retained byte budget" });
        let max_payload_bytes = payload_bytes(&at_limit_payload)?;
        let at_limit = provisional(
            "system.journald",
            "journald.entry.written",
            now,
            at_limit_payload,
        );
        let over_limit = provisional(
            "system.journald",
            "journald.entry.written",
            now,
            over_limit_payload,
        );
        let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
            Duration::from_secs(60),
            16,
            Duration::from_secs(60),
            max_payload_bytes,
        );

        let admitted = buffer.add_provisional_with_pressure(at_limit.clone()).await;
        assert!(admitted.accepted);
        assert_eq!(admitted.rejection_reason, None);
        assert_eq!(admitted.runtime_action(), "admit_with_pressure");
        assert_eq!(admitted.rejected_redelivery_delay_ms(), None);
        assert_eq!(
            admitted.pressure_level,
            ConfirmationBufferPressureLevel::Critical
        );
        assert_eq!(admitted.projected_payload_bytes, max_payload_bytes);
        assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

        let rejected = buffer
            .add_provisional_with_pressure(over_limit.clone())
            .await;
        assert!(!rejected.accepted);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::PayloadBytes)
        );
        assert_eq!(
            rejected.rejected_redelivery_delay(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(2_000));
        assert_eq!(rejected.runtime_action(), "throttle");
        assert_eq!(
            rejected.pressure_level,
            ConfirmationBufferPressureLevel::Critical
        );
        assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);
        assert_eq!(buffer.rejected_count(), 1);
        let saturated_snapshot = buffer.snapshot().await;
        assert_eq!(
            saturated_snapshot.pressure_level,
            ConfirmationBufferPressureLevel::Critical
        );
        assert_eq!(saturated_snapshot.runtime_action, "throttle");

        let confirmed = buffer.confirm(at_limit.event_id).await.ok_or_else(|| {
            color_eyre::eyre::eyre!("expected at-limit event to remain confirmable")
        })?;
        assert_eq!(confirmed.event_id, at_limit.event_id);
        assert_eq!(buffer.retained_payload_bytes(), 0);

        let recovered = buffer.add_provisional_with_pressure(at_limit).await;
        assert!(recovered.accepted);
        assert_eq!(recovered.rejection_reason, None);
        assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

        Ok(())
    }

    #[sinex_test]
    async fn event_capacity_rejection_uses_short_resource_backoff() -> TestResult<()> {
        let now = Timestamp::now();
        let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
            Duration::from_secs(60),
            1,
            Duration::from_secs(60),
            1024 * 1024,
        );
        let admitted = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                now,
                json!({ "MESSAGE": "first" }),
            ))
            .await;
        let rejected = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                now,
                json!({ "MESSAGE": "second" }),
            ))
            .await;

        assert_eq!(admitted.runtime_action(), "admit_with_pressure");
        assert_eq!(admitted.rejected_redelivery_delay(), None);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::EventCapacity)
        );
        assert_eq!(
            rejected.rejected_redelivery_delay(),
            Some(Duration::from_millis(500))
        );
        assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(500));
        assert_eq!(rejected.runtime_action(), "throttle");
        let saturated_snapshot = buffer.snapshot().await;
        assert_eq!(
            saturated_snapshot.pressure_level,
            ConfirmationBufferPressureLevel::Critical
        );
        assert_eq!(saturated_snapshot.runtime_action, "throttle");
        Ok(())
    }

    #[sinex_test]
    async fn resource_budget_sets_candidate_and_payload_runtime_limits() -> TestResult<()> {
        let now = Timestamp::now();
        let first_payload = json!({ "MESSAGE": "accepted by exact budget" });
        let first_payload_bytes = payload_bytes(&first_payload)?;
        let buffer = ConfirmationBuffer::with_resource_budget(
            Duration::from_secs(60),
            test_budget(1, u64::try_from(first_payload_bytes)?),
        );

        assert_eq!(buffer.max_capacity(), 1);
        assert_eq!(buffer.max_payload_bytes(), first_payload_bytes);

        let admitted = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                now,
                first_payload,
            ))
            .await;
        assert!(admitted.accepted);
        assert_eq!(admitted.rejection_reason, None);
        assert_eq!(buffer.retained_payload_bytes(), first_payload_bytes);

        let rejected = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                now,
                json!({ "MESSAGE": "second event exceeds candidate budget" }),
            ))
            .await;
        assert!(!rejected.accepted);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::EventCapacity)
        );

        Ok(())
    }

    #[sinex_test]
    async fn resource_budget_rejects_payloads_above_material_byte_budget() -> TestResult<()> {
        let now = Timestamp::now();
        let buffer =
            ConfirmationBuffer::with_resource_budget(Duration::from_secs(60), test_budget(8, 1));

        let rejected = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                now,
                json!({ "MESSAGE": "larger than one byte" }),
            ))
            .await;

        assert!(!rejected.accepted);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::PayloadBytes)
        );
        assert_eq!(buffer.retained_payload_bytes(), 0);

        Ok(())
    }

    #[sinex_test]
    async fn payload_budget_accounts_for_same_event_replacement() -> TestResult<()> {
        let now = Timestamp::now();
        let initial_payload = json!({ "MESSAGE": "small" });
        let replacement_payload = json!({ "MESSAGE": "larger replacement payload" });
        let oversized_payload = json!({ "MESSAGE": "oversized replacement payload".repeat(16) });
        let max_payload_bytes = payload_bytes(&replacement_payload)?;
        let initial = provisional(
            "system.journald",
            "journald.entry.written",
            now,
            initial_payload,
        );
        let replacement = ProvisionalEvent {
            payload: replacement_payload,
            ..initial.clone()
        };
        let oversized = ProvisionalEvent {
            payload: oversized_payload,
            ..initial.clone()
        };
        let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
            Duration::from_secs(60),
            1,
            Duration::from_secs(60),
            max_payload_bytes,
        );

        assert!(
            buffer
                .add_provisional_with_pressure(initial.clone())
                .await
                .accepted
        );
        let replaced = buffer.add_provisional_with_pressure(replacement).await;
        assert!(replaced.accepted);
        assert_eq!(replaced.pending_count, 1);
        assert_eq!(replaced.projected_payload_bytes, max_payload_bytes);
        assert_eq!(buffer.len().await, 1);
        assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

        let rejected = buffer.add_provisional_with_pressure(oversized).await;
        assert!(!rejected.accepted);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::PayloadBytes)
        );
        assert_eq!(buffer.len().await, 1);
        assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

        Ok(())
    }

    #[sinex_test]
    async fn same_event_replacement_preserves_timeout_grace_state() -> TestResult<()> {
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let initial = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({ "MESSAGE": "original" }),
        );
        let replacement = ProvisionalEvent {
            payload: json!({ "MESSAGE": "redelivered replacement" }),
            ..initial.clone()
        };
        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_millis(0),
            1,
            Duration::from_millis(0),
        );

        assert!(buffer.add_provisional(initial).await);
        assert_eq!(buffer.check_timeouts().await, vec![replacement.event_id]);

        let replaced = buffer.add_provisional_with_pressure(replacement).await;
        assert!(replaced.accepted);
        let retained = buffer.snapshot().await;
        assert_eq!(retained.pending_count, 1);
        assert_eq!(retained.timed_out_retained_count, 1);

        let purged = buffer.purge_expired().await;
        assert_eq!(purged.len(), 1);
        assert_eq!(buffer.len().await, 0);

        Ok(())
    }

    #[sinex_test]
    async fn snapshot_reports_pending_timeout_rejections_and_payload_bytes() -> TestResult<()> {
        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_millis(0),
            2,
            Duration::from_secs(60),
        );
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let first = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({ "MESSAGE": "Late confirmation arrived after provisional timeout" }),
        );
        let second = provisional(
            "sinexd.event_engine",
            "batch.stats",
            old,
            json!({ "events_processed": 42 }),
        );
        let rejected = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({ "MESSAGE": "should be rejected at capacity" }),
        );

        assert!(buffer.add_provisional(first).await);
        assert!(buffer.add_provisional(second).await);
        assert!(!buffer.add_provisional(rejected).await);
        let timed_out = buffer.check_timeouts().await;
        assert_eq!(timed_out.len(), 2);

        let snapshot = buffer.snapshot().await;
        assert_eq!(snapshot.pending_count, 2);
        assert_eq!(snapshot.timed_out_retained_count, 2);
        assert_eq!(snapshot.rejected_count, 1);
        assert_eq!(snapshot.late_confirmation_count, 0);
        assert!(snapshot.approximate_payload_bytes > 0);
        assert_eq!(snapshot.active_payload_bytes, 0);
        assert_eq!(
            snapshot.timed_out_retained_payload_bytes,
            snapshot.approximate_payload_bytes
        );
        assert_eq!(
            snapshot.retained_payload_bytes,
            snapshot.approximate_payload_bytes
        );
        assert_eq!(snapshot.max_payload_bytes, buffer.max_payload_bytes());
        assert!(
            snapshot
                .approximate_payload_bytes_by_kind
                .contains_key("system.journald:journald.entry.written")
        );
        assert!(
            snapshot
                .approximate_payload_bytes_by_kind
                .contains_key("sinexd.event_engine:batch.stats")
        );

        Ok(())
    }

    #[sinex_test]
    async fn snapshot_splits_active_and_timed_out_retained_payload_bytes() -> TestResult<()> {
        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_secs(60),
            2,
            Duration::from_secs(60),
        );
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let current = Timestamp::now();
        let timed_out_payload = json!({ "MESSAGE": "delayed confirmation retained in grace" });
        let active_payload = json!({ "MESSAGE": "fresh provisional event" });
        let timed_out_bytes = payload_bytes(&timed_out_payload)?;
        let active_bytes = payload_bytes(&active_payload)?;

        assert!(
            buffer
                .add_provisional(provisional(
                    "system.journald",
                    "journald.entry.written",
                    old,
                    timed_out_payload,
                ))
                .await
        );
        assert!(
            buffer
                .add_provisional(provisional(
                    "sinexd.event_engine",
                    "batch.stats",
                    current,
                    active_payload,
                ))
                .await
        );

        let timed_out = buffer.check_timeouts().await;
        assert_eq!(timed_out.len(), 1);

        let snapshot = buffer.snapshot().await;
        assert_eq!(snapshot.pending_count, 2);
        assert_eq!(snapshot.timed_out_retained_count, 1);
        assert_eq!(snapshot.active_payload_bytes, active_bytes);
        assert_eq!(snapshot.timed_out_retained_payload_bytes, timed_out_bytes);
        assert_eq!(
            snapshot.approximate_payload_bytes,
            active_bytes + timed_out_bytes
        );
        assert_eq!(
            snapshot.retained_payload_bytes,
            snapshot.approximate_payload_bytes
        );

        Ok(())
    }

    #[sinex_test]
    async fn watermark_late_confirmations_are_counted_without_retaining_backlog() -> TestResult<()>
    {
        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_millis(0),
            16,
            Duration::from_secs(60),
        );
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let first = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({ "MESSAGE": "late confirmation 1" }),
        );
        let second = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({ "MESSAGE": "late confirmation 2" }),
        );
        let watermark = if first.event_id.as_uuid() > second.event_id.as_uuid() {
            first.event_id
        } else {
            second.event_id
        };

        assert!(buffer.add_provisional(first).await);
        assert!(buffer.add_provisional(second).await);
        assert_eq!(buffer.check_timeouts().await.len(), 2);

        let confirmed = buffer
            .confirm_kind_up_to("system.journald", "journald.entry.written", watermark)
            .await;

        assert_eq!(confirmed.len(), 2);
        let snapshot = buffer.snapshot().await;
        assert_eq!(snapshot.pending_count, 0);
        assert_eq!(snapshot.timed_out_retained_count, 0);
        assert_eq!(snapshot.late_confirmation_count, 2);

        Ok(())
    }

    #[sinex_test]
    async fn timed_out_journald_payload_retention_is_bounded_by_capacity_and_grace()
    -> TestResult<()> {
        const CAPACITY: usize = 16;
        const OVERFLOW_ATTEMPTS: usize = 32;

        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_millis(0),
            CAPACITY,
            Duration::from_millis(0),
        );
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let feedback_payload = "Late confirmation arrived after provisional timeout ".repeat(64);

        for index in 0..CAPACITY {
            assert!(
                buffer
                    .add_provisional(provisional(
                        "system.journald",
                        "journald.entry.written",
                        old,
                        json!({
                            "MESSAGE": feedback_payload,
                            "SEQ": index,
                            "_SYSTEMD_UNIT": "sinexd.service"
                        }),
                    ))
                    .await
            );
        }
        for index in 0..OVERFLOW_ATTEMPTS {
            assert!(
                !buffer
                    .add_provisional(provisional(
                        "system.journald",
                        "journald.entry.written",
                        old,
                        json!({
                            "MESSAGE": feedback_payload,
                            "SEQ": CAPACITY + index,
                            "_SYSTEMD_UNIT": "sinexd.service"
                        }),
                    ))
                    .await
            );
        }

        assert_eq!(buffer.check_timeouts().await.len(), CAPACITY);
        let retained = buffer.snapshot().await;
        assert_eq!(retained.pending_count, CAPACITY);
        assert_eq!(retained.timed_out_retained_count, CAPACITY);
        assert_eq!(retained.rejected_count, OVERFLOW_ATTEMPTS as u64);
        assert!(retained.approximate_payload_bytes > 0);
        assert_eq!(retained.active_payload_bytes, 0);
        assert_eq!(
            retained.timed_out_retained_payload_bytes,
            retained.approximate_payload_bytes
        );
        assert_eq!(
            retained.retained_payload_bytes,
            retained.approximate_payload_bytes
        );
        assert_eq!(retained.max_payload_bytes, buffer.max_payload_bytes());
        assert_eq!(
            retained
                .approximate_payload_bytes_by_kind
                .get("system.journald:journald.entry.written"),
            Some(&retained.approximate_payload_bytes)
        );

        let purged = buffer.purge_expired().await;
        assert_eq!(purged.len(), CAPACITY);
        let drained = buffer.snapshot().await;
        assert_eq!(drained.pending_count, 0);
        assert_eq!(drained.timed_out_retained_count, 0);
        assert_eq!(drained.retained_payload_bytes, 0);
        assert_eq!(drained.max_payload_bytes, buffer.max_payload_bytes());
        assert_eq!(drained.approximate_payload_bytes, 0);
        assert_eq!(drained.active_payload_bytes, 0);
        assert_eq!(drained.timed_out_retained_payload_bytes, 0);
        assert!(drained.approximate_payload_bytes_by_kind.is_empty());
        assert_eq!(drained.rejected_count, OVERFLOW_ATTEMPTS as u64);

        Ok(())
    }

    #[sinex_test]
    async fn delayed_confirmation_feedback_logs_are_sparse_and_journald_suppressed()
    -> TestResult<()> {
        const LATE_EVENTS: usize = 20;
        const OVERFLOW_ATTEMPTS: usize = 8;
        let buffer = ConfirmationBuffer::with_capacity_and_grace(
            Duration::from_millis(0),
            LATE_EVENTS,
            Duration::from_secs(60),
        );
        let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
        let mut watermark = None;

        for index in 0..LATE_EVENTS {
            let event = provisional(
                "system.journald",
                "journald.entry.written",
                old,
                json!({
                    "MESSAGE": format!("feedback candidate {index}"),
                    "_SYSTEMD_UNIT": "sinexd.service"
                }),
            );
            watermark = Some(watermark.map_or(event.event_id, |previous: EventId| {
                if event.event_id.as_uuid() > previous.as_uuid() {
                    event.event_id
                } else {
                    previous
                }
            }));
            assert!(buffer.add_provisional(event).await);
        }
        for index in 0..OVERFLOW_ATTEMPTS {
            let rejected = buffer
                .add_provisional_with_pressure(provisional(
                    "system.journald",
                    "journald.entry.written",
                    old,
                    json!({
                        "MESSAGE": format!("overflow feedback candidate {index}"),
                        "_SYSTEMD_UNIT": "sinexd.service"
                    }),
                ))
                .await;
            assert!(!rejected.accepted);
            assert_eq!(
                rejected.rejection_reason,
                Some(ConfirmationBufferRejectionReason::EventCapacity)
            );
            assert_eq!(rejected.runtime_action(), "throttle");
            assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(500));
        }

        assert_eq!(buffer.check_timeouts().await.len(), LATE_EVENTS);
        let before = buffer.snapshot().await;
        assert_eq!(before.pending_count, LATE_EVENTS);
        assert_eq!(before.timed_out_retained_count, LATE_EVENTS);
        assert_eq!(before.rejected_count, OVERFLOW_ATTEMPTS as u64);
        assert_eq!(before.active_payload_bytes, 0);
        assert!(before.timed_out_retained_payload_bytes > 0);
        assert_eq!(
            before.retained_payload_bytes,
            before.timed_out_retained_payload_bytes
        );
        assert_eq!(before.runtime_action, "throttle");

        let captured = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .with_writer(captured.clone())
            .finish();

        {
            let _guard = tracing::subscriber::set_default(subscriber);
            buffer
                .confirm_kind_up_to(
                    "system.journald",
                    "journald.entry.written",
                    watermark.expect("watermark set"),
                )
                .await;
        }

        let after = buffer.snapshot().await;
        assert_eq!(after.pending_count, 0);
        assert_eq!(after.timed_out_retained_count, 0);
        assert_eq!(after.late_confirmation_count, LATE_EVENTS as u64);
        assert_eq!(after.rejected_count, OVERFLOW_ATTEMPTS as u64);
        assert_eq!(after.retained_payload_bytes, 0);
        assert_eq!(after.approximate_payload_bytes, 0);
        assert_eq!(after.active_payload_bytes, 0);
        assert_eq!(after.timed_out_retained_payload_bytes, 0);
        assert!(
            after.approximate_payload_bytes_by_kind.is_empty(),
            "confirmed backlog should not leave payload attribution behind"
        );

        let log_output = captured.output();
        let feedback_lines = log_output
            .lines()
            .filter(|line| line.contains("Late confirmations accepted after timeout"))
            .collect::<Vec<_>>();
        assert_eq!(
            feedback_lines.len(),
            5,
            "20 late confirmations should log only totals 1,2,4,8,16: {log_output}"
        );
        assert!(
            log_output.contains("runtime.confirmation_late_total"),
            "aggregate feedback log should carry the metric field: {log_output}"
        );

        let mid = Id::<SourceMaterial>::new();
        let journal_lines = feedback_lines
            .iter()
            .enumerate()
            .map(|(index, line)| {
                json!({
                    "__CURSOR": format!("s=feedback;i={index}"),
                    "__REALTIME_TIMESTAMP": format!("{}", 1_700_000_000_000_000_i64 + index as i64),
                    "_SYSTEMD_UNIT": "sinexd.service",
                    "SYSLOG_IDENTIFIER": "sinexd",
                    "MESSAGE": line,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        let line_refs = journal_lines.iter().map(String::as_str).collect::<Vec<_>>();
        let records = records_from_journal_lines(mid, &line_refs);
        let mut parser = JournaldParser;
        let ctx = journal_parser_ctx(mid);

        for record in records {
            let intents = parser
                .parse_record(record.expect("journal record should parse"), &ctx)
                .await
                .expect("journald parser should parse feedback-shaped JSON");
            assert!(
                intents.is_empty(),
                "confirmation feedback journal entry should be suppressed"
            );
        }

        let ordinary = json!({
            "__CURSOR": "s=ordinary;i=1",
            "__REALTIME_TIMESTAMP": "1700000000001000",
            "_SYSTEMD_UNIT": "sinexd.service",
            "SYSLOG_IDENTIFIER": "sinexd",
            "MESSAGE": "source catalog exported",
        })
        .to_string();
        let ordinary_records = records_from_journal_lines(mid, &[ordinary.as_str()]);
        let ordinary_intents = parser
            .parse_record(
                ordinary_records[0]
                    .as_ref()
                    .expect("ordinary journal record should parse")
                    .clone(),
                &ctx,
            )
            .await
            .expect("ordinary sinexd journal entry should parse");
        assert_eq!(ordinary_intents.len(), 1);
        assert_eq!(
            ordinary_intents[0].payload["message"],
            Value::from("source catalog exported")
        );

        Ok(())
    }

    #[sinex_test]
    async fn late_confirmation_aggregate_log_schedule_is_sparse() -> TestResult<()> {
        assert!(should_log_late_confirmation_aggregate(1));
        assert!(should_log_late_confirmation_aggregate(2));
        assert!(should_log_late_confirmation_aggregate(1024));
        assert!(should_log_late_confirmation_aggregate(10_000));
        assert!(!should_log_late_confirmation_aggregate(3));
        assert!(!should_log_late_confirmation_aggregate(9_999));
        assert!(!should_log_late_confirmation_aggregate(10_001));

        Ok(())
    }
}
