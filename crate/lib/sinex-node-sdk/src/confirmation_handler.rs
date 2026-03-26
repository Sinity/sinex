//! Confirmation-aware event consumption primitives
//!
//! This module provides the infrastructure for consuming provisional events
//! and processing them after confirmation, with optional immediate provisional processing.

use crate::NodeResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::builder::EventId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Processing model for automata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingModel {
    /// Leader/standby with single active node
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
}

/// Event confirmation from ingestd
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfirmation {
    pub event_id: EventId,
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
    async fn handle_provisional(&self, event: &ProvisionalEvent) -> NodeResult<()>;

    /// Rollback provisional processing if event is not confirmed
    ///
    /// Called when an event goes to DLQ or confirmation timeout occurs.
    async fn rollback_provisional(&self, event_id: EventId) -> NodeResult<()>;
}

/// Handler for confirmed events (required)
#[async_trait]
pub trait ConfirmedEventHandler: Send + Sync {
    /// Process a confirmed event
    ///
    /// This is called after the event has been successfully persisted to the database
    /// and confirmation published to `JetStream`.
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()>;
}

/// Default maximum capacity for the confirmation buffer
pub const DEFAULT_MAX_PENDING_EVENTS: usize = 10_000;

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
    /// Maximum time to wait for confirmation before treating as failure
    timeout: std::time::Duration,
    /// Additional grace period to retain timed-out events so delayed confirmations
    /// can still be matched after temporary confirmation-path failures.
    grace_period: std::time::Duration,
    /// Maximum number of pending events (prevents unbounded memory growth)
    max_capacity: usize,
    /// Counter for rejected events due to capacity limits
    rejected_count: std::sync::atomic::AtomicU64,
}

impl ConfirmationBuffer {
    #[must_use]
    pub fn new(timeout: std::time::Duration) -> Self {
        Self::with_capacity(timeout, DEFAULT_MAX_PENDING_EVENTS)
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
        Self {
            pending: Arc::new(RwLock::new(HashMap::with_capacity(
                max_capacity.min(1000), // Pre-allocate reasonably
            ))),
            timeout,
            grace_period,
            max_capacity,
            rejected_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Add a provisional event to the buffer
    ///
    /// Returns `false` if the buffer is at capacity and the event was rejected.
    /// Callers should handle this by applying backpressure or logging.
    #[tracing::instrument(skip(self, event), fields(event_id = %event.event_id, buffer_size))]
    pub async fn add_provisional(&self, event: ProvisionalEvent) -> bool {
        let acquire_start = std::time::Instant::now();
        let mut pending = self.pending.write().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in add_provisional");
        }

        if pending.len() >= self.max_capacity {
            let rejected = self
                .rejected_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Log periodically to avoid log spam
            if rejected.is_multiple_of(100) {
                tracing::error!(
                    max_capacity = self.max_capacity,
                    rejected_total = rejected + 1,
                    event_id = %event.event_id,
                    "ConfirmationBuffer at capacity - event rejected (memory protection)"
                );
            }
            return false;
        }

        // Warn when approaching capacity
        let current_len = pending.len();
        if current_len > 0 && current_len % 1000 == 0 && current_len > self.max_capacity * 8 / 10 {
            tracing::warn!(
                current = current_len,
                max = self.max_capacity,
                "ConfirmationBuffer approaching capacity limit"
            );
        }

        pending.insert(
            event.event_id,
            PendingEntry {
                event,
                timed_out_at: None,
            },
        );
        tracing::Span::current().record("buffer_size", pending.len());
        true
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
        if let Some(entry) = result.as_ref()
            && entry.timed_out_at.is_some()
        {
            tracing::warn!(
                event_id = %event_id,
                "Late confirmation arrived after provisional timeout; accepting during grace period"
            );
        }
        tracing::Span::current().record("buffer_size", pending.len());
        result.map(|entry| entry.event)
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
            .filter_map(|event_id| pending.remove(&event_id).map(|entry| entry.event))
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
            .filter_map(|id| pending.remove(id).map(|entry| entry.event))
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
        self.rejected_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the maximum capacity
    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }
}
