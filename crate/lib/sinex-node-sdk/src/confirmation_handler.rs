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
    /// Leader/standby with single active processor
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
    /// and confirmation published to JetStream.
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()>;
}

/// Default maximum capacity for the confirmation buffer
pub const DEFAULT_MAX_PENDING_EVENTS: usize = 10_000;

/// Buffer for provisional events awaiting confirmation
///
/// # Lock Contention Analysis
///
/// This buffer uses RwLock with minimal contention risk:
/// - Lock-free critical sections: HashMap insert/remove (~300ns each)
/// - No nested locks or I/O during lock hold
/// - Read-heavy (check_timeouts uses shared lock)
/// - Instrumentation: logs if lock acquisition exceeds 10ms
///
/// For detailed analysis, see `docs/current/analysis/lock-contention-analysis.md`
pub struct ConfirmationBuffer {
    /// Provisional events indexed by event_id
    pending: Arc<RwLock<HashMap<EventId, ProvisionalEvent>>>,
    /// Maximum time to wait for confirmation before treating as failure
    timeout: std::time::Duration,
    /// Maximum number of pending events (prevents unbounded memory growth)
    max_capacity: usize,
    /// Counter for rejected events due to capacity limits
    rejected_count: std::sync::atomic::AtomicU64,
}

impl ConfirmationBuffer {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self::with_capacity(timeout, DEFAULT_MAX_PENDING_EVENTS)
    }

    pub fn with_capacity(timeout: std::time::Duration, max_capacity: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::with_capacity(
                max_capacity.min(1000), // Pre-allocate reasonably
            ))),
            timeout,
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

        pending.insert(event.event_id, event);
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
        tracing::Span::current().record("buffer_size", pending.len());
        result
    }

    /// Check if an event has timed out
    #[tracing::instrument(skip(self), fields(checked_count, timed_out_count))]
    pub async fn check_timeouts(&self) -> Vec<EventId> {
        let mut timed_out = Vec::new();
        let now = sinex_primitives::temporal::now();
        let acquire_start = std::time::Instant::now();
        let pending = self.pending.read().await;
        let acquire_ms = acquire_start.elapsed().as_millis() as u64;
        if acquire_ms > 10 {
            tracing::warn!(acquire_ms, "Slow lock acquisition in check_timeouts");
        }
        tracing::Span::current().record("checked_count", pending.len());

        for (event_id, event) in pending.iter() {
            let age = now - event.received_at;
            // Issue 2 fix: Explicit handling of clock skew with logging
            match std::time::Duration::try_from(age) {
                Ok(age_std) if age_std > self.timeout => {
                    timed_out.push(*event_id);
                }
                Err(_) => {
                    // Negative duration indicates clock skew
                    tracing::warn!(
                        event_id = %event_id,
                        received_at = %event.received_at,
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
            .filter_map(|id| pending.remove(id))
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

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_confirmation_buffer_add_and_confirm() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(60));

        let event_id = EventId::new();
        let event = ProvisionalEvent {
            event_id,
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };

        assert!(buffer.add_provisional(event.clone()).await);
        assert_eq!(buffer.len().await, 1);

        let confirmed = buffer.confirm(event_id).await;
        assert!(confirmed.is_some());
        assert_eq!(buffer.len().await, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_confirmation_buffer_timeout() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(std::time::Duration::from_millis(100));

        let event_id = EventId::new();
        let mut event = ProvisionalEvent {
            event_id,
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };

        event.received_at = event.received_at - time::Duration::seconds(1);
        assert!(buffer.add_provisional(event).await);

        let timed_out = buffer.check_timeouts().await;
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], event_id);

        let removed = buffer.remove_timed_out(&timed_out).await;
        assert_eq!(removed.len(), 1);
        assert_eq!(buffer.len().await, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_confirmation_buffer_capacity_limit() -> TestResult<()> {
        let max_capacity = 5;
        let buffer =
            ConfirmationBuffer::with_capacity(std::time::Duration::from_secs(60), max_capacity);

        // Fill to capacity
        for i in 0..max_capacity {
            let event_id = EventId::new();
            let event = ProvisionalEvent {
                event_id,
                source: EventSource::new(format!("test-{i}")),
                event_type: EventType::new("test.event"),
                payload: serde_json::json!({"index": i}),
                ts_orig: sinex_primitives::temporal::now(),
                received_at: sinex_primitives::temporal::now(),
            };
            assert!(
                buffer.add_provisional(event).await,
                "Should accept event {i}"
            );
        }

        assert_eq!(buffer.len().await, max_capacity);

        // Next event should be rejected
        let event_id = EventId::new();
        let overflow_event = ProvisionalEvent {
            event_id,
            source: EventSource::new("overflow"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"overflow": true}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };
        assert!(
            !buffer.add_provisional(overflow_event).await,
            "Should reject overflow"
        );
        assert_eq!(buffer.rejected_count(), 1);
        assert_eq!(buffer.len().await, max_capacity); // Still at capacity

        Ok(())
    }
}
