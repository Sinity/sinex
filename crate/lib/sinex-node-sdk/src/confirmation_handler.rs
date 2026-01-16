//! Confirmation-aware event consumption primitives
//!
//! This module provides the infrastructure for consuming provisional events
//! and processing them after confirmation, with optional immediate provisional processing.

use crate::NodeResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_core::types::Ulid;
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
    pub event_id: Ulid,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub ts_orig: chrono::DateTime<chrono::Utc>,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

/// Event confirmation from ingestd
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfirmation {
    pub event_id: Ulid,
    pub persisted: bool,
    pub ts_ingest: chrono::DateTime<chrono::Utc>,
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
    async fn rollback_provisional(&self, event_id: Ulid) -> NodeResult<()>;
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

/// Buffer for provisional events awaiting confirmation
pub struct ConfirmationBuffer {
    /// Provisional events indexed by event_id
    pending: Arc<RwLock<HashMap<Ulid, ProvisionalEvent>>>,
    /// Maximum time to wait for confirmation before treating as failure
    timeout: std::time::Duration,
}

impl ConfirmationBuffer {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            timeout,
        }
    }

    /// Add a provisional event to the buffer
    pub async fn add_provisional(&self, event: ProvisionalEvent) {
        let mut pending = self.pending.write().await;
        pending.insert(event.event_id, event);
    }

    /// Retrieve and remove an event upon confirmation
    pub async fn confirm(&self, event_id: Ulid) -> Option<ProvisionalEvent> {
        let mut pending = self.pending.write().await;
        pending.remove(&event_id)
    }

    /// Check if an event has timed out
    pub async fn check_timeouts(&self) -> Vec<Ulid> {
        let mut timed_out = Vec::new();
        let now = chrono::Utc::now();
        let pending = self.pending.read().await;

        for (event_id, event) in pending.iter() {
            let age = now.signed_duration_since(event.received_at);
            if age.to_std().unwrap_or_default() > self.timeout {
                timed_out.push(*event_id);
            }
        }

        timed_out
    }

    /// Remove timed-out events
    pub async fn remove_timed_out(&self, event_ids: &[Ulid]) -> Vec<ProvisionalEvent> {
        let mut pending = self.pending.write().await;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::{sinex_test, TestResult};

    #[sinex_test]
    async fn test_confirmation_buffer_add_and_confirm() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(std::time::Duration::from_secs(60));

        let event_id = Ulid::new();
        let event = ProvisionalEvent {
            event_id,
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: chrono::Utc::now(),
            received_at: chrono::Utc::now(),
        };

        buffer.add_provisional(event.clone()).await;
        assert_eq!(buffer.len().await, 1);

        let confirmed = buffer.confirm(event_id).await;
        assert!(confirmed.is_some());
        assert_eq!(buffer.len().await, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_confirmation_buffer_timeout() -> TestResult<()> {
        let buffer = ConfirmationBuffer::new(std::time::Duration::from_millis(100));

        let event_id = Ulid::new();
        let mut event = ProvisionalEvent {
            event_id,
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: chrono::Utc::now(),
            received_at: chrono::Utc::now(),
        };

        event.received_at -= chrono::Duration::seconds(1);
        buffer.add_provisional(event).await;

        let timed_out = buffer.check_timeouts().await;
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], event_id);

        let removed = buffer.remove_timed_out(&timed_out).await;
        assert_eq!(removed.len(), 1);
        assert_eq!(buffer.len().await, 0);
        Ok(())
    }
}
