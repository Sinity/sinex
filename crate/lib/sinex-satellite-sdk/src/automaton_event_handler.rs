//! Adapter for connecting StatefulStreamProcessor automata to JetStream event consumption
//!
//! This module provides an adapter that allows automata to receive confirmed events
//! from JetStreamEventConsumer.
//!
//! NOTE: This is a work-in-progress adapter. The full integration requires refactoring
//! automata to support streaming consumption patterns. See docs/way.md Phase 2.

use crate::confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent};
use crate::SatelliteResult;
use async_trait::async_trait;
use sinex_core::types::Ulid;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Simple adapter that tracks confirmed events for automata
///
/// This adapter implements ConfirmedEventHandler to receive confirmed events
/// from JetStreamEventConsumer. It maintains a list of processed event IDs
/// for verification and testing.
///
/// Future work: Integrate with StatefulStreamProcessor scan() method to enable
/// streaming consumption.
pub struct AutomatonEventHandler {
    /// List of processed event IDs (for verification)
    processed_event_ids: Arc<RwLock<Vec<Ulid>>>,
    /// Counter for processed events
    processed_count: Arc<RwLock<usize>>,
}

impl AutomatonEventHandler {
    /// Create a new automaton event handler
    pub fn new() -> Self {
        Self {
            processed_event_ids: Arc::new(RwLock::new(Vec::new())),
            processed_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Get the current processed count
    pub async fn processed_count(&self) -> usize {
        *self.processed_count.read().await
    }

    /// Get all processed event IDs
    pub async fn processed_event_ids(&self) -> Vec<Ulid> {
        self.processed_event_ids.read().await.clone()
    }
}

impl Default for AutomatonEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfirmedEventHandler for AutomatonEventHandler {
    async fn handle_confirmed(&self, provisional: &ProvisionalEvent) -> SatelliteResult<()> {
        debug!(
            event_id = %provisional.event_id,
            source = %provisional.source,
            event_type = %provisional.event_type,
            "Processing confirmed event"
        );

        // Track the event ID
        {
            let mut ids = self.processed_event_ids.write().await;
            ids.push(provisional.event_id);
        }

        // Increment counter
        let mut count = self.processed_count.write().await;
        *count += 1;

        if *count % 100 == 0 {
            info!("Processed {} confirmed events", *count);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_automaton_event_handler_basic() {
        let handler = AutomatonEventHandler::new();

        let event_id = Ulid::new();
        let provisional = ProvisionalEvent {
            event_id,
            source: "test".to_string(),
            event_type: "test.event".to_string(),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: chrono::Utc::now(),
            received_at: chrono::Utc::now(),
        };

        handler.handle_confirmed(&provisional).await.unwrap();

        // Counter should be 1
        assert_eq!(handler.processed_count().await, 1);

        // Event ID should be tracked
        let ids = handler.processed_event_ids().await;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], event_id);
    }

    #[tokio::test]
    async fn test_automaton_event_handler_multiple_events() {
        let handler = AutomatonEventHandler::new();

        let mut event_ids = Vec::new();
        for i in 0..10 {
            let event_id = Ulid::new();
            event_ids.push(event_id);

            let provisional = ProvisionalEvent {
                event_id,
                source: format!("test{}", i),
                event_type: "test.event".to_string(),
                payload: serde_json::json!({"index": i}),
                ts_orig: chrono::Utc::now(),
                received_at: chrono::Utc::now(),
            };

            handler.handle_confirmed(&provisional).await.unwrap();
        }

        // Counter should be 10
        assert_eq!(handler.processed_count().await, 10);

        // All event IDs should be tracked
        let tracked_ids = handler.processed_event_ids().await;
        assert_eq!(tracked_ids.len(), 10);
        assert_eq!(tracked_ids, event_ids);
    }
}
