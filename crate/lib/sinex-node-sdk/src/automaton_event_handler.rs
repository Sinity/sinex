//! Adapter for connecting Node automata to JetStream event consumption
//!
//! This module provides an adapter that allows automata to receive confirmed events
//! from JetStreamEventConsumer.
//!
//! NOTE: This is a work-in-progress adapter. The full integration requires refactoring
//! automata to support streaming consumption patterns.

use crate::confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent};
use crate::NodeResult;
use async_trait::async_trait;
use sinex_primitives::events::builder::EventId;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Simple adapter that tracks confirmed events for automata
///
/// This adapter implements ConfirmedEventHandler to receive confirmed events
/// from JetStreamEventConsumer. It maintains a list of processed event IDs
/// for verification and testing.
///
/// Future work: Integrate with Node scan() method to enable
/// streaming consumption.
pub struct AutomatonEventHandler {
    /// List of processed event IDs (for verification)
    processed_event_ids: Arc<RwLock<Vec<EventId>>>,
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
    pub async fn processed_event_ids(&self) -> Vec<EventId> {
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
    async fn handle_confirmed(&self, provisional: &ProvisionalEvent) -> NodeResult<()> {
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
    use sinex_primitives::domain::{EventSource, EventType};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_automaton_event_handler_basic() -> TestResult<()> {
        let handler = AutomatonEventHandler::new();

        let event_id = EventId::new();
        let provisional = ProvisionalEvent {
            event_id,
            source: EventSource::new("test"),
            event_type: EventType::new("test.event"),
            payload: serde_json::json!({"data": "test"}),
            ts_orig: sinex_primitives::temporal::now(),
            received_at: sinex_primitives::temporal::now(),
        };

        handler.handle_confirmed(&provisional).await.unwrap();

        // Counter should be 1
        assert_eq!(handler.processed_count().await, 1);

        // Event ID should be tracked
        let ids = handler.processed_event_ids().await;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], event_id);
        Ok(())
    }

    #[sinex_test]
    async fn test_automaton_event_handler_multiple_events() -> TestResult<()> {
        let handler = AutomatonEventHandler::new();

        let mut event_ids = Vec::new();
        for i in 0..10 {
            let event_id = EventId::new();
            event_ids.push(event_id);

            let provisional = ProvisionalEvent {
                event_id,
                source: EventSource::new(format!("test{}", i)),
                event_type: EventType::new("test.event"),
                payload: serde_json::json!({"index": i}),
                ts_orig: sinex_primitives::temporal::now(),
                received_at: sinex_primitives::temporal::now(),
            };

            handler.handle_confirmed(&provisional).await.unwrap();
        }

        // Counter should be 10
        assert_eq!(handler.processed_count().await, 10);

        // All event IDs should be tracked
        let tracked_ids = handler.processed_event_ids().await;
        assert_eq!(tracked_ids.len(), 10);
        assert_eq!(tracked_ids, event_ids);
        Ok(())
    }
}
