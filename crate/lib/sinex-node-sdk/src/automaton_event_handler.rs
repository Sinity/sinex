//! Adapter for connecting Node automata to JetStream event consumption
//!
//! This module provides an adapter that allows automata to receive confirmed events
//! from JetStreamEventConsumer.
//!
//! NOTE: This is a work-in-progress adapter. The full integration requires refactoring
//! automata to support streaming consumption patterns.

use crate::NodeResult;
use crate::confirmation_handler::{ConfirmedEventHandler, ProvisionalEvent};
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

