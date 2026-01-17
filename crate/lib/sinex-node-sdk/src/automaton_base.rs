//! Automaton base infrastructure
//!
//! This module provides common infrastructure for automaton implementations,
//! reducing boilerplate across the 5+ automaton crates.
//!
//! # Common Patterns Extracted
//!
//! - `AutomatonStats`: Unified statistics tracking
//! - Accessor methods for runtime, db_pool, event_sender
//! - History recording and activity tracking
//! - Event channel management
//!
//! # Usage
//!
//! ```rust,ignore
//! use sinex_node_sdk::automaton_base::{AutomatonStats, AutomatonFields};
//!
//! pub struct MyAutomaton {
//!     fields: AutomatonFields<MyConfig>,
//!     // ... custom fields
//! }
//!
//! impl MyAutomaton {
//!     // Use fields.runtime(), fields.db_pool(), etc.
//! }
//! ```

use crate::confirmation_handler::ProvisionalEvent;
use crate::jetstream_consumer::JetStreamEventConsumer;
use crate::stream_processor::{EventSender, ProcessorRuntimeState, ScanReport};
use crate::{NodeError, NodeResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Default capacity for confirmed event channels
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

// ============================================================================
// Activity tracking types (compatible with sinex_processor_runtime::cli)
// ============================================================================

/// Entry representing recent activity for exploration display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEntry {
    /// Timestamp of activity
    pub timestamp: DateTime<Utc>,
    /// Activity description
    pub description: String,
    /// Optional associated data
    pub data: Option<serde_json::Value>,
}

/// Entry in ingestion history for tracking processing runs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionHistoryEntry {
    /// Scan/ingestion ID
    pub id: String,
    /// Start time
    pub started_at: DateTime<Utc>,
    /// End time (if completed)
    pub completed_at: Option<DateTime<Utc>>,
    /// Number of events generated
    pub events_generated: u64,
    /// Scan report summary
    pub scan_report: Option<ScanReport>,
    /// Error message if the run failed
    pub error: Option<String>,
}

/// Default maximum history entries to retain
pub const DEFAULT_MAX_HISTORY_ENTRIES: usize = 32;

/// Common statistics tracked by all automatons
#[derive(Debug, Default, Clone)]
pub struct AutomatonStats {
    /// Total number of input events processed
    pub inputs_seen: u64,
    /// Total number of output events emitted
    pub outputs_emitted: u64,
    /// Timestamp of last activity
    pub last_activity: Option<DateTime<Utc>>,
}

impl AutomatonStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record input events being processed
    pub fn record_input(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        self.inputs_seen = self.inputs_seen.saturating_add(count as u64);
        self.last_activity = Some(Utc::now());
    }

    /// Record output events being emitted
    pub fn record_output(&mut self, count: u64) {
        if count == 0 {
            return;
        }
        self.outputs_emitted = self.outputs_emitted.saturating_add(count);
        self.last_activity = Some(Utc::now());
    }
}

/// Common fields shared by all automatons
///
/// This struct contains the fields that are duplicated across all automaton
/// implementations. Use this as a field in your automaton struct to get
/// the common infrastructure for free.
pub struct AutomatonFields<C: Default> {
    /// Runtime state from initialization
    pub runtime: Option<ProcessorRuntimeState>,
    /// Automaton-specific configuration
    pub config: C,
    /// Event sender for emitting events
    pub event_sender: Option<EventSender>,
    /// Database connection pool
    pub db_pool: Option<PgPool>,
    /// Sender for incoming confirmed events
    pub incoming_tx: Option<mpsc::Sender<ProvisionalEvent>>,
    /// Receiver for incoming confirmed events
    pub incoming_rx: Option<mpsc::Receiver<ProvisionalEvent>>,
    /// JetStream consumer for event stream
    pub consumer: Option<Arc<JetStreamEventConsumer>>,
    /// Handle to consumer task
    pub consumer_handle: Option<JoinHandle<()>>,
    /// Recent activity history
    pub history: VecDeque<IngestionHistoryEntry>,
    /// Statistics
    pub stats: AutomatonStats,
    /// Maximum history entries to keep
    max_history_entries: usize,
    /// Channel capacity for event buffers
    channel_capacity: usize,
}

impl<C: Default> Default for AutomatonFields<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Default> AutomatonFields<C> {
    /// Create new automaton fields with default configuration
    pub fn new() -> Self {
        Self {
            runtime: None,
            config: C::default(),
            event_sender: None,
            db_pool: None,
            incoming_tx: None,
            incoming_rx: None,
            consumer: None,
            consumer_handle: None,
            history: VecDeque::new(),
            stats: AutomatonStats::new(),
            max_history_entries: DEFAULT_MAX_HISTORY_ENTRIES,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    /// Create with custom capacity settings
    pub fn with_capacity(max_history: usize, channel_capacity: usize) -> Self {
        Self {
            max_history_entries: max_history,
            channel_capacity,
            ..Self::new()
        }
    }

    /// Get runtime state, returning error if not initialized
    pub fn runtime(&self) -> NodeResult<&ProcessorRuntimeState> {
        self.runtime
            .as_ref()
            .ok_or_else(|| NodeError::Lifecycle("Automaton runtime not initialized".into()))
    }

    /// Get database pool, preferring runtime's pool
    pub fn db_pool(&self) -> NodeResult<&PgPool> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.db_pool())
        } else if let Some(pool) = self.db_pool.as_ref() {
            Ok(pool)
        } else {
            Err(NodeError::Processing(
                "Database pool not initialized".into(),
            ))
        }
    }

    /// Get event sender, preferring runtime's sender
    pub fn event_sender(&self) -> NodeResult<EventSender> {
        if let Some(runtime) = self.runtime.as_ref() {
            Ok(runtime.event_sender())
        } else if let Some(sender) = self.event_sender.as_ref() {
            Ok(sender.clone())
        } else {
            Err(NodeError::Processing("Event sender not initialized".into()))
        }
    }

    /// Ensure event channel exists, creating if needed
    pub fn ensure_event_channel(&mut self) {
        if self.incoming_tx.is_none() || self.incoming_rx.is_none() {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            self.incoming_tx = Some(tx);
            self.incoming_rx = Some(rx);
        }
    }

    /// Record a history entry, maintaining max size
    pub fn record_history(&mut self, entry: IngestionHistoryEntry) {
        self.history.push_front(entry);
        while self.history.len() > self.max_history_entries {
            self.history.pop_back();
        }
    }

    /// Get recent activity entries for exploration
    pub fn recent_activity(&self) -> Vec<ActivityEntry> {
        self.history
            .iter()
            .take(5)
            .map(|entry| ActivityEntry {
                timestamp: entry.completed_at.unwrap_or(entry.started_at),
                description: format!("Processed {} events", entry.events_generated),
                data: entry.scan_report.as_ref().map(|report| {
                    serde_json::json!({
                        "events_processed": report.events_processed,
                        "duration": entry.completed_at.map(|c| (c - entry.started_at).to_string())
                    })
                }),
            })
            .collect()
    }

    /// Take the receiver, leaving None in its place
    pub fn take_incoming_rx(&mut self) -> Option<mpsc::Receiver<ProvisionalEvent>> {
        self.incoming_rx.take()
    }

    /// Get reference to incoming sender
    pub fn incoming_tx(&self) -> Option<&mpsc::Sender<ProvisionalEvent>> {
        self.incoming_tx.as_ref()
    }

    /// Get mutable reference to consumer handle
    pub fn consumer_handle_mut(&mut self) -> &mut Option<JoinHandle<()>> {
        &mut self.consumer_handle
    }

    /// Get reference to consumer
    pub fn consumer(&self) -> Option<&Arc<JetStreamEventConsumer>> {
        self.consumer.as_ref()
    }

    /// Set consumer and handle
    pub fn set_consumer(&mut self, consumer: Arc<JetStreamEventConsumer>, handle: JoinHandle<()>) {
        self.consumer = Some(consumer);
        self.consumer_handle = Some(handle);
    }
}

// ============================================================================
// Common event handlers
// ============================================================================

/// Reusable confirmed event handler that forwards events to a channel.
///
/// This handler is used by all automatons to receive confirmed events from
/// the JetStream consumer and forward them to the automaton's processing loop.
#[derive(Clone)]
pub struct ChannelConfirmedEventHandler {
    sender: mpsc::Sender<ProvisionalEvent>,
}

impl ChannelConfirmedEventHandler {
    /// Create a new handler with the given channel sender
    pub fn new(sender: mpsc::Sender<ProvisionalEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl crate::confirmation_handler::ConfirmedEventHandler for ChannelConfirmedEventHandler {
    async fn handle_confirmed(&self, event: &ProvisionalEvent) -> NodeResult<()> {
        match self.sender.try_send(event.clone()) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("Confirmed event channel full; dropping event");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(NodeError::Processing(
                "Failed to forward confirmed event: channel closed".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestConfig;

    #[test]
    fn automaton_stats_tracks_inputs_and_outputs() {
        let mut stats = AutomatonStats::new();
        assert_eq!(stats.inputs_seen, 0);
        assert_eq!(stats.outputs_emitted, 0);
        assert!(stats.last_activity.is_none());

        stats.record_input(10);
        assert_eq!(stats.inputs_seen, 10);
        assert!(stats.last_activity.is_some());

        stats.record_output(5);
        assert_eq!(stats.outputs_emitted, 5);

        // Zero counts don't update counts but activity timestamp remains
        stats.record_input(0);
        stats.record_output(0);
        assert_eq!(stats.inputs_seen, 10);
        assert_eq!(stats.outputs_emitted, 5);
    }

    #[test]
    fn automaton_fields_initializes_with_defaults() {
        let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
        assert!(fields.runtime.is_none());
        assert!(fields.db_pool.is_none());
        assert!(fields.event_sender.is_none());
        assert!(fields.incoming_tx.is_none());
        assert!(fields.incoming_rx.is_none());
        assert!(fields.history.is_empty());
    }

    #[test]
    fn ensure_event_channel_creates_channel() {
        let mut fields: AutomatonFields<TestConfig> = AutomatonFields::new();
        assert!(fields.incoming_tx.is_none());
        assert!(fields.incoming_rx.is_none());

        fields.ensure_event_channel();
        assert!(fields.incoming_tx.is_some());
        assert!(fields.incoming_rx.is_some());
    }

    #[test]
    fn runtime_returns_error_when_not_initialized() {
        let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
        assert!(fields.runtime().is_err());
    }

    #[test]
    fn db_pool_returns_error_when_not_initialized() {
        let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
        assert!(fields.db_pool().is_err());
    }

    #[test]
    fn event_sender_returns_error_when_not_initialized() {
        let fields: AutomatonFields<TestConfig> = AutomatonFields::new();
        assert!(fields.event_sender().is_err());
    }
}
