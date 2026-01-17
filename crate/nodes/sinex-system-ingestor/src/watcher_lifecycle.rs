//! Lifecycle protocol for system node watchers
//!
//! Provides health monitoring, graceful shutdown, and liveness tracking
//! for all watchers in the system node.

use async_trait::async_trait;
use sinex_node_sdk::NodeResult;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Health snapshot for a watcher
#[derive(Debug, Clone)]
pub struct WatcherHealth {
    /// Whether the watcher is currently active
    pub active: bool,
    /// Timestamp of the last event processed
    pub last_event: Option<Instant>,
    /// Last error encountered (if any)
    pub last_error: Option<String>,
    /// Total number of events processed
    pub events_processed: u64,
}

impl WatcherHealth {
    /// Create a new health snapshot
    pub fn new() -> Self {
        Self {
            active: false,
            last_event: None,
            last_error: None,
            events_processed: 0,
        }
    }

    /// Check if watcher is healthy (active and recent events)
    pub fn is_healthy(&self, max_idle_secs: u64) -> bool {
        if !self.active {
            return false;
        }

        if let Some(last_event) = self.last_event {
            let elapsed = last_event.elapsed().as_secs();
            elapsed < max_idle_secs
        } else {
            // No events yet, but active - consider healthy
            true
        }
    }
}

impl Default for WatcherHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Lifecycle protocol for all system node watchers
#[async_trait]
pub trait WatcherLifecycle: Send + Sync {
    /// Get current health snapshot
    fn health_snapshot(&self) -> WatcherHealth;

    /// Graceful shutdown - wait for in-flight work to complete
    ///
    /// If `graceful` is true, waits up to 30s for watcher to drain.
    /// If `graceful` is false or timeout expires, forcefully terminates.
    async fn shutdown(&mut self, graceful: bool) -> NodeResult<()>;

    /// Last event timestamp for liveness checks
    fn last_event_timestamp(&self) -> Option<Instant>;

    /// Get the cancellation token for this watcher
    fn cancellation_token(&self) -> &CancellationToken;
}
