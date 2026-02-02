//! Watcher lifecycle abstraction for managing long-running watcher tasks.
//!
//! This module provides a standardized pattern for managing watcher tasks across
//! different ingestor nodes. It handles state transitions, health tracking, and
//! cleanup of resources associated with background monitoring tasks.
//!
//! # Architecture
//!
//! The `WatcherHandle` encapsulates:
//! - **Lifecycle State**: Initialized → Running → Stopped
//! - **Task Management**: Spawned tokio tasks and forwarder handles
//! - **Health Tracking**: Error/success state for diagnostics
//! - **Material Context**: Optional source material context for provenance
//!
//! # Example
//!
//! ```rust
//! use sinex_node_sdk::watcher_handle::{WatcherHandle, WatcherHealth};
//!
//! // Initialize a watcher
//! let mut handle = WatcherHandle::<()>::initialized("clipboard");
//!
//! // Start monitoring with a spawned task
//! let task = tokio::spawn(async {
//!     // Watcher logic here
//! });
//! handle.start(task, None)?;
//!
//! // Check if active
//! if handle.is_active() {
//!     println!("Watcher is running");
//! }
//!
//! // Get health status
//! let health = handle.health();
//! println!("Active: {}, Last error: {:?}", health.active, health.last_error);
//!
//! // Shutdown (automatically aborts tasks on drop)
//! handle.shutdown().await;
//! ```

use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

use sinex_primitives::{Result as SinexResult, SinexError};

/// State machine for watcher lifecycle
#[derive(Debug)]
pub enum WatcherState {
    /// Watcher has been created but not started
    Initialized,
    /// Watcher is actively running with spawned tasks
    Running {
        /// Main watcher task handle
        task: JoinHandle<()>,
        /// Optional forwarder task handle (e.g., for event forwarding)
        forwarder: Option<JoinHandle<()>>,
    },
    /// Watcher has been stopped (terminal state)
    Stopped,
}

/// Health tracking for a watcher
#[derive(Debug, Clone, Default)]
pub struct WatcherHealth {
    /// Whether the watcher is currently active
    pub active: bool,
    /// Last error encountered by the watcher
    pub last_error: Option<String>,
    /// Timestamp of last successful operation
    pub last_success: Option<sinex_primitives::Timestamp>,
    /// Total number of events processed
    pub events_processed: u64,
}

/// Handle to a watcher task with lifecycle and health tracking.
///
/// Generic over `M` to support optional material context type.
/// Use `WatcherHandle<()>` if no material context is needed.
#[derive(Debug)]
pub struct WatcherHandle<M = ()> {
    /// Watcher identifier for logging and diagnostics
    name: &'static str,
    /// Current state of the watcher
    state: WatcherState,
    /// Optional material context for provenance tracking
    material: Option<M>,
    /// Health tracking state (shared for potential external access)
    health: Arc<RwLock<WatcherHealth>>,
}

impl<M> WatcherHandle<M> {
    /// Create a new watcher in the initialized state.
    ///
    /// # Arguments
    /// * `name` - Static identifier for the watcher (used in logs)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use sinex_node_sdk::watcher_handle::WatcherHandle;
    /// let handle = WatcherHandle::<()>::initialized("dbus");
    /// assert!(!handle.is_active());
    /// ```
    pub fn initialized(name: &'static str) -> Self {
        Self {
            name,
            state: WatcherState::Initialized,
            material: None,
            health: Arc::new(RwLock::new(WatcherHealth::default())),
        }
    }

    /// Create a new watcher directly in the running state.
    ///
    /// Convenience constructor for cases where you want to start immediately
    /// without going through the initialized state.
    ///
    /// # Arguments
    /// * `name` - Static identifier for the watcher
    /// * `task` - Main watcher task handle
    /// * `forwarder` - Optional forwarder task handle
    /// * `material` - Optional material context
    pub fn running(
        name: &'static str,
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
        material: Option<M>,
    ) -> Self {
        let health = Arc::new(RwLock::new(WatcherHealth {
            active: true,
            ..Default::default()
        }));
        Self {
            name,
            state: WatcherState::Running { task, forwarder },
            material,
            health,
        }
    }

    /// Attach material context to this watcher.
    ///
    /// Should be called before `start()` if material context is needed.
    pub fn with_material(mut self, material: M) -> Self {
        self.material = Some(material);
        self
    }

    /// Transition watcher from Initialized to Running state.
    ///
    /// # Arguments
    /// * `task` - Main watcher task handle
    /// * `forwarder` - Optional forwarder task handle
    ///
    /// # Errors
    /// Returns `SinexError::InvalidState` if called when watcher is already in Running or Stopped state.
    pub fn start(
        &mut self,
        task: JoinHandle<()>,
        forwarder: Option<JoinHandle<()>>,
    ) -> SinexResult<()> {
        match &self.state {
            WatcherState::Initialized => {
                self.state = WatcherState::Running { task, forwarder };
                if let Ok(mut health) = self.health.write() {
                    health.active = true;
                }
                Ok(())
            }
            WatcherState::Running { .. } => Err(SinexError::invalid_state(
                "WatcherHandle::start called on already-running watcher",
            )
            .with_context("watcher_name", self.name)),
            WatcherState::Stopped => Err(SinexError::invalid_state(
                "WatcherHandle::start called on stopped watcher",
            )
            .with_context("watcher_name", self.name)),
        }
    }

    /// Check if the watcher is currently active (running and task not finished).
    pub fn is_active(&self) -> bool {
        match &self.state {
            WatcherState::Running { task, .. } => !task.is_finished(),
            WatcherState::Initialized | WatcherState::Stopped => false,
        }
    }

    /// Get the watcher's name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get current health status snapshot.
    pub fn health(&self) -> WatcherHealth {
        self.health.read().map(|h| h.clone()).unwrap_or_default()
    }

    /// Get a handle to the health tracker for external updates.
    ///
    /// This allows watcher tasks to update their own health status.
    pub fn health_tracker(&self) -> Arc<RwLock<WatcherHealth>> {
        Arc::clone(&self.health)
    }

    /// Record a successful operation in the health tracker.
    pub fn record_success(&self) {
        if let Ok(mut health) = self.health.write() {
            health.last_success = Some(sinex_primitives::temporal::Timestamp::now());
            health.events_processed = health.events_processed.saturating_add(1);
        }
    }

    /// Record an error in the health tracker.
    pub fn record_error(&self, error: String) {
        if let Ok(mut health) = self.health.write() {
            health.last_error = Some(error);
        }
    }

    /// Take ownership of the material context, leaving None in its place.
    pub fn take_material(&mut self) -> Option<M> {
        self.material.take()
    }

    /// Gracefully shutdown the watcher, aborting tasks and marking as stopped.
    ///
    /// This is also automatically called on `Drop`, but calling explicitly
    /// allows for proper async cleanup of material contexts and awaiting task completion.
    pub async fn shutdown(mut self) {
        self.abort_tasks_async().await;
        self.state = WatcherState::Stopped;
        if let Ok(mut health) = self.health.write() {
            health.active = false;
        }
        // Material context cleanup should be handled by caller if needed
        // since we can't await in Drop
    }

    /// Abort any running tasks and wait for them to finish.
    async fn abort_tasks_async(&mut self) {
        match &mut self.state {
            WatcherState::Running { task, forwarder } => {
                task.abort();
                let _ = task.await;
                if let Some(fwd) = forwarder.take() {
                    fwd.abort();
                    let _ = fwd.await;
                }
            }
            WatcherState::Initialized | WatcherState::Stopped => {}
        }
    }

    /// Synchronous abort for Drop (best-effort).
    fn abort_tasks_sync(&mut self) {
        match &self.state {
            WatcherState::Running { task, forwarder } => {
                task.abort();
                if let Some(fwd) = forwarder {
                    fwd.abort();
                }
            }
            WatcherState::Initialized | WatcherState::Stopped => {}
        }
    }
}

impl<M> Drop for WatcherHandle<M> {
    fn drop(&mut self) {
        self.abort_tasks_sync();
        if let Ok(mut health) = self.health.write() {
            health.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::time::{sleep, Duration};
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn test_watcher_state_transitions() -> Result<(), Box<dyn std::error::Error>> {
        let mut handle = WatcherHandle::<()>::initialized("test");
        assert!(!handle.is_active());

        let task = tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
        });
        handle.start(task, None)?;
        assert!(handle.is_active());

        // Shutdown consumes self, so check state before shutdown
        let was_active = handle.is_active();
        assert!(was_active);
        handle.shutdown().await;
        // handle is consumed, can't check after
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_running_constructor() -> Result<(), Box<dyn std::error::Error>> {
        let task = tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
        });
        let handle = WatcherHandle::<()>::running("test", task, None, None);
        assert!(handle.is_active());
        let health = handle.health();
        assert!(health.active);
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_health_tracking() -> Result<(), Box<dyn std::error::Error>> {
        let handle = WatcherHandle::<()>::initialized("test");

        let health = handle.health();
        assert!(!health.active);
        assert_eq!(health.events_processed, 0);

        handle.record_success();
        let health = handle.health();
        assert_eq!(health.events_processed, 1);
        assert!(health.last_success.is_some());

        handle.record_error("test error".to_string());
        let health = handle.health();
        assert_eq!(health.last_error, Some("test error".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_shutdown_aborts_task() -> Result<(), Box<dyn std::error::Error>> {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        let task = tokio::spawn(async move {
            sleep(Duration::from_secs(10)).await;
            flag_clone.store(true, Ordering::SeqCst);
        });

        let mut handle = WatcherHandle::<()>::initialized("test");
        handle.start(task, None)?;

        handle.shutdown().await;
        sleep(Duration::from_millis(100)).await;

        // Task should have been aborted, flag should still be false
        assert!(!flag.load(Ordering::SeqCst));
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_with_material() -> Result<(), Box<dyn std::error::Error>> {
        let material = "test_context";
        let mut handle = WatcherHandle::initialized("test").with_material(material);

        let task = tokio::spawn(async {});
        handle.start(task, None)?;

        let extracted = handle.take_material();
        assert_eq!(extracted, Some("test_context"));
        assert!(handle.take_material().is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_watcher_with_forwarder() -> Result<(), Box<dyn std::error::Error>> {
        let main_task = tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
        });
        let forwarder_task = tokio::spawn(async {
            sleep(Duration::from_secs(10)).await;
        });

        let mut handle = WatcherHandle::<()>::initialized("test");
        handle.start(main_task, Some(forwarder_task))?;
        assert!(handle.is_active());

        handle.shutdown().await;
        // Both tasks should be aborted
        Ok(())
    }
}
