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
//! # async fn demo() -> sinex_primitives::Result<()> {
//! handle.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, RwLock};
use tokio::task::{JoinError, JoinHandle};

use sinex_primitives::{Result as SinexResult, SinexError};
use tracing::{debug, warn};

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
    fn shutdown_join_result(
        watcher_name: &'static str,
        task_name: &str,
        result: Result<(), JoinError>,
    ) -> SinexResult<()> {
        match result {
            Ok(()) => {
                debug!(watcher = watcher_name, task = task_name, "Watcher task finished before shutdown");
                Ok(())
            }
            Err(join_error) if join_error.is_cancelled() => {
                debug!(watcher = watcher_name, task = task_name, "Watcher task aborted during shutdown");
                Ok(())
            }
            Err(join_error) => {
                Err(SinexError::processing("Watcher task failed during shutdown")
                    .with_context("watcher_name", watcher_name)
                    .with_context("task", task_name.to_string())
                    .with_context("join_error", join_error.to_string()))
            }
        }
    }

    fn collapse_shutdown_errors(mut errors: Vec<SinexError>) -> SinexResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let mut error = errors.remove(0);
        for (index, extra) in errors.into_iter().enumerate() {
            error = error.with_context(
                format!("additional_shutdown_error_{}", index + 1),
                extra.to_string(),
            );
        }
        Err(error)
    }

    fn recover_health_read(&self) -> WatcherHealth {
        match self.health.read() {
            Ok(health) => health.clone(),
            Err(poisoned) => {
                warn!(
                    watcher = self.name,
                    "Watcher health lock was poisoned during read; recovering last known state"
                );
                poisoned.into_inner().clone()
            }
        }
    }

    fn with_health_write<R>(&self, update: impl FnOnce(&mut WatcherHealth) -> R) -> R {
        match self.health.write() {
            Ok(mut health) => update(&mut health),
            Err(poisoned) => {
                warn!(
                    watcher = self.name,
                    "Watcher health lock was poisoned during write; recovering mutable state"
                );
                let mut health = poisoned.into_inner();
                update(&mut health)
            }
        }
    }

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
    #[must_use]
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
    #[must_use]
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
                self.with_health_write(|health| {
                    health.active = true;
                });
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
        self.recover_health_read()
    }

    /// Get a handle to the health tracker for external updates.
    ///
    /// This allows watcher tasks to update their own health status.
    pub fn health_tracker(&self) -> Arc<RwLock<WatcherHealth>> {
        Arc::clone(&self.health)
    }

    /// Record a successful operation in the health tracker.
    pub fn record_success(&self) {
        self.with_health_write(|health| {
            health.last_success = Some(sinex_primitives::temporal::Timestamp::now());
            health.events_processed = health.events_processed.saturating_add(1);
        });
    }

    /// Record an error in the health tracker.
    pub fn record_error(&self, error: String) {
        self.with_health_write(|health| {
            health.last_error = Some(error);
        });
    }

    /// Get a reference to the material context, if present.
    pub fn material(&self) -> Option<&M> {
        self.material.as_ref()
    }

    /// Take ownership of the material context, leaving None in its place.
    pub fn take_material(&mut self) -> Option<M> {
        self.material.take()
    }

    /// Gracefully shutdown the watcher, aborting tasks and marking as stopped.
    ///
    /// This is also automatically called on `Drop`, but calling explicitly
    /// allows for proper async cleanup of material contexts and awaiting task completion.
    pub async fn shutdown(mut self) -> SinexResult<()> {
        let shutdown_result = self.abort_tasks_async().await;
        self.state = WatcherState::Stopped;
        self.with_health_write(|health| {
            health.active = false;
        });
        // Material context cleanup should be handled by caller if needed
        // since we can't await in Drop
        shutdown_result
    }

    /// Abort any running tasks and wait for them to finish.
    async fn abort_tasks_async(&mut self) -> SinexResult<()> {
        let watcher_name = self.name;
        let mut errors = Vec::new();
        match &mut self.state {
            WatcherState::Running { task, forwarder } => {
                task.abort();
                if let Err(error) =
                    Self::shutdown_join_result(watcher_name, "watcher task", task.await)
                {
                    errors.push(error);
                }
                if let Some(fwd) = forwarder.take() {
                    fwd.abort();
                    if let Err(error) =
                        Self::shutdown_join_result(watcher_name, "watcher forwarder", fwd.await)
                    {
                        errors.push(error);
                    }
                }
            }
            WatcherState::Initialized | WatcherState::Stopped => {}
        }
        Self::collapse_shutdown_errors(errors)
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
        self.with_health_write(|health| {
            health.active = false;
        });
    }
}
