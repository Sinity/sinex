//! Graceful shutdown handling for node processes.
//!
//! This module provides utilities for handling SIGTERM/SIGINT signals
//! and ensuring state is saved before process exit. This is critical
//! for hot reload support where state must survive process restarts.
//!
//! # Usage
//!
//! ```rust,ignore
//! use sinex_node_sdk::shutdown::{ShutdownHandler, ShutdownSignal};
//!
//! // Create handler with state save callback
//! let handler = ShutdownHandler::new("/tmp/my-processor.checkpoint");
//!
//! // Register the signal handler
//! let signal = handler.install()?;
//!
//! // In your processing loop, check for shutdown
//! loop {
//!     if signal.is_shutdown_requested() {
//!         // Save state and exit
//!         handler.save_state(&my_state)?;
//!         break;
//!     }
//!     // ... process events
//! }
//! ```

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, info};

#[cfg(feature = "messaging")]
use crate::checkpoint::CheckpointState;

/// Shutdown signal receiver for checking if shutdown was requested.
#[derive(Clone)]
pub struct ShutdownSignal {
    shutdown_requested: Arc<AtomicBool>,
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    /// Check if a shutdown signal has been received.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Wait for a shutdown signal asynchronously.
    pub async fn wait_for_shutdown(&mut self) {
        loop {
            if self.is_shutdown_requested() {
                return;
            }
            // Wait for change or check periodically
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                self.receiver.changed(),
            )
            .await;
        }
    }

    /// Get a clone of the watch receiver for use in select!
    pub fn watch_receiver(&self) -> watch::Receiver<bool> {
        self.receiver.clone()
    }
}

/// Handler for graceful shutdown with state persistence.
pub struct ShutdownHandler {
    /// Path to save checkpoint state on shutdown
    checkpoint_path: PathBuf,
    /// Shutdown flag
    shutdown_requested: Arc<AtomicBool>,
    /// Watch channel for async notification
    sender: watch::Sender<bool>,
    /// Receiver for creating signals
    receiver: watch::Receiver<bool>,
}

impl ShutdownHandler {
    /// Create a new shutdown handler.
    ///
    /// # Arguments
    /// - `checkpoint_path`: Path where state will be saved on shutdown
    pub fn new(checkpoint_path: impl Into<PathBuf>) -> Self {
        let (sender, receiver) = watch::channel(false);
        Self {
            checkpoint_path: checkpoint_path.into(),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            sender,
            receiver,
        }
    }

    /// Get the checkpoint file path.
    pub fn checkpoint_path(&self) -> &std::path::Path {
        &self.checkpoint_path
    }

    /// Get a shutdown signal receiver.
    pub fn signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            shutdown_requested: self.shutdown_requested.clone(),
            receiver: self.receiver.clone(),
        }
    }

    /// Install signal handlers for SIGTERM and SIGINT.
    ///
    /// Returns a ShutdownSignal that can be used to check for shutdown requests.
    #[cfg(unix)]
    pub fn install_signal_handlers(&self) -> std::io::Result<ShutdownSignal> {
        use tokio::signal::unix::{signal, SignalKind};

        let shutdown_flag = self.shutdown_requested.clone();
        let sender = self.sender.clone();

        // Spawn task to handle signals
        #[allow(clippy::expect_used)] // Fatal: signal handlers must be installable
        tokio::spawn(async move {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT, initiating graceful shutdown");
                }
            }

            shutdown_flag.store(true, Ordering::SeqCst);
            let _ = sender.send(true);
        });

        debug!("Installed signal handlers for graceful shutdown");
        Ok(self.signal())
    }

    /// Install signal handlers (non-Unix stub).
    #[cfg(not(unix))]
    pub fn install_signal_handlers(&self) -> std::io::Result<ShutdownSignal> {
        warn!("Signal handlers not available on this platform");
        Ok(self.signal())
    }

    /// Trigger shutdown manually (for testing or programmatic shutdown).
    pub fn trigger_shutdown(&self) {
        info!("Manual shutdown triggered");
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.sender.send(true);
    }

    /// Save checkpoint state to file.
    ///
    /// Called during graceful shutdown to persist state.
    #[cfg(feature = "messaging")]
    pub async fn save_state(&self, state: &CheckpointState) -> std::io::Result<()> {
        state.save_to_file(&self.checkpoint_path).await
    }

    /// Load checkpoint state from file if it exists.
    ///
    /// Called during startup when --restore-state is specified.
    #[cfg(feature = "messaging")]
    pub async fn load_state(&self) -> Option<CheckpointState> {
        CheckpointState::load_from_file(&self.checkpoint_path).await
    }

    /// Delete the checkpoint file after successful sync to primary store.
    #[cfg(feature = "messaging")]
    pub async fn clear_state(&self) -> std::io::Result<()> {
        CheckpointState::delete_file(&self.checkpoint_path).await
    }
}

/// Default checkpoint file path for a processor.
pub fn default_checkpoint_path(processor_name: &str) -> PathBuf {
    let runtime_dir = std::env::var("SINEX_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(runtime_dir).join(format!("{processor_name}.checkpoint.json"))
}

/// Configuration for shutdown behavior.
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Whether to save state to file on shutdown
    pub save_state_on_shutdown: bool,
    /// Whether to restore state from file on startup
    pub restore_state_on_startup: bool,
    /// Custom checkpoint file path (None = use default)
    pub checkpoint_path: Option<PathBuf>,
    /// Grace period before forced shutdown (seconds)
    pub grace_period_secs: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            save_state_on_shutdown: true,
            restore_state_on_startup: true,
            checkpoint_path: None,
            grace_period_secs: 30,
        }
    }
}

impl ShutdownConfig {
    /// Get the checkpoint path, using default if not specified.
    pub fn checkpoint_path(&self, processor_name: &str) -> PathBuf {
        self.checkpoint_path
            .clone()
            .unwrap_or_else(|| default_checkpoint_path(processor_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_shutdown_handler_creation() -> TestResult<()> {
        let handler = ShutdownHandler::new("/tmp/test.checkpoint");
        assert!(!handler.signal().is_shutdown_requested());
        Ok(())
    }

    #[sinex_test]
    async fn test_manual_shutdown() -> TestResult<()> {
        let handler = ShutdownHandler::new("/tmp/test.checkpoint");
        let signal = handler.signal();

        assert!(!signal.is_shutdown_requested());
        handler.trigger_shutdown();
        assert!(signal.is_shutdown_requested());
        Ok(())
    }

    #[sinex_test]
    async fn test_state_save_load() -> TestResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join("test.checkpoint.json");

        let handler = ShutdownHandler::new(&checkpoint_path);

        let state = CheckpointState::default();
        handler.save_state(&state).await.unwrap();

        let loaded = handler.load_state().await;
        assert!(loaded.is_some());

        handler.clear_state().await.unwrap();
        assert!(handler.load_state().await.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn test_default_checkpoint_path() -> TestResult<()> {
        let path = default_checkpoint_path("my-processor");
        assert!(path
            .to_string_lossy()
            .ends_with("my-processor.checkpoint.json"));
        Ok(())
    }
}
