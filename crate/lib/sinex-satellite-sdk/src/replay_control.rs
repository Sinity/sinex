//! Replay control mechanisms for pause/resume functionality

use crate::SatelliteResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Control handle for managing replay execution
#[derive(Clone)]
pub struct ReplayController {
    /// Flag indicating if replay is paused
    paused: Arc<AtomicBool>,

    /// Notification for pause/resume events
    pause_notify: Arc<Notify>,

    /// Flag for cancellation
    cancelled: Arc<AtomicBool>,

    /// Notification for cancellation
    cancel_notify: Arc<Notify>,
}

impl ReplayController {
    /// Create a new replay controller
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
            cancel_notify: Arc::new(Notify::new()),
        }
    }

    /// Pause the replay
    pub fn pause(&self) {
        let was_paused = self.paused.swap(true, Ordering::SeqCst);
        if !was_paused {
            info!("Replay paused");
            self.pause_notify.notify_waiters();
        }
    }

    /// Resume the replay
    pub fn resume(&self) {
        let was_paused = self.paused.swap(false, Ordering::SeqCst);
        if was_paused {
            info!("Replay resumed");
            self.pause_notify.notify_waiters();
        }
    }

    /// Cancel the replay
    pub fn cancel(&self) {
        let was_cancelled = self.cancelled.swap(true, Ordering::SeqCst);
        if !was_cancelled {
            warn!("Replay cancelled");
            self.cancel_notify.notify_waiters();
            // Also notify pause waiters to unblock them
            self.pause_notify.notify_waiters();
        }
    }

    /// Check if replay is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Check if replay is cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Wait while paused (returns immediately if not paused)
    /// Returns Ok(()) if resumed, Err if cancelled
    pub async fn wait_if_paused(&self) -> SatelliteResult<()> {
        while self.is_paused() && !self.is_cancelled() {
            debug!("Replay is paused, waiting for resume signal");

            // Wait for either pause state change or cancellation
            tokio::select! {
                _ = self.pause_notify.notified() => {
                    // Pause state changed, loop will check if still paused
                }
                _ = self.cancel_notify.notified() => {
                    // Cancelled, break out
                    break;
                }
            }
        }

        if self.is_cancelled() {
            return Err(crate::SatelliteError::OperationCancelled(
                "Replay was cancelled".to_string(),
            ));
        }

        Ok(())
    }

    /// Check for cancellation and return error if cancelled
    pub fn check_cancelled(&self) -> SatelliteResult<()> {
        if self.is_cancelled() {
            return Err(crate::SatelliteError::OperationCancelled(
                "Replay was cancelled".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for ReplayController {
    fn default() -> Self {
        Self::new()
    }
}

/// State management for replay operations
#[derive(Debug, Clone, bon::Builder)]
pub struct ReplayState {
    /// Current batch number being processed
    pub current_batch: usize,

    /// Total batches processed
    pub total_batches: usize,

    /// Current offset in the dataset
    pub current_offset: usize,

    /// Last processed event ID (if available)
    pub last_event_id: Option<String>,

    /// Whether replay is currently paused
    pub is_paused: bool,

    /// Whether replay has been cancelled
    pub is_cancelled: bool,
}

impl ReplayState {
    /// Create new replay state
    pub fn new() -> Self {
        Self {
            current_batch: 0,
            total_batches: 0,
            current_offset: 0,
            last_event_id: None,
            is_paused: false,
            is_cancelled: false,
        }
    }

    /// Update state for batch completion
    pub fn complete_batch(&mut self, batch_size: usize, last_event_id: Option<String>) {
        self.current_batch += 1;
        self.total_batches += 1;
        self.current_offset += batch_size;
        if last_event_id.is_some() {
            self.last_event_id = last_event_id;
        }
    }

    /// Mark as paused
    pub fn pause(&mut self) {
        self.is_paused = true;
    }

    /// Mark as resumed
    pub fn resume(&mut self) {
        self.is_paused = false;
    }

    /// Mark as cancelled
    pub fn cancel(&mut self) {
        self.is_cancelled = true;
    }
}

impl Default for ReplayState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pause_resume() {
        let controller = ReplayController::new();

        // Initially not paused
        assert!(!controller.is_paused());

        // Pause
        controller.pause();
        assert!(controller.is_paused());

        // Resume
        controller.resume();
        assert!(!controller.is_paused());
    }

    #[tokio::test]
    async fn test_cancel() {
        let controller = ReplayController::new();

        // Initially not cancelled
        assert!(!controller.is_cancelled());

        // Cancel
        controller.cancel();
        assert!(controller.is_cancelled());

        // Check should return error
        assert!(controller.check_cancelled().is_err());
    }

    #[tokio::test]
    async fn test_wait_if_paused() {
        let controller = ReplayController::new();
        let controller_clone = controller.clone();

        // Pause the replay
        controller.pause();

        // Spawn task to resume after delay
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            controller_clone.resume();
        });

        // Wait should complete when resumed
        let result = controller.wait_if_paused().await;
        assert!(result.is_ok());
        assert!(!controller.is_paused());
    }

    #[tokio::test]
    async fn test_wait_if_paused_with_cancel() {
        let controller = ReplayController::new();
        let controller_clone = controller.clone();

        // Pause the replay
        controller.pause();

        // Spawn task to cancel after delay
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            controller_clone.cancel();
        });

        // Wait should return error when cancelled
        let result = controller.wait_if_paused().await;
        assert!(result.is_err());
        assert!(controller.is_cancelled());
    }
}
