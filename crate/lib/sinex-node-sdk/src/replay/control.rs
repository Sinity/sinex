use crate::{NodeResult, SinexError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

/// Control handle for managing replay execution
#[derive(Clone)]
pub struct ReplayController {
    paused: Arc<AtomicBool>,
    pause_notify: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
    cancel_notify: Arc<Notify>,
}

impl ReplayController {
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
            cancel_notify: Arc::new(Notify::new()),
        }
    }

    pub fn pause(&self) {
        let was_paused = self.paused.swap(true, Ordering::SeqCst);
        if !was_paused {
            info!("Replay paused");
            self.pause_notify.notify_waiters();
        }
    }

    pub fn resume(&self) {
        let was_paused = self.paused.swap(false, Ordering::SeqCst);
        if was_paused {
            info!("Replay resumed");
            self.pause_notify.notify_waiters();
        }
    }

    pub fn cancel(&self) {
        let was_cancelled = self.cancelled.swap(true, Ordering::SeqCst);
        if !was_cancelled {
            warn!("Replay cancelled");
            self.cancel_notify.notify_waiters();
            self.pause_notify.notify_waiters();
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn wait_if_paused(&self) -> NodeResult<()> {
        while self.is_paused() && !self.is_cancelled() {
            debug!("Replay is paused, waiting for resume");
            tokio::select! {
                _ = self.pause_notify.notified() => {},
                _ = self.cancel_notify.notified() => break,
            }
        }

        if self.is_cancelled() {
            return Err(SinexError::from(sinex_primitives::SinexError::cancelled(
                "Replay was cancelled",
            )));
        }

        Ok(())
    }

    pub fn check_cancelled(&self) -> NodeResult<()> {
        if self.is_cancelled() {
            return Err(SinexError::from(sinex_primitives::SinexError::cancelled(
                "Replay was cancelled",
            )));
        }
        Ok(())
    }
}

impl Default for ReplayController {
    fn default() -> Self {
        Self::new()
    }
}
