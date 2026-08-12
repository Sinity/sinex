//! Shared control primitives for bounded maintenance and rebuild work.
//!
//! This is deliberately an in-process contract first. Durable operation rows,
//! checkpoints, and host-wide admission can be layered on top without making
//! each caller reinvent cancellation, accounting, or destructive-boundary
//! checks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::runtime::{RuntimeResult, SinexError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkIdentity {
    pub operation_id: String,
    pub kind: String,
    pub scope_fingerprint: String,
    pub generation: u64,
}

impl WorkIdentity {
    #[must_use]
    pub fn ephemeral(kind: impl Into<String>, scope_fingerprint: impl Into<String>) -> Self {
        Self {
            operation_id: uuid::Uuid::now_v7().to_string(),
            kind: kind.into(),
            scope_fingerprint: scope_fingerprint.into(),
            generation: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkBudget {
    pub max_items: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_runtime: Option<Duration>,
    pub items_per_sec: Option<f64>,
    pub bytes_per_sec: Option<f64>,
}

impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            max_items: None,
            max_bytes: None,
            // A generic controller cannot safely guess an operation's deadline
            // or throughput. Callers must opt into limits appropriate to their
            // work and host-admission policy.
            max_runtime: None,
            items_per_sec: None,
            bytes_per_sec: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkProgress {
    pub phase: String,
    pub items_done: u64,
    pub bytes_done: u64,
    pub checkpoint: Option<String>,
    pub blocked_on: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkStopReason {
    Cancelled,
    RuntimeBudget,
    ItemBudget,
    ByteBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkOutcome {
    Completed,
    Partial(WorkStopReason),
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct WorkCancellation {
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    wake: Arc<Notify>,
}

impl Default for WorkCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Arc::new(Notify::new()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        self.wake.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Host- or subsystem-scoped admission. Tokio's semaphore is FIFO, so queued
/// maintenance operations do not jump the line merely because they poll more
/// aggressively.
#[derive(Debug, Clone)]
pub struct WorkAdmission {
    permits: Arc<Semaphore>,
}

impl WorkAdmission {
    #[must_use]
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrency.max(1))),
        }
    }

    pub async fn acquire(
        &self,
        cancellation: &WorkCancellation,
    ) -> RuntimeResult<OwnedSemaphorePermit> {
        // Register before checking the sticky flag so a cancellation between
        // the check and the select cannot be lost by notify_waiters().
        let cancellation_wake = cancellation.wake.notified();
        if cancellation.is_cancelled() {
            return Err(SinexError::validation("work admission cancelled"));
        }
        tokio::select! {
            permit = self.permits.clone().acquire_owned() => {
                permit.map_err(|error| SinexError::validation(format!("work admission closed: {error}")))
            }
            () = cancellation_wake => {
                Err(SinexError::validation("work admission cancelled"))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkController {
    identity: WorkIdentity,
    budget: WorkBudget,
    cancellation: WorkCancellation,
    started: Instant,
    progress: WorkProgress,
}

impl WorkController {
    #[must_use]
    pub fn new(
        identity: WorkIdentity,
        budget: WorkBudget,
        cancellation: WorkCancellation,
    ) -> Self {
        Self {
            identity,
            budget,
            cancellation,
            started: Instant::now(),
            progress: WorkProgress {
                phase: "starting".to_owned(),
                items_done: 0,
                bytes_done: 0,
                checkpoint: None,
                blocked_on: None,
            },
        }
    }

    #[must_use]
    pub fn identity(&self) -> &WorkIdentity {
        &self.identity
    }

    #[must_use]
    pub fn progress(&self) -> &WorkProgress {
        &self.progress
    }

    #[must_use]
    pub fn cancellation(&self) -> WorkCancellation {
        self.cancellation.clone()
    }

    pub fn check(&self, items: u64, bytes: u64) -> RuntimeResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(SinexError::validation("work cancelled"));
        }
        if self
            .budget
            .max_runtime
            .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            return Err(SinexError::validation("work runtime budget exhausted"));
        }
        if self
            .budget
            .max_items
            .is_some_and(|limit| self.progress.items_done.saturating_add(items) > limit)
        {
            return Err(SinexError::validation("work item budget exhausted"));
        }
        if self
            .budget
            .max_bytes
            .is_some_and(|limit| self.progress.bytes_done.saturating_add(bytes) > limit)
        {
            return Err(SinexError::validation("work byte budget exhausted"));
        }
        Ok(())
    }

    /// Record a completed batch and cooperatively wait for the configured
    /// sustained rate. Every wait is interruptible by cancellation.
    pub async fn record_batch(
        &mut self,
        phase: impl Into<String>,
        items: u64,
        bytes: u64,
        checkpoint: Option<String>,
    ) -> RuntimeResult<()> {
        self.check(items, bytes)?;
        self.progress.phase = phase.into();
        self.progress.items_done = self.progress.items_done.saturating_add(items);
        self.progress.bytes_done = self.progress.bytes_done.saturating_add(bytes);
        self.progress.checkpoint = checkpoint;

        let mut required = Duration::ZERO;
        let elapsed = self.started.elapsed().as_secs_f64();
        if let Some(rate) = self.budget.items_per_sec.filter(|rate| *rate > 0.0) {
            required = required.max(Duration::from_secs_f64(
                self.progress.items_done as f64 / rate,
            ));
        }
        if let Some(rate) = self.budget.bytes_per_sec.filter(|rate| *rate > 0.0) {
            required = required.max(Duration::from_secs_f64(
                self.progress.bytes_done as f64 / rate,
            ));
        }
        let wait = required.saturating_sub(Duration::from_secs_f64(elapsed));
        if wait.is_zero() {
            return Ok(());
        }
        self.progress.blocked_on = Some("rate_budget".to_owned());
        tokio::select! {
            () = tokio::time::sleep(wait) => {
                self.progress.blocked_on = None;
                Ok(())
            }
            () = self.cancellation.wake.notified() => {
                Err(SinexError::validation("work cancelled while rate limited"))
            }
        }
    }

    pub fn destructive_boundary_check(&self) -> RuntimeResult<()> {
        self.check(0, 0)
    }
}
