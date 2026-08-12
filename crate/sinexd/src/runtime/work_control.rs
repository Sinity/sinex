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

impl WorkProgress {
    /// Construct a resumable progress cursor. Callers may persist this value
    /// in their operation record and pass it back to `WorkController::resume`.
    #[must_use]
    pub fn at(
        phase: impl Into<String>,
        items_done: u64,
        bytes_done: u64,
        checkpoint: Option<String>,
    ) -> Self {
        Self {
            phase: phase.into(),
            items_done,
            bytes_done,
            checkpoint,
            blocked_on: None,
        }
    }
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
    Failed,
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
    terminal: Option<WorkOutcome>,
    stop_reason: Option<WorkStopReason>,
}

impl WorkController {
    #[must_use]
    pub fn new(identity: WorkIdentity, budget: WorkBudget, cancellation: WorkCancellation) -> Self {
        Self::resume(
            identity,
            budget,
            cancellation,
            WorkProgress::at("starting", 0, 0, None),
        )
    }

    /// Resume work from a caller-owned durable cursor. The controller keeps
    /// only the latest cursor, never a history of batches, so a large scan
    /// cannot grow memory merely because it reports progress.
    #[must_use]
    pub fn resume(
        identity: WorkIdentity,
        budget: WorkBudget,
        cancellation: WorkCancellation,
        progress: WorkProgress,
    ) -> Self {
        Self {
            identity,
            budget,
            cancellation,
            started: Instant::now(),
            progress,
            terminal: None,
            stop_reason: None,
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
    pub fn outcome(&self) -> WorkOutcome {
        if let Some(outcome) = self.terminal {
            return outcome;
        }
        if self.cancellation.is_cancelled() {
            return WorkOutcome::Cancelled;
        }
        self.stop_reason()
            .map_or(WorkOutcome::Completed, WorkOutcome::Partial)
    }

    /// Mark a caller-observed, non-budget failure. Keeping this explicit
    /// prevents an operation from reporting `Completed` after it has caught a
    /// database, filesystem, or parser error.
    pub fn mark_failed(&mut self) {
        self.terminal = Some(WorkOutcome::Failed);
    }

    fn stop_reason(&self) -> Option<WorkStopReason> {
        self.stop_reason
    }

    #[must_use]
    pub fn cancellation(&self) -> WorkCancellation {
        self.cancellation.clone()
    }

    pub fn check(&mut self, items: u64, bytes: u64) -> RuntimeResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(SinexError::validation("work cancelled"));
        }
        if self
            .budget
            .max_runtime
            .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            self.stop_reason = Some(WorkStopReason::RuntimeBudget);
            self.progress.blocked_on = Some("runtime_budget".to_owned());
            return Err(SinexError::validation("work runtime budget exhausted"));
        }
        if self
            .budget
            .max_items
            .is_some_and(|limit| self.progress.items_done.saturating_add(items) > limit)
        {
            self.stop_reason = Some(WorkStopReason::ItemBudget);
            self.progress.blocked_on = Some("item_budget".to_owned());
            return Err(SinexError::validation("work item budget exhausted"));
        }
        if self
            .budget
            .max_bytes
            .is_some_and(|limit| self.progress.bytes_done.saturating_add(bytes) > limit)
        {
            self.stop_reason = Some(WorkStopReason::ByteBudget);
            self.progress.blocked_on = Some("byte_budget".to_owned());
            return Err(SinexError::validation("work byte budget exhausted"));
        }
        Ok(())
    }

    /// Cooperatively pause behind an external pressure signal (PSI, a
    /// database-pool gate, a stream backlog, or a caller-defined policy).
    /// Pressure is a scheduling condition, not a correctness limit: once the
    /// signal clears, the same operation continues from its latest cursor.
    pub async fn wait_for_pressure<F>(
        &mut self,
        is_pressured: F,
        poll_interval: Duration,
    ) -> RuntimeResult<()>
    where
        F: Fn() -> bool,
    {
        let poll_interval = poll_interval.max(Duration::from_millis(1));
        while is_pressured() {
            self.check(0, 0)?;
            self.progress.blocked_on = Some("pressure".to_owned());
            tokio::select! {
                () = tokio::time::sleep(poll_interval) => {}
                () = self.cancellation.wake.notified() => {
                    return Err(SinexError::validation("work cancelled while pressure limited"));
                }
            }
        }
        self.progress.blocked_on = None;
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

    pub fn destructive_boundary_check(&mut self) -> RuntimeResult<()> {
        self.check(0, 0)
    }
}
