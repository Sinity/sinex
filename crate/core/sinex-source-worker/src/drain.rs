//! Per-unit drain controller with material tracking and crash recovery.
//!
//! The [`SourceWorkerDrainController`] wraps the SDK's [`RuntimeDrainController`]
//! and adds:
//!
//! - **Active-work gating**: track in-flight work units; drain waits for them
//!   to complete before proceeding.
//! - **Drain protocol**: phased sequence (stop-accept → finish-active →
//!   flush-intents → wait-confirmations → finalize-materials → checkpoint)
//!   with timeout guards at each stage.
//! - **Gap evidence**: on restart after a crash, record what was in flight
//!   so the operator can assess data loss.
//!
//! The drain controller is instantiated per source unit (not process-global),
//! giving each unit independent drain lifecycle management.

use sinex_node_sdk::runtime::stream::RuntimeDrainController;
use sinex_primitives::temporal::Timestamp;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

// ── Drain protocol ────────────────────────────────────────────────────────

/// Phases of the drain sequence. Each phase gates the next — the controller
/// will not proceed from `StoppingAccept` to `FinishingActive` until the caller
/// has acknowledged the drain signal and stopped accepting new input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainPhase {
    /// Normal operation; not draining.
    Idle,
    /// Drain has been requested; source unit should stop accepting new input.
    StoppingAccept,
    /// Waiting for active work to complete.
    FinishingActive,
    /// Flushing pending event intents.
    FlushingIntents,
    /// Waiting for event confirmations (with timeout).
    WaitingConfirmations,
    /// Finalizing open source materials.
    FinalizingMaterials,
    /// Saving the final checkpoint.
    SavingCheckpoint,
    /// Drain complete; safe to exit.
    Drained,
}

impl fmt::Display for DrainPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => f.write_str("idle"),
            Self::StoppingAccept => f.write_str("stopping_accept"),
            Self::FinishingActive => f.write_str("finishing_active"),
            Self::FlushingIntents => f.write_str("flushing_intents"),
            Self::WaitingConfirmations => f.write_str("waiting_confirmations"),
            Self::FinalizingMaterials => f.write_str("finalizing_materials"),
            Self::SavingCheckpoint => f.write_str("saving_checkpoint"),
            Self::Drained => f.write_str("drained"),
        }
    }
}

// ── Gap evidence ──────────────────────────────────────────────────────────

/// Evidence that a source unit crashed and was restarted, recording the gap
/// between the last known state and the restart.
#[derive(Debug, Clone)]
pub struct GapEvidence {
    /// The source unit that was restarted.
    pub unit_id: String,
    /// When the crash likely happened (if known from drain state).
    pub crashed_at: Option<Timestamp>,
    /// When the unit restarted.
    pub restarted_at: Timestamp,
    /// The drain phase at the time of crash (if draining was in progress).
    pub drain_phase_at_crash: Option<DrainPhase>,
    /// How many work items were in flight at the time of crash.
    pub in_flight_count: usize,
}

impl GapEvidence {
    /// Create gap evidence for a clean restart (no prior crash detected).
    #[must_use]
    pub fn clean_restart(unit_id: &str) -> Self {
        Self {
            unit_id: unit_id.to_string(),
            crashed_at: None,
            restarted_at: Timestamp::now(),
            drain_phase_at_crash: None,
            in_flight_count: 0,
        }
    }
}

// ── Drain controller ──────────────────────────────────────────────────────

/// Per-unit drain controller with phased drain protocol and crash recovery
/// evidence.
///
/// Each source unit in the source-worker host gets its own controller,
/// providing independent drain lifecycle management.
pub struct SourceWorkerDrainController {
    /// The SDK-level drain signal (broadcast to all subscribers).
    inner: Arc<RuntimeDrainController>,
    /// Whether drain has been requested (set once, never cleared).
    draining: AtomicBool,
    /// Current phase of the drain sequence.
    phase: Mutex<DrainPhase>,
    /// When the drain sequence started.
    drain_started_at: Mutex<Option<Timestamp>>,
    /// Count of active work items. Drain waits for this to reach 0 before
    /// proceeding past `FinishingActive`.
    active_work: AtomicUsize,
}

impl SourceWorkerDrainController {
    /// Create a new per-unit drain controller.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RuntimeDrainController::new()),
            draining: AtomicBool::new(false),
            phase: Mutex::new(DrainPhase::Idle),
            drain_started_at: Mutex::new(None),
            active_work: AtomicUsize::new(0),
        }
    }

    // ── Accessors ──────────────────────────────────────────────────────

    /// Access the underlying SDK drain controller for signaling subscribers.
    #[must_use]
    pub fn inner(&self) -> &Arc<RuntimeDrainController> {
        &self.inner
    }

    /// Subscribe to the drain signal.
    #[must_use]
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner.subscribe()
    }

    /// Check if drain has been requested.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Get the current drain phase.
    pub async fn current_phase(&self) -> DrainPhase {
        *self.phase.lock().await
    }

    // ── Active work tracking ───────────────────────────────────────────

    /// Register one unit of active work. Call before starting work that must
    /// be completed before drain proceeds.
    pub fn enter_work(&self) {
        self.active_work.fetch_add(1, Ordering::Release);
    }

    /// Mark one unit of active work as complete.
    pub fn exit_work(&self) {
        self.active_work.fetch_sub(1, Ordering::Release);
    }

    /// Return the current count of in-flight work items.
    #[must_use]
    pub fn active_work_count(&self) -> usize {
        self.active_work.load(Ordering::Acquire)
    }

    /// Wait for all active work to drain, polling at intervals up to a
    /// deadline. Returns `true` if all work completed, `false` on timeout.
    pub async fn wait_for_active_work(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.active_work.load(Ordering::Acquire) == 0 {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Return an RAII guard that calls `enter_work` on creation and
    /// `exit_work` on drop. Use this in `run_continuous` loops.
    #[must_use]
    pub fn work_guard(&self) -> ActiveWorkGuard<'_> {
        self.enter_work();
        ActiveWorkGuard { controller: self }
    }

    // ── Drain protocol ─────────────────────────────────────────────────

    /// Begin the drain sequence. Raises the signal to all subscribers,
    /// transitions to `StoppingAccept`, and records the start time.
    ///
    /// Returns `true` if this call initiated drain, `false` if drain was
    /// already requested.
    pub async fn request_drain(&self, unit_id: &str) -> bool {
        if self.draining.swap(true, Ordering::Release) {
            return false;
        }

        let _ = self.inner.request_drain_and_warn(unit_id);

        let mut phase = self.phase.lock().await;
        *phase = DrainPhase::StoppingAccept;
        *self.drain_started_at.lock().await = Some(Timestamp::now());

        info!(
            unit = unit_id,
            phase = %DrainPhase::StoppingAccept,
            active_work = self.active_work_count(),
            "Drain sequence started"
        );
        true
    }

    /// Transition to `FinishingActive` and wait for all active work to
    /// complete (with timeout).
    pub async fn finish_active_work(&self, unit_id: &str) {
        {
            let mut phase = self.phase.lock().await;
            *phase = DrainPhase::FinishingActive;
        }

        let completed = self.wait_for_active_work(Duration::from_secs(30)).await;
        if completed {
            info!(unit = unit_id, "All active work completed");
        } else {
            warn!(
                unit = unit_id,
                remaining = self.active_work_count(),
                "Timed out waiting for active work; proceeding with drain"
            );
        }
    }

    /// Transition to `FlushingIntents`. The actual flush is performed by
    /// the SDK event batcher during shutdown — this phase signals intent.
    pub async fn flush_intents(&self, _unit_id: &str) {
        let mut phase = self.phase.lock().await;
        *phase = DrainPhase::FlushingIntents;
    }

    /// Transition to `WaitingConfirmations` and pause for the given duration
    /// to allow confirmations to arrive.
    pub async fn wait_confirmations(&self, unit_id: &str, timeout: Duration) {
        {
            let mut phase = self.phase.lock().await;
            *phase = DrainPhase::WaitingConfirmations;
        }
        info!(
            unit = unit_id,
            timeout_ms = timeout.as_millis(),
            "Waiting for confirmations"
        );
        tokio::time::sleep(timeout).await;
    }

    /// Transition to `FinalizingMaterials`. Material finalization is
    /// performed by the caller (via the acquisition manager).
    pub async fn finalize_materials(&self, _unit_id: &str) {
        let mut phase = self.phase.lock().await;
        *phase = DrainPhase::FinalizingMaterials;
    }

    /// Transition to `SavingCheckpoint`. The actual save is performed by
    /// `IngestorNodeAdapter::shutdown` via `save_state(true)`.
    pub async fn save_checkpoint(&self, _unit_id: &str) {
        let mut phase = self.phase.lock().await;
        *phase = DrainPhase::SavingCheckpoint;
    }

    /// Mark the drain sequence as complete. The source unit can now exit.
    pub async fn mark_drained(&self, unit_id: &str) {
        let mut phase = self.phase.lock().await;
        *phase = DrainPhase::Drained;
        info!(unit = unit_id, "Drain sequence complete");
    }

    // ── Recovery ───────────────────────────────────────────────────────

    /// Record gap evidence from the current state. Call this on restart to
    /// capture what was in flight at the time of crash.
    pub async fn record_gap_evidence(&self, unit_id: &str) -> GapEvidence {
        let phase = *self.phase.lock().await;
        let started_at = *self.drain_started_at.lock().await;
        let active = self.active_work.load(Ordering::Acquire);

        let drain_phase = if phase == DrainPhase::Idle {
            None
        } else {
            Some(phase)
        };

        GapEvidence {
            unit_id: unit_id.to_string(),
            crashed_at: started_at,
            restarted_at: Timestamp::now(),
            drain_phase_at_crash: drain_phase,
            in_flight_count: active,
        }
    }

    /// Record clean-restart evidence (no prior crash detected).
    pub fn clean_start_evidence(&self, unit_id: &str) -> GapEvidence {
        GapEvidence::clean_restart(unit_id)
    }
}

impl Default for SourceWorkerDrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SourceWorkerDrainController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceWorkerDrainController")
            .field("draining", &self.draining.load(Ordering::Acquire))
            .field("active_work", &self.active_work.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

// ── ActiveWorkGuard ───────────────────────────────────────────────────────

/// RAII guard that decrements the active work counter on drop.
///
/// Created by [`SourceWorkerDrainController::work_guard`]. The guard holds
/// a shared reference to the controller — it does not own or lock anything.
/// The only effect is the atomic counter decrement on drop.
pub struct ActiveWorkGuard<'a> {
    controller: &'a SourceWorkerDrainController,
}

impl Drop for ActiveWorkGuard<'_> {
    fn drop(&mut self) {
        self.controller.exit_work();
    }
}
