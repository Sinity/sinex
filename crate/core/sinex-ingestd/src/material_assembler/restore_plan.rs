//! Pure restore-plan derivation for material assembler startup recovery.
//!
//! This module owns the typed decision boundary for crash recovery. Filesystem,
//! database, and content-store probes build a [`RestorePlanInput`]; plan derivation
//! itself is a pure function so the classification and audit trace can be tested
//! without standing up the assembler runtime.

use super::state::{AssemblyPhase, MaterialEndMessage, PendingWrite, WalEntry};
use serde::{Deserialize, Serialize};
use sinex_primitives::Uuid;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RestorePlan {
    pub material_id: Uuid,
    pub classification: RestoreClassification,
    pub trace: Vec<RestorePlanTrace>,
}

impl RestorePlan {
    #[must_use]
    pub fn cleanup_state(&self) -> bool {
        matches!(
            self.classification,
            RestoreClassification::Discard {
                cleanup_state: true,
                ..
            }
        )
    }

    #[must_use]
    pub fn restores_state(&self) -> bool {
        matches!(
            self.classification,
            RestoreClassification::Keep { .. } | RestoreClassification::Finalize { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreClassification {
    Keep {
        reason: RestoreKeepReason,
    },
    Finalize {
        reason: RestoreFinalizeReason,
    },
    Discard {
        reason: RestoreDiscardReason,
        cleanup_state: bool,
    },
    Quarantine {
        reason: RestoreQuarantineReason,
    },
}

impl fmt::Display for RestoreClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Keep { reason } => write!(f, "keep:{reason:?}"),
            Self::Finalize { reason } => write!(f, "finalize:{reason:?}"),
            Self::Discard {
                reason,
                cleanup_state,
            } => write!(f, "discard:{reason:?}:cleanup={cleanup_state}"),
            Self::Quarantine { reason } => write!(f, "quarantine:{reason:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreKeepReason {
    InProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreFinalizeReason {
    PendingEndReady,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreDiscardReason {
    MissingWalWithoutArtifacts,
    CorruptWal,
    EmptyWal,
    TerminalMaterial,
    FileProgressMismatch,
    StaleIncompleteAssembly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RestoreQuarantineReason {
    MissingWalWithArtifacts,
    MissingReplayedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RestorePlanTrace {
    pub code: &'static str,
    pub detail: String,
}

impl RestorePlanTrace {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RestorePlanInput<'a> {
    pub material_id: Uuid,
    pub wal_present: bool,
    pub has_state_artifacts: bool,
    pub replay_corrupted: bool,
    pub has_envelope_entries: bool,
    pub has_non_empty_lines: bool,
    pub material_terminal: bool,
    pub file_progress_error: Option<String>,
    pub stale: bool,
    pub replayed_state: Option<&'a ReplayedState>,
}

impl<'a> RestorePlanInput<'a> {
    pub fn from_replayed(material_id: Uuid, replayed_state: &'a ReplayedState) -> Self {
        Self {
            material_id,
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: false,
            has_envelope_entries: true,
            has_non_empty_lines: true,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: Some(replayed_state),
        }
    }
}

#[must_use]
pub(super) fn derive_restore_plan(input: RestorePlanInput<'_>) -> RestorePlan {
    let mut trace = vec![RestorePlanTrace::new(
        "material",
        input.material_id.to_string(),
    )];

    if !input.wal_present {
        return if input.has_state_artifacts {
            trace.push(RestorePlanTrace::new(
                "wal_missing",
                "state directory still contains staged or buffered artifacts",
            ));
            RestorePlan {
                material_id: input.material_id,
                classification: RestoreClassification::Quarantine {
                    reason: RestoreQuarantineReason::MissingWalWithArtifacts,
                },
                trace,
            }
        } else {
            trace.push(RestorePlanTrace::new(
                "wal_missing",
                "state directory has no WAL and no recoverable artifacts",
            ));
            RestorePlan {
                material_id: input.material_id,
                classification: RestoreClassification::Discard {
                    reason: RestoreDiscardReason::MissingWalWithoutArtifacts,
                    cleanup_state: false,
                },
                trace,
            }
        };
    }

    if input.replay_corrupted {
        trace.push(RestorePlanTrace::new(
            "wal_corrupt",
            "WAL replay stopped at a corrupt or invalid envelope",
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Discard {
                reason: RestoreDiscardReason::CorruptWal,
                cleanup_state: true,
            },
            trace,
        };
    }

    if !input.has_envelope_entries {
        trace.push(RestorePlanTrace::new(
            "wal_empty",
            if input.has_non_empty_lines {
                "WAL had non-empty lines but no valid envelope entries"
            } else {
                "WAL had no valid envelope entries"
            },
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Discard {
                reason: RestoreDiscardReason::EmptyWal,
                cleanup_state: true,
            },
            trace,
        };
    }

    if input.material_terminal {
        trace.push(RestorePlanTrace::new(
            "terminal_material",
            "database says material already reached a terminal state",
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Discard {
                reason: RestoreDiscardReason::TerminalMaterial,
                cleanup_state: true,
            },
            trace,
        };
    }

    if let Some(error) = input.file_progress_error {
        trace.push(RestorePlanTrace::new("file_progress_mismatch", error));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Discard {
                reason: RestoreDiscardReason::FileProgressMismatch,
                cleanup_state: true,
            },
            trace,
        };
    }

    if input.stale {
        trace.push(RestorePlanTrace::new(
            "stale_incomplete",
            "restored assembly exceeded the slice-arrival timeout before startup",
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Discard {
                reason: RestoreDiscardReason::StaleIncompleteAssembly,
                cleanup_state: true,
            },
            trace,
        };
    }

    let Some(state) = input.replayed_state else {
        trace.push(RestorePlanTrace::new(
            "missing_replayed_state",
            "valid WAL facts were reported without a replayed state snapshot",
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Quarantine {
                reason: RestoreQuarantineReason::MissingReplayedState,
            },
            trace,
        };
    };

    if state.phase != AssemblyPhase::PendingBegin && state.pending_end.is_some() {
        trace.push(RestorePlanTrace::new(
            "pending_end",
            "restored state has a pending end record that should be retried",
        ));
        return RestorePlan {
            material_id: input.material_id,
            classification: RestoreClassification::Finalize {
                reason: RestoreFinalizeReason::PendingEndReady,
            },
            trace,
        };
    }

    trace.push(RestorePlanTrace::new(
        "in_progress",
        "restored state remains in progress",
    ));
    RestorePlan {
        material_id: input.material_id,
        classification: RestoreClassification::Keep {
            reason: RestoreKeepReason::InProgress,
        },
        trace,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct ReplayedState {
    pub expected_offset: i64,
    pub slice_count: usize,
    pub started_at: String,
    pub last_slice_received: Option<String>,
    pub material_kind: String,
    pub source_identifier: String,
    pub metadata: serde_json::Value,
    pub phase: AssemblyPhase,
    pub pending_write: Option<PendingWrite>,
    pub pending_end: Option<MaterialEndMessage>,
}

impl ReplayedState {
    pub fn apply(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Begin(msg) => {
                self.phase = AssemblyPhase::Accumulating;
                self.started_at = msg.started_at;
                self.material_kind = msg.material_kind;
                self.source_identifier = msg.source_identifier;
                self.metadata = msg.metadata;
            }
            WalEntry::Slice { offset: _, len } => {
                // WAL implies this slice was processed successfully.
                self.expected_offset += len as i64;
                self.slice_count += 1;
                self.pending_write = None;
            }
            WalEntry::End(msg) => {
                self.pending_end = Some(msg);
            }
            WalEntry::Checkpoint(state) => {
                self.expected_offset = state.expected_offset;
                self.slice_count = state.slice_count;
                self.started_at = state.started_at;
                self.last_slice_received = state.last_slice_received;
                self.material_kind = state.material_kind;
                self.source_identifier = state.source_identifier;
                self.metadata = state.metadata;
                self.phase = state.phase;
                self.pending_write = state.pending_write;
                self.pending_end = state.pending_end;
            }
            _ => {} // Buffer events do not change core state reconstruction directly.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use xtask::sandbox::prelude::*;

    fn material_id() -> Uuid {
        Uuid::now_v7()
    }

    fn replayed_state() -> ReplayedState {
        ReplayedState {
            expected_offset: 42,
            slice_count: 1,
            started_at: "2026-04-22T00:00:00Z".to_string(),
            material_kind: "test".to_string(),
            source_identifier: "test://restore-plan".to_string(),
            metadata: json!({}),
            phase: AssemblyPhase::Accumulating,
            ..Default::default()
        }
    }

    #[sinex_test]
    async fn missing_wal_without_artifacts_is_discarded_without_cleanup() -> TestResult<()> {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id: material_id(),
            wal_present: false,
            has_state_artifacts: false,
            replay_corrupted: false,
            has_envelope_entries: false,
            has_non_empty_lines: false,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::MissingWalWithoutArtifacts,
                cleanup_state: false,
            }
        ));
        assert!(!plan.cleanup_state());
        Ok(())
    }

    #[sinex_test]
    async fn missing_wal_with_artifacts_is_quarantined() -> TestResult<()> {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id: material_id(),
            wal_present: false,
            has_state_artifacts: true,
            replay_corrupted: false,
            has_envelope_entries: false,
            has_non_empty_lines: false,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Quarantine {
                reason: RestoreQuarantineReason::MissingWalWithArtifacts,
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn corrupt_wal_is_discarded_with_cleanup() -> TestResult<()> {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id: material_id(),
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: true,
            has_envelope_entries: false,
            has_non_empty_lines: true,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::CorruptWal,
                cleanup_state: true,
            }
        ));
        assert!(plan.cleanup_state());
        Ok(())
    }

    #[sinex_test]
    async fn empty_wal_is_discarded_with_cleanup() -> TestResult<()> {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id: material_id(),
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: false,
            has_envelope_entries: false,
            has_non_empty_lines: false,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::EmptyWal,
                cleanup_state: true,
            }
        ));
        assert!(plan.cleanup_state());
        Ok(())
    }

    #[sinex_test]
    async fn terminal_material_is_discarded_with_cleanup() -> TestResult<()> {
        let state = replayed_state();
        let plan = derive_restore_plan(RestorePlanInput {
            material_terminal: true,
            ..RestorePlanInput::from_replayed(material_id(), &state)
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::TerminalMaterial,
                cleanup_state: true,
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn file_progress_mismatch_is_discarded_with_cleanup() -> TestResult<()> {
        let state = replayed_state();
        let plan = derive_restore_plan(RestorePlanInput {
            file_progress_error: Some("staged file size mismatch".to_string()),
            ..RestorePlanInput::from_replayed(material_id(), &state)
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::FileProgressMismatch,
                cleanup_state: true,
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn stale_incomplete_state_is_discarded_with_cleanup() -> TestResult<()> {
        let state = replayed_state();
        let plan = derive_restore_plan(RestorePlanInput {
            stale: true,
            ..RestorePlanInput::from_replayed(material_id(), &state)
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Discard {
                reason: RestoreDiscardReason::StaleIncompleteAssembly,
                cleanup_state: true,
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn pending_end_ready_state_is_classified_for_finalization() -> TestResult<()> {
        let mut state = replayed_state();
        state.pending_end = Some(MaterialEndMessage {
            material_id: material_id().to_string(),
            ended_at: "2026-04-22T00:01:00Z".to_string(),
            content_hash: "blake3:test".to_string(),
            total_slices: 1,
            total_size_bytes: 42,
            metadata: json!({}),
        });

        let plan = derive_restore_plan(RestorePlanInput::from_replayed(material_id(), &state));

        assert!(matches!(
            plan.classification,
            RestoreClassification::Finalize {
                reason: RestoreFinalizeReason::PendingEndReady,
            }
        ));
        assert!(plan.restores_state());
        Ok(())
    }

    #[sinex_test]
    async fn missing_replayed_state_is_quarantined() -> TestResult<()> {
        let plan = derive_restore_plan(RestorePlanInput {
            material_id: material_id(),
            wal_present: true,
            has_state_artifacts: true,
            replay_corrupted: false,
            has_envelope_entries: true,
            has_non_empty_lines: true,
            material_terminal: false,
            file_progress_error: None,
            stale: false,
            replayed_state: None,
        });

        assert!(matches!(
            plan.classification,
            RestoreClassification::Quarantine {
                reason: RestoreQuarantineReason::MissingReplayedState,
            }
        ));
        assert!(!plan.restores_state());
        Ok(())
    }

    #[sinex_test]
    async fn in_progress_state_is_kept() -> TestResult<()> {
        let state = replayed_state();
        let plan = derive_restore_plan(RestorePlanInput::from_replayed(material_id(), &state));

        assert!(matches!(
            plan.classification,
            RestoreClassification::Keep {
                reason: RestoreKeepReason::InProgress,
            }
        ));
        assert!(plan.restores_state());
        Ok(())
    }
}
