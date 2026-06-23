//! Operation log and operation-run vocabulary.

use schemars::JsonSchema;
use std::fmt;
use std::str::FromStr;

/// Result status of an operation in the operations log.
///
/// Typed registry over the `operation_type` strings stored in `core.operations_log`.
///
/// The managed kinds are enforced by the DB `core.start_operation()` function
/// (see `sinex-schema/src/apply.rs`). Additional variants name operation
/// records emitted by API/CLI handlers that write directly to
/// `core.operations_log`. `Other` captures future or non-managed kinds without
/// breaking deserialization.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    /// Full re-ingestion of source material through the pipeline
    Replay,
    /// Move events to cold storage (live → archive)
    Archive,
    /// Move events back from cold storage (archive → live)
    Restore,
    /// Purge events from storage entirely
    Purge,
    /// Schedule permanent deletion with operator approval
    Tombstone,
    /// Requeue raw-ingest DLQ entries.
    #[serde(rename = "dlq.requeue")]
    DlqRequeue,
    /// Purge raw-ingest DLQ entries.
    #[serde(rename = "dlq.purge")]
    DlqPurge,
    /// Drain a runtime module or package.
    #[serde(rename = "runtime.drain")]
    RuntimeDrain,
    /// Resume a drained runtime module or package.
    #[serde(rename = "runtime.resume")]
    RuntimeResume,
    /// Set a runtime processing horizon.
    #[serde(rename = "runtime.set_horizon")]
    RuntimeSetHorizon,
    /// Finalize an accepted curation judgment.
    #[serde(rename = "curation.finalize")]
    CurationFinalize,
    /// Change private-mode runtime state.
    #[serde(rename = "privacy.private_mode")]
    PrivacyPrivateMode,
    /// Archive events whose source-material bytes fail integrity validation.
    #[serde(rename = "archive.integrity_mismatch")]
    ArchiveIntegrityMismatch,
    /// Rebuild derived projections/artifacts from an invalidation scope.
    #[serde(rename = "projection-rebuild")]
    ProjectionRebuild,
    /// An operation kind not in the managed set (forward-compat)
    #[serde(untagged)]
    Other(String),
}

impl OperationKind {
    /// Return the canonical string representation stored in `operations_log`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Replay => "replay",
            Self::Archive => "archive",
            Self::Restore => "restore",
            Self::Purge => "purge",
            Self::Tombstone => "tombstone",
            Self::DlqRequeue => "dlq.requeue",
            Self::DlqPurge => "dlq.purge",
            Self::RuntimeDrain => "runtime.drain",
            Self::RuntimeResume => "runtime.resume",
            Self::RuntimeSetHorizon => "runtime.set_horizon",
            Self::CurationFinalize => "curation.finalize",
            Self::PrivacyPrivateMode => "privacy.private_mode",
            Self::ArchiveIntegrityMismatch => "archive.integrity_mismatch",
            Self::ProjectionRebuild => "projection-rebuild",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OperationKind {
    type Err = !;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "replay" => Self::Replay,
            "archive" => Self::Archive,
            "restore" => Self::Restore,
            "purge" => Self::Purge,
            "tombstone" => Self::Tombstone,
            "dlq.requeue" => Self::DlqRequeue,
            "dlq.purge" => Self::DlqPurge,
            "runtime.drain" => Self::RuntimeDrain,
            "runtime.resume" => Self::RuntimeResume,
            "runtime.set_horizon" => Self::RuntimeSetHorizon,
            "curation.finalize" => Self::CurationFinalize,
            "privacy.private_mode" => Self::PrivacyPrivateMode,
            "archive.integrity_mismatch" => Self::ArchiveIntegrityMismatch,
            "projection-rebuild" => Self::ProjectionRebuild,
            other => Self::Other(other.to_string()),
        })
    }
}

impl From<&str> for OperationKind {
    fn from(s: &str) -> Self {
        s.parse().unwrap_or_else(|_| Self::Other(s.to_string()))
    }
}

impl From<String> for OperationKind {
    fn from(s: String) -> Self {
        OperationKind::from(s.as_str())
    }
}

/// Matches the values stored in `core.operations_log.result_status`.
///
/// The `Display` rendering is the on-disk representation. The
/// `#[derive(DbCheck)]` declaration below makes the schema-apply engine the
/// source of truth for the DB `CHECK` constraint: bumping `version` ships a
/// rename without a manual migration. See issue #1236.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    JsonSchema,
    sinex_macros::DbCheck,
)]
#[serde(rename_all = "snake_case")]
#[db_check(
    schema = "core",
    table = "operations_log",
    column = "result_status",
    version = 1
)]
pub enum OperationStatus {
    /// Operation is actively running
    Running,
    /// Operation completed successfully
    Success,
    /// Operation failed
    #[db_check(rename = "failure")]
    Failed,
    /// Operation was cancelled before completion
    Cancelled,
    /// Operation is queued but not yet started
    Pending,
}

/// Shared operation-run lifecycle status for parser jobs, acquisition jobs,
/// replay operations, and lifecycle workflows.
///
/// This is the canonical fine-grained status vocabulary used by
/// `raw.parser_jobs.status`, `raw.acquisition_jobs.status`,
/// `audit.replay_operations.status`, and future lifecycle tables.
/// Each variant maps to a stable string representation stored in the
/// database CHECK constraint.
///
/// The [`OperationStatus`] enum above is a coarser result-oriented
/// vocabulary used by `core.operations_log.result_status`. Keep the
/// two enums distinct — one describes the run lifecycle, the other
/// describes the terminal result.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationRunStatus {
    /// Job is declared but not yet claimed by a worker.
    Queued,
    /// A worker has claimed a lease but not yet started processing.
    Leased,
    /// Job is actively executing.
    Running,
    /// Job is blocked waiting for source material to be ready.
    WaitingMaterial,
    /// Job is blocked waiting for downstream confirmations.
    WaitingConfirmation,
    /// Job failed transiently and is waiting to retry.
    RetryWait,
    /// Job completed successfully.
    Completed,
    /// Job completed but with non-fatal warnings or partial results.
    CompletedWithCaveats,
    /// Job failed with a transient error and can be retried.
    FailedRetryable,
    /// Job failed with a permanent error and should not be retried.
    FailedPermanent,
    /// Job was cancelled before completion.
    Cancelled,
    /// Job is blocked by a policy rule (e.g., rate limit, privacy hold).
    BlockedByPolicy,
    /// Job was superseded by a newer job for the same material+parser.
    Superseded,
}

impl OperationRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::WaitingMaterial => "waiting_material",
            Self::WaitingConfirmation => "waiting_confirmation",
            Self::RetryWait => "retry_wait",
            Self::Completed => "completed",
            Self::CompletedWithCaveats => "completed_with_caveats",
            Self::FailedRetryable => "failed_retryable",
            Self::FailedPermanent => "failed_permanent",
            Self::Cancelled => "cancelled",
            Self::BlockedByPolicy => "blocked_by_policy",
            Self::Superseded => "superseded",
        }
    }

    /// Whether this status represents a terminal state (no further
    /// transitions expected).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::CompletedWithCaveats
                | Self::FailedPermanent
                | Self::Cancelled
                | Self::Superseded
        )
    }

    /// Whether this status represents an active/in-flight state where
    /// a worker holds the job.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Leased | Self::Running | Self::RetryWait)
    }
}

impl fmt::Display for OperationRunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OperationRunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "running" => Ok(Self::Running),
            "waiting_material" => Ok(Self::WaitingMaterial),
            "waiting_confirmation" => Ok(Self::WaitingConfirmation),
            "retry_wait" => Ok(Self::RetryWait),
            "completed" => Ok(Self::Completed),
            "completed_with_caveats" => Ok(Self::CompletedWithCaveats),
            "failed_retryable" => Ok(Self::FailedRetryable),
            "failed_permanent" => Ok(Self::FailedPermanent),
            "cancelled" => Ok(Self::Cancelled),
            "blocked_by_policy" => Ok(Self::BlockedByPolicy),
            "superseded" => Ok(Self::Superseded),
            _ => Err(format!("unknown operation run status: {s}")),
        }
    }
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failure"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::Pending => write!(f, "pending"),
        }
    }
}

impl std::str::FromStr for OperationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" | "in_progress" => Ok(Self::Running),
            "success" | "ok" => Ok(Self::Success),
            "failed" | "failure" | "error" | "expired" => Ok(Self::Failed),
            "cancelled" | "canceled" => Ok(Self::Cancelled),
            "pending" => Ok(Self::Pending),
            _ => Err(format!("unknown operation status: {s}")),
        }
    }
}
