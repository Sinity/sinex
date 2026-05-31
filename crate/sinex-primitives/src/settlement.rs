//! Settlement types for receipt-gated runtime contracts.
//!
//! These types enable the runtime to mechanically execute failure handling
//! decisions instead of guessing. Nodes return a `Settlement`; the runtime
//! executes it. DLQ is a settlement variant, not a catch-all fallback.

use crate::error::{ErrorClass, SinexError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Why a settlement parked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParkReason {
    /// Waiting for a dependency (e.g. NATS, DB).
    DependencyUnavailable,
    /// Output channel saturated — backpressure.
    OutputBackpressure,
    /// Explicit retry budget exhausted for transient infra.
    RetryBudgetExhausted,
    /// Operator-requested pause.
    OperatorPause,
}

/// Why a settlement halted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HaltReason {
    /// Checkpoint CAS revision conflict — permanent.
    CheckpointCasConflict,
    /// Output bridge closed while node is live.
    OutputChannelClosed,
    /// Configuration is invalid or permissions denied.
    ConfigurationOrPermission,
    /// Lifecycle state corruption.
    LifecycleCorruption,
    /// Transport degraded beyond circuit-breaker budget.
    TransportDegraded,
    /// Explicit operator escalation required.
    EscalateOperator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectKind {
    DerivedOutput,
    ProcessingFailure,
    ScopeInvalidation,
    Confirmation,
    DlqRouting,
    Quarantine,
}

/// Operation context for failure classification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureContext {
    pub unit_id: String,
    pub operation: RuntimeOperation,
    pub phase: RuntimePhase,
    pub input_scope: Option<InputScope>,
    pub effect_kind: Option<EffectKind>,
    pub delivery_count: Option<u64>,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeOperation {
    ProcessBatch,
    HandleInvalidation,
    HistoricalReplay,
    OutputEmission,
    CheckpointSave,
    DlqRouting,
    JournalIngestion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimePhase {
    /// Event processing: input received, not yet mutated.
    ProcessInput,
    /// Side effect emission: publishing outputs.
    EmitEffect,
    /// Progress persistence: saving checkpoint/cursor.
    PersistProgress,
    /// Draining/shutting down.
    Drain,
    /// Recovery/replay on startup.
    Recovery,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputScope {
    pub stream: String,
    pub consumer: String,
    pub first_sequence: u64,
    pub batch_size: usize,
}

/// Policy for translating errors into settlements, with optional
/// contextual overrides for specific (error, operation, phase) tuples.
pub trait FailurePolicy: Send + Sync {
    fn settle(&self, err: &SinexError, ctx: &FailureContext) -> Settlement;
}

/// The runtime action to take for a failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Settlement {
    Commit,
    SendToProcessingFailure,
    Retry {
        backoff: Backoff,
        budget: RetryBudget,
    },
    Park {
        reason: ParkReason,
    },
    Quarantine {
        reason: String,
    },
    HaltNode {
        reason: HaltReason,
    },
    DrainRuntimeUnit {
        reason: String,
    },
}

/// Backoff strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Backoff {
    Constant(Duration),
    Exponential { base: Duration, max: Duration },
    None,
}

/// Finite retry budget. Every retry path must declare one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryBudget {
    pub max_attempts: u32,
    pub max_elapsed: Option<Duration>,
    pub backoff: Backoff,
    pub terminal: Box<Settlement>,
}

/// A progress proposal from a node to the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressProposal {
    pub advance_checkpoint: bool,
    pub advance_cursor: Option<String>,
    pub processed_event_ids: Vec<crate::Uuid>,
}

/// A processing failure intent for a single bad event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingFailureIntent {
    pub event_id: crate::Id<crate::events::Event>,
    pub error: SinexError,
    pub error_fingerprint: String,
    pub effect_id: String,
}

/// Default failure policy: maps `ErrorClass` → Settlement with standard budgets.
pub struct DefaultFailurePolicy;

impl FailurePolicy for DefaultFailurePolicy {
    fn settle(&self, err: &SinexError, ctx: &FailureContext) -> Settlement {
        match err.error_class() {
            ErrorClass::DataError => Settlement::SendToProcessingFailure,
            ErrorClass::NodeFatal => Settlement::HaltNode {
                reason: match err {
                    SinexError::Checkpoint(_) => HaltReason::CheckpointCasConflict,
                    SinexError::Lifecycle(_) => HaltReason::LifecycleCorruption,
                    SinexError::Configuration(_) | SinexError::PermissionDenied(_) => {
                        HaltReason::ConfigurationOrPermission
                    }
                    SinexError::ChannelSend(_) => HaltReason::OutputChannelClosed,
                    _ => HaltReason::EscalateOperator,
                },
            },
            ErrorClass::TransientInfra => {
                if ctx.attempts >= 10 {
                    Settlement::Park {
                        reason: ParkReason::RetryBudgetExhausted,
                    }
                } else {
                    Settlement::Retry {
                        backoff: Backoff::Exponential {
                            base: Duration::from_millis(200),
                            max: Duration::from_secs(30),
                        },
                        budget: RetryBudget {
                            max_attempts: 10,
                            max_elapsed: Some(Duration::from_mins(5)),
                            backoff: Backoff::Exponential {
                                base: Duration::from_millis(200),
                                max: Duration::from_secs(30),
                            },
                            terminal: Box::new(Settlement::HaltNode {
                                reason: HaltReason::EscalateOperator,
                            }),
                        },
                    }
                }
            }
            ErrorClass::TransportDegraded => Settlement::HaltNode {
                reason: HaltReason::TransportDegraded,
            },
        }
    }
}
