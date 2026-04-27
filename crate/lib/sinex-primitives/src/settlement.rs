//! Settlement types for receipt-gated runtime contracts.
//!
//! These types enable the runtime to mechanically execute failure handling
//! decisions instead of guessing. Nodes return a `BatchSettlement`; the runtime
//! executes it. DLQ is a settlement variant, not a catch-all fallback.

use crate::error::{ErrorClass, SinexError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A single event's outcome within a batch settlement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventSettlement {
    /// Event processed successfully with these side effects.
    Committed {
        effects: Vec<EffectIntent>,
        progress: ProgressProposal,
    },
    /// Event data was invalid — route to processing-failure.
    DataFailed {
        failure: ProcessingFailureIntent,
    },
    /// Transient failure — retry with backoff, finite budget.
    Retry {
        reason: SinexError,
        backoff: Backoff,
        budget: RetryBudget,
    },
    /// Park the input; do not advance progress. Resume when dependency recovers.
    Park {
        reason: ParkReason,
    },
    /// Permanently fatal — halt the runtime unit.
    Halt {
        reason: HaltReason,
    },
}

/// Policy for events beyond the failed one in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemainingPolicy {
    /// Process remaining events normally.
    Continue,
    /// Skip remaining events, park the batch.
    ParkRemaining,
    /// Halt the runtime unit — remaining events are unprocessed.
    HaltRemaining,
}

/// Outcome of processing a batch of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSettlement {
    pub outcomes: Vec<EventSettlement>,
    pub remaining: RemainingPolicy,
}

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

/// A receipt proving a side effect was durably accepted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Receipt {
    JetStreamAccepted {
        stream: String,
        sequence: u64,
        msg_id: String,
    },
    NatsKvRevision {
        bucket: String,
        key: String,
        revision: u64,
    },
    LocalSegmentFsynced {
        path: String,
        segment: u64,
        offset: u64,
    },
    InputAcked {
        stream: String,
        consumer: String,
        sequence: u64,
    },
    QuarantineStored {
        store: String,
        key: String,
    },
}

/// Durability domain for a receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityDomain {
    /// Remote JetStream/NATS KV — survives host loss.
    Remote,
    /// Local disk fsync — survives process restart.
    Local,
}

impl Receipt {
    #[must_use]
    pub fn durability_domain(&self) -> DurabilityDomain {
        match self {
            Self::JetStreamAccepted { .. }
            | Self::NatsKvRevision { .. }
            | Self::InputAcked { .. } => DurabilityDomain::Remote,
            Self::LocalSegmentFsynced { .. } | Self::QuarantineStored { .. } => DurabilityDomain::Local,
        }
    }
}

/// A required side effect that must be durably accepted before progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectIntent {
    pub effect_id: String,
    pub kind: EffectKind,
    pub idempotency_key: String,
    pub required_for_progress: bool,
    pub payload: serde_json::Value,
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

/// Deterministic idempotency key for a derived output effect.
#[must_use]
pub fn derived_output_effect_id(
    node_id: &str,
    input_event_ids: &[uuid::Uuid],
    output_kind: &str,
    output_sequence: u64,
    schema_version: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"derived-output");
    hasher.update(node_id.as_bytes());
    for id in input_event_ids {
        hasher.update(id.as_bytes());
    }
    hasher.update(output_kind.as_bytes());
    hasher.update(&output_sequence.to_le_bytes());
    hasher.update(schema_version.as_bytes());
    hasher.finalize().to_hex()[..32].to_string()
}

/// Deterministic idempotency key for a processing failure effect.
#[must_use]
pub fn processing_failure_effect_id(
    node_id: &str,
    input_event_id: uuid::Uuid,
    error_fingerprint: &str,
    policy_version: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"processing-failure");
    hasher.update(node_id.as_bytes());
    hasher.update(input_event_id.as_bytes());
    hasher.update(error_fingerprint.as_bytes());
    hasher.update(policy_version.as_bytes());
    hasher.finalize().to_hex()[..32].to_string()
}

/// Deterministic idempotency key for a scope invalidation effect.
#[must_use]
pub fn invalidation_effect_id(
    operation_id: uuid::Uuid,
    scope_hash: &str,
    invalidation_kind: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"scope-invalidation");
    hasher.update(operation_id.as_bytes());
    hasher.update(scope_hash.as_bytes());
    hasher.update(invalidation_kind.as_bytes());
    hasher.finalize().to_hex()[..32].to_string()
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
    Retry { backoff: Backoff, budget: RetryBudget },
    Park { reason: ParkReason },
    Quarantine { reason: String },
    HaltNode { reason: HaltReason },
    DrainRuntimeUnit { reason: String },
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
    pub event_id: crate::Uuid,
    pub error: SinexError,
    pub error_fingerprint: String,
    pub effect_id: String,
}

/// Default failure policy: maps ErrorClass → Settlement with standard budgets.
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
                            max_elapsed: Some(Duration::from_secs(300)),
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
