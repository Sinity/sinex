//! Material frame redelivery policy.
//!
//! The assembler consumes an ordered `JetStream` frame stream. Settlement policy is
//! intentionally separated from frame processing so transient failures cannot
//! accidentally become silent ACKs and permanent poison frames cannot loop forever.

use std::time::Duration;

use crate::event_engine::SinexError;

pub(super) const REDELIVERY_ERROR_KIND_CONTEXT: &str = "material_redelivery_error_kind";

pub(super) mod redelivery_error_class {
    pub(crate) const CONTENT_STORE_TRANSIENT: &str = "content_store_transient";
    pub(crate) const ORDERING_INCOMPLETE: &str = "ordering_incomplete";
}

const BASE_REDELIVERY_DELAY: Duration = Duration::from_millis(200);
const MAX_REDELIVERY_DELAY: Duration = Duration::from_secs(30);
const MAX_REDELIVERY_EXPONENT: u32 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RedeliveryDecision {
    Ack {
        reason: &'static str,
    },
    Nak {
        reason: &'static str,
        delay: Duration,
    },
    Dlq {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Policy table intentionally names categories the current caller can only classify via tests or future typed assembler outcomes"
)]
pub(super) enum RedeliveryErrorKind {
    MalformedFrame { reason: String },
    SchemaInvalid { reason: String },
    DuplicateMaterial { reason: String },
    DuplicateDelivery,
    OrderingIncomplete,
    ContentInvalid { reason: String },
    ContentStoreTransient,
    DatabaseTransient,
    DatabasePermanent { reason: String },
    ProcessingTransient { reason: String },
    ProcessingPermanent { reason: String },
    ConsumerPanic { panic: String },
}

impl RedeliveryDecision {
    pub(super) fn processed() -> Self {
        Self::Ack {
            reason: "material_frame_processed",
        }
    }

    pub(super) fn for_error(kind: RedeliveryErrorKind, delivery_attempt: i64) -> Self {
        match kind {
            RedeliveryErrorKind::DuplicateDelivery => Self::Ack {
                reason: "material_frame_duplicate_delivery",
            },
            RedeliveryErrorKind::OrderingIncomplete => Self::Nak {
                reason: "material_frame_ordering_incomplete",
                delay: redelivery_delay(delivery_attempt),
            },
            RedeliveryErrorKind::ContentStoreTransient => Self::Nak {
                reason: "material_content_store_transient",
                delay: redelivery_delay(delivery_attempt),
            },
            RedeliveryErrorKind::DatabaseTransient => Self::Nak {
                reason: "material_database_transient",
                delay: redelivery_delay(delivery_attempt),
            },
            RedeliveryErrorKind::ProcessingTransient { .. } => Self::Nak {
                reason: "material_frame_processing_transient",
                delay: redelivery_delay(delivery_attempt),
            },
            RedeliveryErrorKind::MalformedFrame { reason }
            | RedeliveryErrorKind::SchemaInvalid { reason }
            | RedeliveryErrorKind::DuplicateMaterial { reason }
            | RedeliveryErrorKind::ContentInvalid { reason }
            | RedeliveryErrorKind::DatabasePermanent { reason }
            | RedeliveryErrorKind::ProcessingPermanent { reason } => Self::Dlq { reason },
            RedeliveryErrorKind::ConsumerPanic { panic } => Self::Dlq {
                reason: format!("material_frame_consumer_panic: {panic}"),
            },
        }
    }

    pub(super) fn for_processing_error(error: &SinexError, delivery_attempt: i64) -> Self {
        Self::for_error(classify_processing_error(error), delivery_attempt)
    }
}

fn classify_processing_error(error: &SinexError) -> RedeliveryErrorKind {
    if let Some(kind) = error.context_map().get(REDELIVERY_ERROR_KIND_CONTEXT) {
        match kind.as_str() {
            redelivery_error_class::ORDERING_INCOMPLETE => {
                return RedeliveryErrorKind::OrderingIncomplete;
            }
            redelivery_error_class::CONTENT_STORE_TRANSIENT => {
                return RedeliveryErrorKind::ContentStoreTransient;
            }
            _ => {}
        }
    }

    if error
        .context_map()
        .get("finalization_stage")
        .is_some_and(|stage| stage == "commit_outcome_unknown")
    {
        return RedeliveryErrorKind::DatabaseTransient;
    }

    if sinex_db::query_helpers::is_retryable_db_error(error) {
        return RedeliveryErrorKind::DatabaseTransient;
    }

    match error {
        SinexError::Parse(_) | SinexError::Serialization(_) | SinexError::Validation(_) => {
            RedeliveryErrorKind::SchemaInvalid {
                reason: error.to_string(),
            }
        }
        SinexError::AlreadyExists(_) => RedeliveryErrorKind::DuplicateMaterial {
            reason: error.to_string(),
        },
        SinexError::InvalidState(_) => RedeliveryErrorKind::ProcessingPermanent {
            reason: error.to_string(),
        },
        SinexError::PermissionDenied(_)
        | SinexError::Configuration(_)
        | SinexError::MaxRetriesExceeded(_) => RedeliveryErrorKind::ProcessingPermanent {
            reason: error.to_string(),
        },
        SinexError::Database(_) | SinexError::DbPersistenceFailed(_) => {
            RedeliveryErrorKind::DatabaseTransient
        }
        SinexError::BlobStorage(_) => RedeliveryErrorKind::ContentStoreTransient,
        SinexError::Io(_) => RedeliveryErrorKind::ProcessingTransient {
            reason: error.to_string(),
        },
        SinexError::Network(_)
        | SinexError::ChannelSend(_)
        | SinexError::ChannelReceive(_)
        | SinexError::Timeout(_)
        | SinexError::ResourceExhausted(_)
        | SinexError::Unknown(_)
        | SinexError::Service(_)
        | SinexError::Processing(_)
        | SinexError::Kv(_)
        | SinexError::Checkpoint(_)
        | SinexError::Lifecycle(_)
        | SinexError::Automaton(_)
        | SinexError::Coordination(_)
        | SinexError::Cancelled(_)
        | SinexError::NotFound(_) => RedeliveryErrorKind::ProcessingTransient {
            reason: error.to_string(),
        },
        #[cfg(feature = "nats")]
        SinexError::Nats(_)
        | SinexError::NatsAckFailed(_)
        | SinexError::NatsPublish(_)
        | SinexError::NatsSubscribe(_) => RedeliveryErrorKind::ProcessingTransient {
            reason: error.to_string(),
        },
        _ => RedeliveryErrorKind::ProcessingTransient {
            reason: error.to_string(),
        },
    }
}

fn redelivery_delay(delivery_attempt: i64) -> Duration {
    let exponent = delivery_attempt
        .saturating_sub(1)
        .max(0)
        .try_into()
        .unwrap_or(MAX_REDELIVERY_EXPONENT)
        .min(MAX_REDELIVERY_EXPONENT);
    let factor = 1u128.checked_shl(exponent).unwrap_or(u128::MAX);
    let delay_ms = BASE_REDELIVERY_DELAY
        .as_millis()
        .saturating_mul(factor)
        .min(MAX_REDELIVERY_DELAY.as_millis());
    Duration::from_millis(delay_ms as u64)
}

#[cfg(test)]
#[path = "redelivery_decision_test.rs"]
mod tests;
