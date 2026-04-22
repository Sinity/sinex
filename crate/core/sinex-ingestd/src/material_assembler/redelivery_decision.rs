//! Material frame redelivery policy.
//!
//! The assembler consumes an ordered `JetStream` frame stream. Settlement policy is
//! intentionally separated from frame processing so transient failures cannot
//! accidentally become silent ACKs and permanent poison frames cannot loop forever.

use std::time::Duration;

use crate::SinexError;

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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn processed_frames_ack() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::processed(),
            RedeliveryDecision::Ack {
                reason: "material_frame_processed"
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn duplicate_delivery_acks_without_dlq() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::for_error(RedeliveryErrorKind::DuplicateDelivery, 1),
            RedeliveryDecision::Ack {
                reason: "material_frame_duplicate_delivery"
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn malformed_frames_route_to_dlq() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::for_error(
                RedeliveryErrorKind::MalformedFrame {
                    reason: "begin_payload_invalid".to_string(),
                },
                1,
            ),
            RedeliveryDecision::Dlq {
                reason: "begin_payload_invalid".to_string(),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn permanent_errors_route_to_dlq_with_reason() -> TestResult<()> {
        for kind in [
            RedeliveryErrorKind::SchemaInvalid {
                reason: "schema invalid".to_string(),
            },
            RedeliveryErrorKind::DuplicateMaterial {
                reason: "duplicate material".to_string(),
            },
            RedeliveryErrorKind::ContentInvalid {
                reason: "hash mismatch".to_string(),
            },
            RedeliveryErrorKind::DatabasePermanent {
                reason: "constraint violation".to_string(),
            },
            RedeliveryErrorKind::ProcessingPermanent {
                reason: "invalid state".to_string(),
            },
        ] {
            assert!(matches!(
                RedeliveryDecision::for_error(kind, 1),
                RedeliveryDecision::Dlq { .. }
            ));
        }
        Ok(())
    }

    #[sinex_test]
    async fn transient_errors_nak_with_exponential_backoff() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::for_error(RedeliveryErrorKind::DatabaseTransient, 1),
            RedeliveryDecision::Nak {
                reason: "material_database_transient",
                delay: Duration::from_millis(200),
            }
        );
        assert_eq!(
            RedeliveryDecision::for_error(RedeliveryErrorKind::DatabaseTransient, 2),
            RedeliveryDecision::Nak {
                reason: "material_database_transient",
                delay: Duration::from_millis(400),
            }
        );
        assert_eq!(
            RedeliveryDecision::for_error(RedeliveryErrorKind::ContentStoreTransient, 3),
            RedeliveryDecision::Nak {
                reason: "material_content_store_transient",
                delay: Duration::from_millis(800),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn redelivery_backoff_caps_at_ack_wait_scale() -> TestResult<()> {
        assert_eq!(redelivery_delay(100), Duration::from_secs(30));
        Ok(())
    }

    #[sinex_test]
    async fn ordering_incomplete_naks_for_later_slices() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::for_error(RedeliveryErrorKind::OrderingIncomplete, 1),
            RedeliveryDecision::Nak {
                reason: "material_frame_ordering_incomplete",
                delay: Duration::from_millis(200),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn consumer_panic_routes_to_dlq() -> TestResult<()> {
        assert_eq!(
            RedeliveryDecision::for_error(
                RedeliveryErrorKind::ConsumerPanic {
                    panic: "boom".to_string(),
                },
                1,
            ),
            RedeliveryDecision::Dlq {
                reason: "material_frame_consumer_panic: boom".to_string(),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn processing_error_context_can_force_ordering_class() -> TestResult<()> {
        let error = SinexError::service("end arrived before all slices").with_context(
            REDELIVERY_ERROR_KIND_CONTEXT,
            redelivery_error_class::ORDERING_INCOMPLETE,
        );
        assert_eq!(
            RedeliveryDecision::for_processing_error(&error, 1),
            RedeliveryDecision::Nak {
                reason: "material_frame_ordering_incomplete",
                delay: Duration::from_millis(200),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn processing_validation_error_routes_to_schema_dlq() -> TestResult<()> {
        let error = SinexError::validation("bad payload");
        assert!(matches!(
            RedeliveryDecision::for_processing_error(&error, 1),
            RedeliveryDecision::Dlq { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn retryable_database_error_naks() -> TestResult<()> {
        let error = SinexError::database("deadlock detected");
        assert_eq!(
            RedeliveryDecision::for_processing_error(&error, 1),
            RedeliveryDecision::Nak {
                reason: "material_database_transient",
                delay: Duration::from_millis(200),
            }
        );
        Ok(())
    }
}
