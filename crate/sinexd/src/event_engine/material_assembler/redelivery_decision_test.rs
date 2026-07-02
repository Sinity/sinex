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
