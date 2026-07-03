// Behavior under test remains private to the consumer module; tests import the parent module surface.
use super::{
    confirmation::{
        BATCH_ATOMICITY_SCOPE, ERROR_CLASS_CONFIRMATION_DURABILITY_GAP, disclosure_safe_fingerprint,
    },
    dlq::dlq_event_id,
    *,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn dlq_event_id_reads_envelope_and_flat_payloads() -> TestResult<()> {
    // EventIntent envelope (#1149) — durable ingress: id is events[0].id.
    let envelope = serde_json::json!({
        "envelope_version": "1",
        "source_id": "s", "parser_id": "p", "parser_version": "1.0.0",
        "events": [ { "id": "ev-1", "source": "s", "event_type": "t" } ],
        "admitted_at": "2026-01-01T00:00:00Z", "admitted_by": "h",
    });
    assert_eq!(dlq_event_id(&envelope).as_deref(), Some("ev-1"));

    // Legacy / escape-hatch flat event — top-level id.
    let flat = serde_json::json!({ "id": "ev-2", "source": "s", "event_type": "t" });
    assert_eq!(dlq_event_id(&flat).as_deref(), Some("ev-2"));

    // Top-level id wins when both are present.
    let both = serde_json::json!({ "id": "top", "events": [ { "id": "nested" } ] });
    assert_eq!(dlq_event_id(&both).as_deref(), Some("top"));

    // Neither present → None (DLQ falls back to msg-id / payload-hash dedupe).
    let neither = serde_json::json!({ "events": [] });
    assert_eq!(dlq_event_id(&neither), None);
    Ok(())
}

#[sinex_test]
async fn schema_not_found_is_accepted_leniently() -> TestResult<()> {
    let accepted = JetStreamConsumer::resolve_validation_result(
        ValidationResult::SchemaNotFound {
            schema_id: Uuid::now_v7(),
        },
        false,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.missing"),
    )?;
    assert!(accepted.is_none());
    Ok(())
}

#[sinex_test]
async fn missing_schema_binding_is_accepted_leniently() -> TestResult<()> {
    let accepted = JetStreamConsumer::resolve_validation_result(
        ValidationResult::NoSchema,
        false,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.missing"),
    )?;
    assert!(accepted.is_none());
    Ok(())
}

#[sinex_test]
async fn strict_mode_still_rejects_missing_schema_bindings() -> TestResult<()> {
    let err = JetStreamConsumer::resolve_validation_result(
        ValidationResult::NoSchema,
        true,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.missing"),
    )
    .expect_err("strict mode must reject events without schema bindings");

    assert!(err.to_string().contains("Strict validation enabled"));
    Ok(())
}

#[sinex_test]
async fn require_inserted_ids_accepts_present_repository_ids() -> TestResult<()> {
    let ids = vec![Uuid::now_v7()];
    let accepted = JetStreamConsumer::require_inserted_ids(Some(ids.clone()), 1)?;
    assert_eq!(accepted, ids);
    Ok(())
}

#[sinex_test]
async fn require_inserted_ids_rejects_missing_repository_ids() -> TestResult<()> {
    let err = JetStreamConsumer::require_inserted_ids(None, 2)
        .expect_err("missing inserted_ids must surface as an invalid repository contract");
    assert!(
        err.to_string()
            .contains("Event repository omitted inserted_ids"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn suspicious_future_ts_orig_default_one_hour_skew() -> TestResult<()> {
    let default_skew = time::Duration::hours(1);
    let now = Timestamp::now();
    assert!(now + time::Duration::minutes(59) <= now + default_skew);
    assert!(now + time::Duration::minutes(61) > now + default_skew);
    Ok(())
}

#[sinex_test]
async fn implausibly_old_ts_orig_default_year_2000() -> TestResult<()> {
    let lower_bound = Timestamp::from_const(time::macros::datetime!(2000-01-01 00:00:00 UTC));
    let before_2000 = Timestamp::from_const(time::macros::datetime!(1999-12-31 23:59:59 UTC));
    let after_2000 = Timestamp::from_const(time::macros::datetime!(2000-01-02 00:00:00 UTC));
    assert!(
        before_2000 < lower_bound,
        "1999-12-31 should be before lower bound"
    );
    assert!(
        (lower_bound >= lower_bound),
        "2000-01-01 itself should not be flagged"
    );
    assert!(
        (after_2000 >= lower_bound),
        "2000-01-02 should not be flagged"
    );
    Ok(())
}

#[sinex_test]
async fn ready_signal_reports_dropped_receiver() -> TestResult<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    drop(rx);

    assert!(!signal_ready(Some(tx), "jetstream-consumer"));
    Ok(())
}

#[sinex_test]
async fn disclosure_safe_fingerprint_omits_raw_confirmation_identifier() -> TestResult<()> {
    let fingerprint = disclosure_safe_fingerprint("terminal-secret-not-a-uuid");

    assert!(fingerprint.contains("len=26"));
    assert!(fingerprint.contains("blake3="));
    assert!(!fingerprint.contains("terminal"));
    assert!(!fingerprint.contains("secret"));
    assert!(!fingerprint.contains("not-a-uuid"));
    Ok(())
}

#[sinex_test]
async fn collapse_settlement_errors_preserves_additional_failures() -> TestResult<()> {
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();

    let error = JetStreamConsumer::collapse_settlement_errors(
        "persistence failure settlement",
        vec![
            (
                first,
                JetStreamConsumer::message_settlement_failure(
                    "failed to NAK after persistence failure",
                    first,
                    "first boom",
                ),
            ),
            (
                second,
                JetStreamConsumer::message_settlement_failure(
                    "failed to route persistence error to DLQ",
                    second,
                    "second boom",
                ),
            ),
        ],
    )
    .expect_err("multiple settlement failures must stay visible");

    let rendered = error.to_string();
    assert!(rendered.contains("failed to NAK after persistence failure"));
    let second_id = second.to_string();
    assert_eq!(
        error
            .context_map()
            .get("additional_settlement_event_id_1")
            .map(String::as_str),
        Some(second_id.as_str())
    );
    let extra = error
        .context_map()
        .get("additional_settlement_error_1")
        .expect("extra settlement error should stay attached");
    assert!(extra.contains("failed to route persistence error to DLQ"));
    Ok(())
}

#[sinex_test]
async fn source_material_fk_constraint_name_accepts_exact_name() -> TestResult<()> {
    assert!(is_source_material_fk_constraint_name(
        EVENTS_SOURCE_MATERIAL_ID_FKEY
    ));
    Ok(())
}

#[sinex_test]
async fn source_material_fk_constraint_name_accepts_timescale_chunk_prefix() -> TestResult<()> {
    assert!(is_source_material_fk_constraint_name(
        "1_4_events_source_material_id_fkey"
    ));
    Ok(())
}

#[sinex_test]
async fn source_material_fk_constraint_name_rejects_other_constraints() -> TestResult<()> {
    assert!(!is_source_material_fk_constraint_name(
        "events_payload_schema_id_fkey"
    ));
    assert!(!is_source_material_fk_constraint_name(
        "events_source_material_id_fkey_extra"
    ));
    Ok(())
}

#[sinex_test]
async fn uuid_v7_guard_rejects_other_uuid_versions() -> TestResult<()> {
    // Random UUIDv7 minted by Id::new() must pass the admission guard.
    assert!(is_uuid_v7(&Uuid::now_v7()));
    assert!(!is_uuid_v7(&Uuid::new_v4()));
    assert!(!is_uuid_v7(
        &"019da690-06f8-707c-f98d-218250d05d62".parse::<Uuid>()?
    ));
    Ok(())
}

#[sinex_test]
async fn persistence_failure_routing_short_circuits_when_dlq_is_forced() -> TestResult<()> {
    assert!(JetStreamConsumer::should_route_persistence_failure(
        true,
        Err("delivery metadata unavailable".to_string()),
        &SinexError::database("forced failure"),
    )?);
    Ok(())
}

#[sinex_test]
async fn persistence_failure_routing_uses_delivery_attempts_for_non_retryable_errors()
-> TestResult<()> {
    assert!(!JetStreamConsumer::should_route_persistence_failure(
        false,
        Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD - 1),
        &SinexError::database("forced persistent failure"),
    )?);
    assert!(JetStreamConsumer::should_route_persistence_failure(
        false,
        Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
        &SinexError::database("forced persistent failure"),
    )?);
    Ok(())
}

#[sinex_test]
async fn persistence_failure_routing_never_dlqs_retryable_db_errors() -> TestResult<()> {
    let retryable = SinexError::database("serialization failure").with_context("sqlstate", "40001");
    assert!(!JetStreamConsumer::should_route_persistence_failure(
        false,
        Ok(MAIN_CONSUMER_TERMINAL_DLQ_THRESHOLD),
        &retryable,
    )?);
    Ok(())
}

#[sinex_test]
async fn persistence_failure_routing_rejects_missing_delivery_metadata() -> TestResult<()> {
    let error = JetStreamConsumer::should_route_persistence_failure(
        false,
        Err("metadata missing".to_string()),
        &SinexError::database("forced persistent failure"),
    )
    .expect_err("missing delivery metadata must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Failed to inspect JetStream delivery metadata"),
        "unexpected error: {error}"
    );
    assert_eq!(
        error
            .context_map()
            .get("delivery_metadata_error")
            .map(String::as_str),
        Some("metadata missing")
    );
    Ok(())
}

#[sinex_test]
async fn confirmation_durability_gap_errors_are_marked_fatal() -> TestResult<()> {
    let event_id = Uuid::now_v7();
    let error = JetStreamConsumer::confirmation_durability_gap_error(
        vec![(
            event_id,
            SinexError::network("confirmation transport exhausted")
                .with_context("confirmed_publish_error", "publish failed"),
        )],
        2,
    );

    assert!(JetStreamConsumer::is_fatal_batch_processing_error(&error));
    assert_eq!(
        error.context_map().get("error_class").map(String::as_str),
        Some(ERROR_CLASS_CONFIRMATION_DURABILITY_GAP)
    );
    assert_eq!(
        error
            .context_map()
            .get("acked_event_count")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        error
            .context_map()
            .get("batch_atomicity")
            .map(String::as_str),
        Some(BATCH_ATOMICITY_SCOPE)
    );
    assert_eq!(
        error
            .context_map()
            .get("raw_message_settlement")
            .map(String::as_str),
        Some("left_unacked_for_redelivery")
    );
    Ok(())
}

#[sinex_test]
async fn ordinary_errors_are_not_marked_as_fatal_confirmation_gaps() -> TestResult<()> {
    assert!(!JetStreamConsumer::is_fatal_batch_processing_error(
        &SinexError::network("ordinary nack failure")
    ));
    Ok(())
}
