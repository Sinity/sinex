use super::{
    DlqRetryHandler, combine_retry_counts, dlq_event_id, dlq_payload_event_id, dlq_requeue_target,
    ensure_durable_failure_evidence, next_requeue_generation, next_retry_count,
};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn combine_retry_counts_prefers_larger_delivery_count() -> TestResult<()> {
    let retries = combine_retry_counts(2, Ok(5))?;
    assert_eq!(retries, 5);
    Ok(())
}

#[sinex_test]
async fn combine_retry_counts_uses_stored_header_when_delivery_metadata_is_missing()
-> TestResult<()> {
    let retries = combine_retry_counts(3, Err("metadata unavailable".to_string()))?;
    assert_eq!(retries, 3);
    Ok(())
}

#[sinex_test]
async fn combine_retry_counts_rejects_missing_delivery_metadata_without_header() -> TestResult<()> {
    let error = combine_retry_counts(0, Err("metadata unavailable".to_string()))
        .expect_err("missing delivery metadata without stored retries must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Failed to inspect DLQ delivery metadata")
    );
    assert!(error.to_string().contains("metadata unavailable"));
    Ok(())
}

#[sinex_test]
async fn combine_retry_counts_rejects_delivery_count_overflow() -> TestResult<()> {
    let error = combine_retry_counts(0, Ok(i64::from(u32::MAX) + 1))
        .expect_err("overflowing delivery count must fail honestly");
    assert!(error.to_string().contains("exceeds supported range"));
    Ok(())
}

#[sinex_test]
async fn retry_arithmetic_rejects_counter_overflow() -> TestResult<()> {
    assert!(
        next_retry_count(u32::MAX)
            .expect_err("retry count overflow must fail closed")
            .to_string()
            .contains("exceeds supported range")
    );
    assert!(
        next_requeue_generation(u32::MAX)
            .expect_err("requeue generation overflow must fail closed")
            .to_string()
            .contains("exceeds supported range")
    );
    assert_eq!(next_retry_count(4)?, 5);
    assert_eq!(next_requeue_generation(4)?, 5);
    Ok(())
}

#[sinex_test]
async fn dlq_payload_event_id_rejects_invalid_json() -> TestResult<()> {
    let error = dlq_payload_event_id(br#"{"event_id":"oops""#)
        .expect_err("invalid DLQ payload JSON must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Failed to parse DLQ payload while extracting event ID")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_event_id_falls_back_to_subject_when_payload_parse_fails() -> TestResult<()> {
    let headers = async_nats::HeaderMap::new();
    let event_id = dlq_event_id(
        "events.dlq.source.00000000-0000-7000-8000-000000000001",
        &headers,
        br#"{"event_id":"oops""#,
    )?;
    assert_eq!(
        event_id.as_deref(),
        Some("00000000-0000-7000-8000-000000000001")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_event_id_rejects_payload_parse_failure_without_subject_fallback() -> TestResult<()> {
    let headers = async_nats::HeaderMap::new();
    let error = dlq_event_id("events.dlq", &headers, br#"{"event_id":"oops""#)
        .expect_err("payload parse failure without subject fallback must fail honestly");
    assert!(error.to_string().contains("subject"));
    assert!(
        error
            .to_string()
            .contains("Failed to parse DLQ payload while extracting event ID")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_uses_subject_event_id_fallback() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");

    let payload = serde_json::json!({
        "payload_authority": "exact_raw_bytes",
        "raw_bytes_base64": base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            br#"{"command":"ls"}"#,
        ),
        "original_payload": {
            "command": "ls"
        }
    });

    let target = dlq_requeue_target(
        &headers,
        "events.dlq.source.00000000-0000-7000-8000-000000000042",
        &serde_json::to_vec(&payload)?,
    )?;
    assert_eq!(
        target.event_id.as_deref(),
        Some("00000000-0000-7000-8000-000000000042")
    );
    assert_eq!(
        target.original_nats_msg_id.as_deref(),
        Some("00000000-0000-7000-8000-000000000042")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_preserves_envelope_event_id_without_reparse() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");

    let payload = serde_json::json!({
        "payload_authority": "exact_raw_bytes",
        "raw_bytes_base64": base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            br#"{"command":"pwd"}"#,
        ),
        "event_id": "00000000-0000-7000-8000-000000000099",
        "original_payload": {
            "command": "pwd"
        }
    });

    let target = dlq_requeue_target(
        &headers,
        "events.dlq.source.ignored-subject-id",
        &serde_json::to_vec(&payload)?,
    )?;
    assert_eq!(
        target.event_id.as_deref(),
        Some("00000000-0000-7000-8000-000000000099")
    );
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_rejects_redacted_preview_as_raw_input() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");
    let payload = serde_json::json!({
        "payload_authority": "operator_preview",
        "requeue_blocked_reason": "raw bytes unavailable: DLQ privacy policy redacted the payload",
        "original_payload": {"command": "<REDACTED>"}
    });

    let error = super::dlq_requeue_target(
        &headers,
        "events.dlq.shell.00000000-0000-7000-8000-000000000043",
        &serde_json::to_vec(&payload)?,
    )
    .expect_err("redacted preview must never be serialized as a raw retry");
    assert!(error.to_string().contains("operator preview"));
    assert!(error.to_string().contains("privacy policy"));
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_rejects_metadata_stub_as_raw_input() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");
    let payload = serde_json::json!({
        "payload_authority": "operator_preview",
        "requeue_blocked_reason": "raw bytes unavailable: DLQ envelope exceeded NATS publish budget",
        "original_payload": {
            "_original_payload_omitted": true,
            "_original_payload_len": 9000000
        }
    });

    let error = super::dlq_requeue_target(
        &headers,
        "events.dlq.shell.00000000-0000-7000-8000-000000000044",
        &serde_json::to_vec(&payload)?,
    )
    .expect_err("metadata stub must never be serialized as a raw retry");
    assert!(error.to_string().contains("operator preview"));
    assert!(error.to_string().contains("publish budget"));
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_decodes_exact_raw_bytes() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");
    let raw_bytes = b" {\"command\":\"ls\",\"spacing\":true} ";
    let payload = serde_json::json!({
        "payload_authority": "exact_raw_bytes",
        "raw_bytes_base64": base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            raw_bytes,
        ),
        "original_payload": {"command": "ls", "spacing": true}
    });

    let target = super::dlq_requeue_target(
        &headers,
        "events.dlq.shell.00000000-0000-7000-8000-000000000045",
        &serde_json::to_vec(&payload)?,
    )?;
    assert_eq!(target.original_payload, raw_bytes);
    Ok(())
}

#[sinex_test]
async fn dlq_requeue_target_rejects_privacy_suppressed_parse_failure_as_raw_input(
) -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Original-Subject", "events.raw.shell.command");
    let payload = serde_json::json!({
        "payload_authority": "operator_preview",
        "requeue_blocked_reason": "raw bytes unavailable: original payload failed JSON parsing and was privacy-suppressed",
        "original_payload": {
            "_raw_bytes_suppressed": true,
            "_raw_bytes_len": 17
        }
    });

    let error = super::dlq_requeue_target(
        &headers,
        "events.dlq.shell.00000000-0000-7000-8000-000000000046",
        &serde_json::to_vec(&payload)?,
    )
    .expect_err("privacy-suppressed parse failure must never be raw retry input");
    assert!(error.to_string().contains("operator preview"));
    assert!(error.to_string().contains("privacy-suppressed"));
    Ok(())
}

#[sinex_test]
async fn dlq_message_settlement_error_preserves_subject_context() -> TestResult<()> {
    let error = DlqRetryHandler::message_settlement_error(
        "failed to ack retried DLQ message",
        "events.dlq.test.subject",
        "nats unavailable",
    );

    let message = format!("{error:#}");
    assert!(message.contains("failed to ack retried DLQ message"));
    assert!(message.contains("events.dlq.test.subject"));
    assert!(message.contains("nats unavailable"));
    Ok(())
}

#[sinex_test]
async fn terminal_dlq_settlement_requires_postgres_evidence_header() -> TestResult<()> {
    let headers = async_nats::HeaderMap::new();
    let error = ensure_durable_failure_evidence(Some(&headers))
        .expect_err("terminal settlement must not discard an unrecorded DLQ entry");
    assert!(error.to_string().contains("durable failure evidence"));
    Ok(())
}
