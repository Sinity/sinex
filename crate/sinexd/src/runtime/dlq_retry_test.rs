use super::{
    DlqRetryHandler, combine_retry_counts, dlq_event_id, dlq_payload_event_id,
    dlq_requeue_target,
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
async fn combine_retry_counts_rejects_missing_delivery_metadata_without_header()
-> TestResult<()> {
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
async fn dlq_event_id_rejects_payload_parse_failure_without_subject_fallback() -> TestResult<()>
{
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
