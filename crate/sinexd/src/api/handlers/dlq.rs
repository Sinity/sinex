//! Raw-ingest DLQ management handlers
//!
//! This module provides RPC endpoints for managing the operator-facing raw-ingest
//! DLQ in NATS:
//! - List raw DLQ statistics
//! - Peek at raw DLQ messages without removing them
//! - Requeue raw DLQ messages back to the main raw-event stream
//! - Purge raw DLQ messages

use crate::api::service_container::ServiceContainer;
use sinex_node_sdk::dlq_retry::{DlqRetryConfig, DlqRetryHandler};
use sinex_primitives::privacy::{self, ProcessingContext};
use sinex_primitives::validation::normalize_unicode;
use sinex_primitives::{Result, SinexError};
use tracing::warn;

// Re-export RPC types for consistency
pub use sinex_primitives::rpc::dlq::{
    DlqListRequest, DlqListResponse, DlqMessagePeek, DlqPeekRequest, DlqPeekResponse,
    DlqPurgeRequest, DlqPurgeResponse, DlqRequeueRequest, DlqRequeueResponse,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct SanitizedPreview {
    text: String,
    redacted: bool,
    caveats: Vec<String>,
}

fn parse_retry_count_header(headers: Option<&async_nats::HeaderMap>) -> Result<u32> {
    let Some(value) = headers.and_then(|headers| headers.get("Retry-Count")) else {
        return Ok(0);
    };

    value.as_str().parse::<u32>().map_err(|error| {
        SinexError::validation("Retry-Count header is invalid")
            .with_context("value", value)
            .with_std_error(&error)
    })
}

fn require_stream_sequence(sequence: std::result::Result<u64, String>) -> Result<u64> {
    sequence.map_err(|error| {
        SinexError::service("Failed to inspect DLQ message sequence").with_source(error)
    })
}

fn truncate_preview(payload: &str, max_chars: usize) -> String {
    let preview: String = payload.chars().take(max_chars).collect();
    if payload.chars().count() > max_chars {
        format!("{preview}...")
    } else {
        preview
    }
}

fn payload_preview(payload: &str, max_chars: usize) -> SanitizedPreview {
    const SUPPRESSED_PREVIEW: &str = "[payload preview suppressed by privacy policy]";
    const ENGINE_UNAVAILABLE_PREVIEW: &str = "[payload preview unavailable: privacy engine failed]";

    let mut current = payload.to_string();
    let mut redacted = false;
    let mut caveats = Vec::new();

    // DLQ messages may contain raw command, journal, or document-shaped payloads.
    // Run the same privacy engine across those operator-relevant contexts before
    // any bytes leave the gateway boundary.
    for context in [
        ProcessingContext::Command,
        ProcessingContext::Journal,
        ProcessingContext::Document,
    ] {
        let processed = match privacy::process(&current, context) {
            Ok(processed) => processed,
            Err(error) => {
                warn!(
                    error = %error,
                    "DLQ payload preview privacy processing failed; suppressing preview"
                );
                return SanitizedPreview {
                    text: ENGINE_UNAVAILABLE_PREVIEW.to_string(),
                    redacted: true,
                    caveats: vec!["privacy_engine_unavailable".to_string()],
                };
            }
        };

        if processed.suppressed {
            return SanitizedPreview {
                text: SUPPRESSED_PREVIEW.to_string(),
                redacted: true,
                caveats: vec!["payload_preview_suppressed".to_string()],
            };
        }

        if processed.any_matched() {
            redacted = true;
            caveats.push(format!("redacted:{context:?}"));
            current = processed.text.into_owned();
        }
    }

    SanitizedPreview {
        text: truncate_preview(&current, max_chars),
        redacted,
        caveats,
    }
}

/// Handle raw-DLQ list request - returns statistics about the raw-ingest DLQ.
pub async fn handle_dlq_list(
    services: &ServiceContainer,
    _request: DlqListRequest,
) -> Result<DlqListResponse> {
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();
    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let stats = handler
        .get_stats()
        .await
        .map_err(|error| SinexError::service("Failed to get DLQ statistics").with_source(error))?;

    let response = DlqListResponse {
        total_messages: stats.total_messages,
        total_bytes: stats.total_bytes,
        first_seq: stats.first_seq,
        last_seq: stats.last_seq,
    };

    Ok(response)
}

/// Handle raw-DLQ peek request - preview messages without removing them.
pub async fn handle_dlq_peek(
    services: &ServiceContainer,
    peek_params: DlqPeekRequest,
) -> Result<DlqPeekResponse> {
    use async_nats::jetstream;
    use futures::StreamExt;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

    let stream = js.get_stream(&dlq_stream_name).await.map_err(|error| {
        SinexError::service("Failed to get DLQ stream")
            .with_context("stream", &dlq_stream_name)
            .with_source(error)
    })?;

    // Create ephemeral consumer for peeking
    // Issue 126: Add timeout to NATS consumer creation
    let timeout = services.config().nats_consumer_create_timeout();
    let consumer = tokio::time::timeout(
        timeout,
        stream.create_consumer(jetstream::consumer::pull::Config {
            name: None, // ephemeral
            durable_name: None,
            filter_subject: env.nats_subject("events.dlq.>"),
            ack_policy: jetstream::consumer::AckPolicy::None, // Don't ack, just peek
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| {
        SinexError::timeout("Consumer creation timed out")
            .with_context("timeout_ms", timeout.as_millis())
            .with_std_error(&error)
    })?
    .map_err(|error| SinexError::service("Failed to create peek consumer").with_source(error))?;

    let mut messages = consumer
        .messages()
        .await
        .map_err(|error| SinexError::service("Failed to get messages").with_source(error))?;

    let mut previews = Vec::new();
    let mut count = 0;

    while count < peek_params.limit {
        match messages.next().await {
            Some(Ok(msg)) => {
                let retry_count = parse_retry_count_header(msg.headers.as_ref())?;

                let original_subject = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Original-Subject"))
                    .map(std::string::ToString::to_string);

                // Create safe preview of payload (limit size).
                // Normalize unicode to NFC and reject confusable/direction-override chars;
                // if normalization fails (e.g. RTL override in payload), fall back to a
                // safe placeholder so operator UIs are not misled.
                let payload_str = String::from_utf8_lossy(&msg.payload);
                let normalized_payload = match normalize_unicode(payload_str.as_ref()) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(
                            error = %e,
                            payload_len = msg.payload.len(),
                            "DLQ payload contains dangerous Unicode; replacing preview with sanitized placeholder"
                        );
                        "[payload contains dangerous Unicode characters]".to_string()
                    }
                };
                let payload_preview = payload_preview(&normalized_payload, 200);
                let sequence = require_stream_sequence(
                    msg.info()
                        .map(|info| info.stream_sequence)
                        .map_err(|error| error.to_string()),
                )?;

                previews.push(DlqMessagePeek {
                    subject: msg.subject.to_string(),
                    sequence,
                    retry_count,
                    original_subject,
                    payload_preview: payload_preview.text,
                    payload_redacted: payload_preview.redacted,
                    privacy_caveats: payload_preview.caveats,
                });

                count += 1;
            }
            Some(Err(e)) => {
                return Err(SinexError::service("Error reading DLQ message").with_source(e));
            }
            None => break, // No more messages
        }
    }

    let response = DlqPeekResponse { messages: previews };
    Ok(response)
}

/// Handle raw-DLQ requeue request - move raw-ingest failures back to the main stream.
///
/// # Authorization
///
/// This is a dangerous operation that requeues failed messages back to the main stream.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_requeue(
    services: &ServiceContainer,
    requeue_params: DlqRequeueRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<DlqRequeueResponse> {
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let requeued_count = if let Some(ref event_id) = requeue_params.event_id {
        // Requeue specific event
        info!(
            actor = %auth.actor_id(),
            event_id = %event_id,
            "DLQ requeue operation initiated"
        );
        handler.retry_by_id(event_id).await.map_err(|error| {
            SinexError::service("Failed to requeue event")
                .with_context("event_id", event_id)
                .with_source(error)
        })?;
        1usize
    } else if requeue_params.all {
        // Requeue all events
        info!(
            actor = %auth.actor_id(),
            "DLQ requeue all operation initiated"
        );
        let result = handler.retry_all().await.map_err(|error| {
            SinexError::service("Failed to requeue all DLQ messages").with_source(error)
        })?;
        if result.permanently_failed > 0 {
            warn!(
                permanently_failed = result.permanently_failed,
                "Some DLQ messages exceeded max retries and were permanently discarded"
            );
        }
        result.retried
    } else {
        return Err(SinexError::validation(
            "Must specify either 'event_id' or 'all: true'",
        ));
    };

    let response = DlqRequeueResponse {
        status: "success".to_string(),
        requeued_count: requeued_count as u64,
    };
    Ok(response)
}

/// Handle raw-DLQ purge request - permanently delete raw-ingest DLQ messages.
///
/// # Authorization
///
/// This is a destructive operation that permanently deletes ALL DLQ messages.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_purge(
    services: &ServiceContainer,
    purge_params: DlqPurgeRequest,
    auth: &crate::api::rpc_server::RpcAuthContext,
) -> Result<DlqPurgeResponse> {
    use async_nats::jetstream;
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    if !purge_params.confirm {
        return Err(SinexError::validation(
            "Purge operation requires 'confirm: true' parameter",
        ));
    }

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");

    let mut stream = js.get_stream(&dlq_stream_name).await.map_err(|error| {
        SinexError::service("Failed to get DLQ stream")
            .with_context("stream", &dlq_stream_name)
            .with_source(error)
    })?;

    // Get current stats before purge
    let info = stream
        .info()
        .await
        .map_err(|error| SinexError::service("Failed to get stream info").with_source(error))?;
    let messages_before = info.state.messages;

    info!(
        actor = %auth.actor_id(),
        messages_to_purge = messages_before,
        "DLQ purge operation initiated"
    );

    // Purge the stream
    stream
        .purge()
        .await
        .map_err(|error| SinexError::service("Failed to purge DLQ stream").with_source(error))?;

    let response = DlqPurgeResponse {
        status: "success".to_string(),
        purged_count: messages_before,
    };
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::{parse_retry_count_header, payload_preview, require_stream_sequence};
    use sinex_primitives::error::ErrorClass;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn parse_retry_count_header_defaults_when_missing() -> TestResult<()> {
        assert_eq!(parse_retry_count_header(None)?, 0);
        Ok(())
    }

    #[sinex_test]
    async fn parse_retry_count_header_rejects_invalid_value() -> TestResult<()> {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Retry-Count", "not-a-number");

        let error = parse_retry_count_header(Some(&headers))
            .expect_err("invalid Retry-Count header should fail honestly");

        assert_eq!(error.error_class(), ErrorClass::DataError);
        assert!(error.to_string().contains("Retry-Count header is invalid"));
        assert!(error.to_string().contains("not-a-number"));
        Ok(())
    }

    #[sinex_test]
    async fn require_stream_sequence_rejects_missing_metadata() -> TestResult<()> {
        let error = require_stream_sequence(Err("missing reply metadata".to_string()))
            .expect_err("missing message metadata must fail honestly");
        assert_eq!(error.error_class(), ErrorClass::TransientInfra);
        assert!(
            error
                .to_string()
                .contains("Failed to inspect DLQ message sequence")
        );
        assert!(error.to_string().contains("missing reply metadata"));
        Ok(())
    }

    #[sinex_test]
    async fn payload_preview_truncates_without_breaking_unicode() -> TestResult<()> {
        let payload = "żółw".repeat(80);
        let preview = payload_preview(&payload, 200);
        assert!(preview.text.ends_with("..."));
        assert_eq!(preview.text.chars().count(), 203);
        assert!(!preview.redacted);
        assert!(preview.caveats.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn payload_preview_redacts_raw_dlq_secret_bytes() -> TestResult<()> {
        let payload = r#"{
            "original_subject": "dev.sinex.events.raw.shell.command",
            "original_payload": {
                "command": "export GITHUB_TOKEN=ghp_abcdefghijklmnopqrstuvwxyz123456"
            }
        }"#;

        let preview = payload_preview(payload, 200);

        assert!(preview.redacted);
        assert!(
            preview
                .caveats
                .iter()
                .any(|caveat| caveat.starts_with("redacted:")),
            "redaction must be visible to machine clients: {:?}",
            preview.caveats
        );
        assert!(
            !preview
                .text
                .contains("ghp_abcdefghijklmnopqrstuvwxyz123456")
        );
        assert!(!preview.text.contains("GITHUB_TOKEN=ghp_"));
        Ok(())
    }
}
