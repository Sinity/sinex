//! Raw-ingest DLQ management handlers
//!
//! This module provides RPC endpoints for managing the operator-facing raw-ingest
//! DLQ in NATS:
//! - List raw DLQ statistics
//! - Peek at raw DLQ messages without removing them
//! - Requeue raw DLQ messages back to the main raw-event stream
//! - Purge raw DLQ messages

use crate::service_container::ServiceContainer;
use serde_json::Value;
use sinex_node_sdk::dlq_retry::{DlqRetryConfig, DlqRetryHandler};
use sinex_primitives::{Result, SinexError};
use tracing::warn;

// Re-export RPC types for consistency
pub use sinex_primitives::rpc::dlq::{
    DlqListResponse, DlqMessagePeek, DlqPeekRequest, DlqPeekResponse, DlqPurgeRequest,
    DlqPurgeResponse, DlqRequeueRequest, DlqRequeueResponse,
};

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

fn payload_preview(payload: &str, max_chars: usize) -> String {
    let preview: String = payload.chars().take(max_chars).collect();
    if payload.chars().count() > max_chars {
        format!("{preview}...")
    } else {
        preview
    }
}

/// Handle raw-DLQ list request - returns statistics about the raw-ingest DLQ.
pub async fn handle_dlq_list(services: &ServiceContainer, _params: Value) -> Result<Value> {
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

    serialize_dlq_response("dlq.list", response)
}

/// Handle raw-DLQ peek request - preview messages without removing them.
pub async fn handle_dlq_peek(services: &ServiceContainer, params: Value) -> Result<Value> {
    use async_nats::jetstream;
    use futures::StreamExt;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let peek_params: DlqPeekRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid DLQ peek parameters").with_std_error(&error)
    })?;

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

                // Create safe preview of payload (limit size)
                let payload_str = String::from_utf8_lossy(&msg.payload);
                let payload_preview = payload_preview(payload_str.as_ref(), 200);
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
                    payload_preview,
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
    serialize_dlq_response("dlq.peek", response)
}

/// Handle raw-DLQ requeue request - move raw-ingest failures back to the main stream.
///
/// # Authorization
///
/// This is a dangerous operation that requeues failed messages back to the main stream.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_requeue(
    services: &ServiceContainer,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let requeue_params: DlqRequeueRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid DLQ requeue parameters").with_std_error(&error)
    })?;

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
    serialize_dlq_response("dlq.requeue", response)
}

/// Handle raw-DLQ purge request - permanently delete raw-ingest DLQ messages.
///
/// # Authorization
///
/// This is a destructive operation that permanently deletes ALL DLQ messages.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_purge(
    services: &ServiceContainer,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use async_nats::jetstream;
    use tracing::info;
    let nats_client = services
        .nats_client()
        .ok_or_else(|| SinexError::configuration("NATS client is not available"))?;
    let env = services.environment();

    let purge_params: DlqPurgeRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid DLQ purge parameters").with_std_error(&error)
    })?;

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
    serialize_dlq_response("dlq.purge", response)
}

fn serialize_dlq_response<T: serde::Serialize>(method: &'static str, response: T) -> Result<Value> {
    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization(format!("failed to serialize {method} response"))
            .with_std_error(&error)
    })
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
        assert!(preview.ends_with("..."));
        assert_eq!(preview.chars().count(), 203);
        Ok(())
    }
}
