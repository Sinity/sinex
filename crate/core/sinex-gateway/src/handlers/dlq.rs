//! DLQ (Dead Letter Queue) management handlers
//!
//! This module provides RPC endpoints for managing the NATS Dead Letter Queue:
//! - List DLQ statistics
//! - Peek at DLQ messages without removing them
//! - Requeue messages from DLQ back to main stream
//! - Purge DLQ messages

use color_eyre::eyre::{eyre, Context, Result};
use serde_json::Value;
use sinex_node_sdk::dlq_retry::{DlqRetryConfig, DlqRetryHandler};
use sinex_primitives::environment::SinexEnvironment;
use std::time::Duration;

// Re-export RPC types for consistency
pub use sinex_primitives::rpc::dlq::{
    DlqListResponse, DlqMessagePeek, DlqPeekRequest, DlqPeekResponse, DlqPurgeRequest,
    DlqPurgeResponse, DlqRequeueRequest, DlqRequeueResponse,
};

fn env_var_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default),
    )
}

/// Handle DLQ list request - returns statistics about DLQ
pub async fn handle_dlq_list(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    _params: Value,
) -> Result<Value> {
    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let stats = handler
        .get_stats()
        .await
        .map_err(|e| eyre!("Failed to get DLQ statistics: {}", e))?;

    let response = DlqListResponse {
        total_messages: stats.total_messages,
        total_bytes: stats.total_bytes,
        first_seq: stats.first_seq,
        last_seq: stats.last_seq,
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle DLQ peek request - preview messages without removing them
pub async fn handle_dlq_peek(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    use async_nats::jetstream;
    use futures::StreamExt;

    let peek_params: DlqPeekRequest =
        serde_json::from_value(params).wrap_err("Invalid DLQ peek parameters")?;

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("EVENTS_DLQ");

    let stream = js
        .get_stream(&dlq_stream_name)
        .await
        .map_err(|e| eyre!("Failed to get DLQ stream: {}", e))?;

    // Create ephemeral consumer for peeking
    // Issue 126: Add timeout to NATS consumer creation
    let timeout = env_var_duration_secs("SINEX_NATS_CONSUMER_CREATE_TIMEOUT_SECS", 10);
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
    .map_err(|_| eyre!("Consumer creation timed out after {:?}", timeout))?
    .map_err(|e| eyre!("Failed to create peek consumer: {}", e))?;

    let mut messages = consumer
        .messages()
        .await
        .map_err(|e| eyre!("Failed to get messages: {}", e))?;

    let mut previews = Vec::new();
    let mut count = 0;

    while count < peek_params.limit {
        match messages.next().await {
            Some(Ok(msg)) => {
                let retry_count = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Retry-Count"))
                    .and_then(|v| v.as_str().parse::<u32>().ok())
                    .unwrap_or(0);

                let original_subject = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Original-Subject"))
                    .map(|v| v.to_string());

                // Create safe preview of payload (limit size)
                let payload_str = String::from_utf8_lossy(&msg.payload);
                let payload_preview = if payload_str.len() > 200 {
                    format!("{}...", &payload_str[..200])
                } else {
                    payload_str.to_string()
                };

                let sequence = msg.info().map_or(0, |info| info.stream_sequence);

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
                return Err(eyre!("Error reading DLQ message: {}", e));
            }
            None => break, // No more messages
        }
    }

    let response = DlqPeekResponse { messages: previews };
    Ok(serde_json::to_value(response)?)
}

/// Handle DLQ requeue request - move messages back to main stream
///
/// # Authorization
///
/// This is a dangerous operation that requeues failed messages back to the main stream.
/// The auth context is logged for audit purposes.
pub async fn handle_dlq_requeue(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
    auth: &crate::rpc_server::RpcAuthContext,
) -> Result<Value> {
    use tracing::info;

    let requeue_params: DlqRequeueRequest =
        serde_json::from_value(params).wrap_err("Invalid DLQ requeue parameters")?;

    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let requeued_count = if let Some(ref event_id) = requeue_params.event_id {
        // Requeue specific event
        info!(
            token_prefix = %auth.token_prefix,
            event_id = %event_id,
            "DLQ requeue operation initiated"
        );
        handler
            .retry_by_id(event_id)
            .await
            .map_err(|e| eyre!("Failed to requeue event {}: {}", event_id, e))?;
        1
    } else if requeue_params.all {
        // Requeue all events
        info!(
            token_prefix = %auth.token_prefix,
            "DLQ requeue all operation initiated"
        );
        handler
            .retry_all()
            .await
            .map_err(|e| eyre!("Failed to requeue all DLQ messages: {}", e))?
    } else {
        return Err(eyre!("Must specify either 'event_id' or 'all: true'"));
    };

    let response = DlqRequeueResponse {
        status: "success".to_string(),
        requeued_count: requeued_count as u64,
    };
    Ok(serde_json::to_value(response)?)
}

/// Handle DLQ purge request - permanently delete DLQ messages
pub async fn handle_dlq_purge(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    use async_nats::jetstream;

    let purge_params: DlqPurgeRequest =
        serde_json::from_value(params).wrap_err("Invalid DLQ purge parameters")?;

    if !purge_params.confirm {
        return Err(eyre!("Purge operation requires 'confirm: true' parameter"));
    }

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("EVENTS_DLQ");

    let mut stream = js
        .get_stream(&dlq_stream_name)
        .await
        .map_err(|e| eyre!("Failed to get DLQ stream: {}", e))?;

    // Get current stats before purge
    let info = stream
        .info()
        .await
        .map_err(|e| eyre!("Failed to get stream info: {}", e))?;
    let messages_before = info.state.messages;

    // Purge the stream
    stream
        .purge()
        .await
        .map_err(|e| eyre!("Failed to purge DLQ stream: {}", e))?;

    let response = DlqPurgeResponse {
        status: "success".to_string(),
        purged_count: messages_before,
    };
    Ok(serde_json::to_value(response)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::{environment, temporal};
    use xtask::sandbox::{sinex_test, EphemeralNats};

    #[sinex_test]
    async fn dlq_list_returns_stats() -> TestResult<()> {
        use async_nats::jetstream;

        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();
        let js = jetstream::new(client.clone());

        // Create DLQ stream
        let stream_name = env.nats_stream_name("EVENTS_DLQ");
        js.get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![env.nats_subject("events.dlq.>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 1000,
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;

        let result = handle_dlq_list(&client, &env, json!({})).await?;
        assert!(result.get("total_messages").is_some());
        assert!(result.get("total_bytes").is_some());

        Ok(())
    }

    #[sinex_test]
    async fn dlq_requeue_requires_params() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        let test_auth = crate::rpc_server::RpcAuthContext {
            token_prefix: "test****".to_string(),
            authenticated_at: temporal::now(),
        };

        // Should fail without event_id or all flag
        let err = handle_dlq_requeue(&client, &env, json!({}), &test_auth)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Must specify either"));

        Ok(())
    }

    #[sinex_test]
    async fn dlq_purge_requires_confirmation() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();

        // Should fail without confirm flag
        let err = handle_dlq_purge(&client, &env, json!({"confirm": false}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires 'confirm: true'"));

        Ok(())
    }
}
