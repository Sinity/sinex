//! DLQ (Dead Letter Queue) management handlers
//!
//! This module provides RPC endpoints for managing the NATS Dead Letter Queue:
//! - List DLQ statistics
//! - Peek at DLQ messages without removing them
//! - Requeue messages from DLQ back to main stream
//! - Purge DLQ messages

use color_eyre::eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sinex_core::environment::SinexEnvironment;
use sinex_node_sdk::dlq_retry::{DlqRetryConfig, DlqRetryHandler};

/// DLQ statistics response
#[derive(Debug, Serialize)]
pub struct DlqStatsResponse {
    pub total_messages: u64,
    pub total_bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
}

/// DLQ message peek response
#[derive(Debug, Serialize)]
pub struct DlqMessagePeek {
    pub subject: String,
    pub sequence: u64,
    pub retry_count: u32,
    pub original_subject: Option<String>,
    pub payload_preview: String,
}

/// Parameters for peeking at DLQ messages
#[derive(Debug, Deserialize)]
struct DlqPeekParams {
    #[serde(default = "default_peek_limit")]
    limit: usize,
}

fn default_peek_limit() -> usize {
    10
}

/// Parameters for requeuing DLQ messages
#[derive(Debug, Deserialize)]
struct DlqRequeueParams {
    /// Optional event ID to requeue specific message
    event_id: Option<String>,
    /// Requeue all messages if true
    #[serde(default)]
    all: bool,
}

/// Parameters for purging DLQ messages
#[derive(Debug, Deserialize)]
struct DlqPurgeParams {
    /// Confirm purge operation
    confirm: bool,
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

    let response = DlqStatsResponse {
        total_messages: stats.total_messages,
        total_bytes: stats.total_bytes,
        first_seq: stats.first_seq,
        last_seq: stats.last_seq,
    };

    Ok(json!(response))
}

/// Handle DLQ peek request - preview messages without removing them
pub async fn handle_dlq_peek(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    use async_nats::jetstream;
    use futures::StreamExt;

    let peek_params: DlqPeekParams =
        serde_json::from_value(params).wrap_err("Invalid DLQ peek parameters")?;

    let js = jetstream::new(nats_client.clone());
    let dlq_stream_name = env.nats_stream_name("EVENTS_DLQ");

    let stream = js
        .get_stream(&dlq_stream_name)
        .await
        .map_err(|e| eyre!("Failed to get DLQ stream: {}", e))?;

    // Create ephemeral consumer for peeking
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            name: None, // ephemeral
            durable_name: None,
            filter_subject: env.nats_subject("events.dlq.>"),
            ack_policy: jetstream::consumer::AckPolicy::None, // Don't ack, just peek
            deliver_policy: jetstream::consumer::DeliverPolicy::All,
            ..Default::default()
        })
        .await
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

                let sequence = msg.info().map(|info| info.stream_sequence).unwrap_or(0);

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

    Ok(json!({
        "messages": previews,
        "total_peeked": count,
    }))
}

/// Handle DLQ requeue request - move messages back to main stream
pub async fn handle_dlq_requeue(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    let requeue_params: DlqRequeueParams =
        serde_json::from_value(params).wrap_err("Invalid DLQ requeue parameters")?;

    let config = DlqRetryConfig::default();
    let handler = DlqRetryHandler::new(nats_client.clone(), env.clone(), config);

    let requeued_count = if let Some(event_id) = requeue_params.event_id {
        // Requeue specific event
        handler
            .retry_by_id(&event_id)
            .await
            .map_err(|e| eyre!("Failed to requeue event {}: {}", event_id, e))?;
        1
    } else if requeue_params.all {
        // Requeue all events
        handler
            .retry_all()
            .await
            .map_err(|e| eyre!("Failed to requeue all DLQ messages: {}", e))?
    } else {
        return Err(eyre!("Must specify either 'event_id' or 'all: true'"));
    };

    Ok(json!({
        "requeued": requeued_count,
    }))
}

/// Handle DLQ purge request - permanently delete DLQ messages
pub async fn handle_dlq_purge(
    nats_client: &async_nats::Client,
    env: &SinexEnvironment,
    params: Value,
) -> Result<Value> {
    use async_nats::jetstream;

    let purge_params: DlqPurgeParams =
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

    Ok(json!({
        "purged": messages_before,
        "status": "success",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_core::environment;
    use sinex_test_utils::{sinex_test, EphemeralNats, TestResult};

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

        // Should fail without event_id or all flag
        let err = handle_dlq_requeue(&client, &env, json!({}))
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
