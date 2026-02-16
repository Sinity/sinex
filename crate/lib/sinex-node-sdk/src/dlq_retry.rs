//! Dead Letter Queue retry mechanism
//!
//! This module provides utilities for manually retrying messages from the DLQ.

use crate::{NodeResult, SinexError};
use async_nats::jetstream;
use futures::StreamExt;
use sinex_primitives::{environment::SinexEnvironment, units::Seconds};
use std::time::Duration;
use tracing::{error, info, warn};

// Default DLQ retry configuration values
const DEFAULT_DLQ_CONSUMER_NAME: &str = "dlq-retry-consumer";
const DEFAULT_DLQ_BATCH_SIZE: usize = 10;
const DEFAULT_DLQ_MAX_RETRIES: u32 = 3;
const DEFAULT_DLQ_RETRY_DELAY: Seconds = Seconds::from_secs(60);
const DEFAULT_DLQ_ACK_WAIT: Seconds = Seconds::from_secs(60);
const DEFAULT_DLQ_INTER_BATCH_DELAY_MS: u64 = 200;

/// Configuration for DLQ retry operations
#[derive(Debug, Clone)]
pub struct DlqRetryConfig {
    /// DLQ consumer name
    pub consumer_name: String,
    /// Batch size for processing DLQ messages
    pub batch_size: usize,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Delay between retries
    pub retry_delay: Duration,
    /// Per-message delay in milliseconds to smooth burst republishing within batches.
    /// Prevents downstream spike when an entire batch is republished at once.
    pub per_message_delay_ms: u64,
}

/// Default per-message delay (10ms) — smooths burst within a batch without
/// significantly slowing overall throughput (10 msgs × 10ms = 100ms/batch).
const DEFAULT_DLQ_PER_MESSAGE_DELAY_MS: u64 = 10;

impl Default for DlqRetryConfig {
    fn default() -> Self {
        Self {
            consumer_name: DEFAULT_DLQ_CONSUMER_NAME.to_string(),
            batch_size: DEFAULT_DLQ_BATCH_SIZE,
            max_retries: DEFAULT_DLQ_MAX_RETRIES,
            retry_delay: Duration::from_secs(DEFAULT_DLQ_RETRY_DELAY.as_secs()),
            per_message_delay_ms: DEFAULT_DLQ_PER_MESSAGE_DELAY_MS,
        }
    }
}

/// DLQ retry handler
pub struct DlqRetryHandler {
    nats_client: async_nats::Client,
    env: SinexEnvironment,
    config: DlqRetryConfig,
}

impl DlqRetryHandler {
    /// Create a new DLQ retry handler
    pub fn new(
        nats_client: async_nats::Client,
        env: SinexEnvironment,
        config: DlqRetryConfig,
    ) -> Self {
        Self {
            nats_client,
            env,
            config,
        }
    }

    /// Retry all messages from DLQ
    pub async fn retry_all(&self) -> NodeResult<usize> {
        info!("Starting DLQ retry operation");

        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream = self.env.nats_stream_name("EVENTS_DLQ");

        let stream = js
            .get_stream(&dlq_stream)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(self.config.consumer_name.clone()),
                durable_name: Some(self.config.consumer_name.clone()),
                filter_subject: self.env.nats_subject("events.dlq.>"),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(DEFAULT_DLQ_ACK_WAIT.as_secs()),
                max_ack_pending: self.config.batch_size as i64,
                ..Default::default()
            })
            .await
            .map_err(|e| SinexError::processing("Failed to create DLQ consumer").with_source(e))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ messages").with_source(e))?;

        let mut retried = 0;
        let mut processed = 0u64;

        while let Some(result) = messages.next().await {
            match result {
                Ok(msg) => {
                    if self.handle_dlq_message(&js, &msg).await? {
                        retried += 1;
                    }
                    processed += 1;

                    // Per-message delay: smooth burst republishing within a batch
                    if self.config.per_message_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(self.config.per_message_delay_ms))
                            .await;
                    }

                    // Additional inter-batch pause to avoid overwhelming downstream
                    if processed.is_multiple_of(self.config.batch_size as u64) {
                        tokio::time::sleep(Duration::from_millis(DEFAULT_DLQ_INTER_BATCH_DELAY_MS))
                            .await;
                    }
                }
                Err(e) => {
                    error!("Error reading DLQ message: {e}");
                }
            }
        }

        info!("DLQ retry complete: {retried} messages retried");
        Ok(retried)
    }

    /// Retry a specific message by ID
    pub async fn retry_by_id(&self, event_id: &str) -> NodeResult<()> {
        info!("Retrying specific event: {event_id}");

        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream = self.env.nats_stream_name("EVENTS_DLQ");

        let stream = js
            .get_stream(&dlq_stream)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                name: Some(format!("{}-specific", self.config.consumer_name)),
                durable_name: None,
                filter_subject: format!("{}.{}", self.env.nats_subject("events.dlq"), event_id),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(DEFAULT_DLQ_ACK_WAIT.as_secs()),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SinexError::processing("Failed to create specific DLQ consumer").with_source(e)
            })?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| SinexError::processing("Failed to get messages").with_source(e))?;

        // Use timeout to avoid blocking forever when event doesn't exist
        let next_msg = tokio::time::timeout(Duration::from_secs(5), messages.next()).await;
        if let Ok(Some(Ok(msg))) = next_msg {
            let retry_count = msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Retry-Count"))
                .and_then(|v| v.as_str().parse::<u32>().ok())
                .unwrap_or(0);

            self.retry_message(&js, &msg, retry_count).await?;
            msg.ack().await.map_err(|e| {
                SinexError::processing(format!("Failed to ack retried message: {e}"))
            })?;

            info!("Successfully retried event: {event_id}");
        } else {
            return Err(SinexError::processing(format!(
                "Event not found in DLQ: {event_id}"
            )));
        }

        Ok(())
    }

    /// Process a single DLQ message: check retry count, retry or permanently fail.
    /// Returns `true` if the message was successfully retried.
    async fn handle_dlq_message(
        &self,
        js: &jetstream::Context,
        msg: &jetstream::Message,
    ) -> NodeResult<bool> {
        let retry_count = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Retry-Count"))
            .and_then(|v| v.as_str().parse::<u32>().ok())
            .unwrap_or(0);

        if retry_count >= self.config.max_retries {
            warn!("Message exceeded max retries ({retry_count}), permanently failing");
            if let Err(e) = msg.ack().await {
                error!("Failed to ack permanently failed message: {e}");
            }
            return Ok(false);
        }

        match self.retry_message(js, msg, retry_count).await {
            Ok(()) => {
                if let Err(e) = msg.ack().await {
                    error!("Failed to ack retried message: {e}");
                }
                Ok(true)
            }
            Err(e) => {
                error!("Failed to retry message: {e}");
                Ok(false)
            }
        }
    }

    async fn retry_message(
        &self,
        js: &jetstream::Context,
        msg: &jetstream::Message,
        retry_count: u32,
    ) -> NodeResult<()> {
        let original_subject = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("Original-Subject"))
            .ok_or_else(|| SinexError::processing("Missing Original-Subject header".to_string()))?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_count_str = (retry_count + 1).to_string();
        let retried_at_str =
            sinex_primitives::temporal::format_rfc3339(sinex_primitives::temporal::now());
        headers.insert("Retry-Count", retry_count_str.as_str());
        headers.insert("Retried-At", retried_at_str.as_str());

        if let Some(ref original_headers) = msg.headers {
            if let Some(msg_id) = original_headers.get("Nats-Msg-Id") {
                headers.insert("Nats-Msg-Id", msg_id.as_str());
            }
        }

        js.publish_with_headers(original_subject.to_string(), headers, msg.payload.clone())
            .await
            .map_err(|e| SinexError::processing("Failed to republish message").with_source(e))?
            .await
            .map_err(|e| SinexError::processing("Failed to await publish ack").with_source(e))?;

        Ok(())
    }

    /// Get DLQ statistics
    pub async fn get_stats(&self) -> NodeResult<DlqStats> {
        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream_name = self.env.nats_stream_name("EVENTS_DLQ");

        let mut stream = js
            .get_stream(&dlq_stream_name)
            .await
            .map_err(|e| SinexError::processing("Failed to get DLQ stream").with_source(e))?;

        let info = stream
            .info()
            .await
            .map_err(|e| SinexError::processing("Failed to get stream info").with_source(e))?;

        Ok(DlqStats {
            total_messages: info.state.messages,
            total_bytes: info.state.bytes,
            first_seq: info.state.first_sequence,
            last_seq: info.state.last_sequence,
        })
    }
}

/// DLQ statistics
#[derive(Debug, Clone)]
pub struct DlqStats {
    pub total_messages: u64,
    pub total_bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::jetstream;
    use sinex_primitives::environment::environment;
    use xtask::sandbox::{sinex_test, EphemeralNats};

    #[sinex_test]
    fn test_dlq_retry_config_defaults() -> TestResult<()> {
        let config = DlqRetryConfig::default();
        assert_eq!(config.consumer_name, "dlq-retry-consumer");
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay, Duration::from_secs(60));
        assert_eq!(config.per_message_delay_ms, 10);
        Ok(())
    }

    #[sinex_test]
    async fn dlq_retry_errors_without_stream() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment().clone();
        let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());

        let err = handler.get_stats().await.unwrap_err();
        assert!(
            err.to_string().contains("Failed to get DLQ stream"),
            "got: {err}"
        );

        let err = handler.retry_all().await.unwrap_err();
        assert!(
            err.to_string().contains("Failed to get DLQ stream"),
            "got: {err}"
        );

        let err = handler.retry_by_id("missing").await.unwrap_err();
        assert!(
            err.to_string().contains("Failed to get DLQ stream"),
            "got: {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn dlq_retry_by_id_reports_missing_event() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment().clone();
        let js = jetstream::new(client.clone());

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

        let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());
        let err = handler.retry_by_id("missing").await.unwrap_err();
        assert!(err.to_string().contains("Event not found in DLQ"));
        Ok(())
    }
}
