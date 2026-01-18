//! Dead Letter Queue retry mechanism
//!
//! This module provides utilities for manually retrying messages from the DLQ.

use crate::{NodeError, NodeResult};
use async_nats::jetstream;
use futures::StreamExt;
use sinex_core::environment::SinexEnvironment;
use sinex_core::types::Seconds;
use std::time::Duration;
use tracing::{error, info, warn};

// Default DLQ retry configuration values
const DEFAULT_DLQ_CONSUMER_NAME: &str = "dlq-retry-consumer";
const DEFAULT_DLQ_BATCH_SIZE: usize = 10;
const DEFAULT_DLQ_MAX_RETRIES: u32 = 3;
const DEFAULT_DLQ_RETRY_DELAY: Seconds = Seconds::from_secs(60);
const DEFAULT_DLQ_ACK_WAIT: Seconds = Seconds::from_secs(60);

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
}

impl Default for DlqRetryConfig {
    fn default() -> Self {
        Self {
            consumer_name: DEFAULT_DLQ_CONSUMER_NAME.to_string(),
            batch_size: DEFAULT_DLQ_BATCH_SIZE,
            max_retries: DEFAULT_DLQ_MAX_RETRIES,
            retry_delay: Duration::from_secs(DEFAULT_DLQ_RETRY_DELAY.as_secs()),
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
            .map_err(|e| NodeError::Processing(format!("Failed to get DLQ stream: {}", e)))?;

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
            .map_err(|e| NodeError::Processing(format!("Failed to create DLQ consumer: {}", e)))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to get DLQ messages: {}", e)))?;

        let mut retried = 0;

        while let Some(result) = messages.next().await {
            match result {
                Ok(msg) => {
                    let retry_count = msg
                        .headers
                        .as_ref()
                        .and_then(|h| h.get("Retry-Count"))
                        .and_then(|v| v.as_str().parse::<u32>().ok())
                        .unwrap_or(0);

                    if retry_count >= self.config.max_retries {
                        warn!(
                            "Message exceeded max retries ({}), permanently failing",
                            retry_count
                        );
                        if let Err(e) = msg.ack().await {
                            error!("Failed to ack permanently failed message: {}", e);
                        }
                        continue;
                    }

                    match self.retry_message(&js, &msg, retry_count).await {
                        Ok(()) => {
                            retried += 1;
                            if let Err(e) = msg.ack().await {
                                error!("Failed to ack retried message: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to retry message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Error reading DLQ message: {}", e);
                }
            }
        }

        info!("DLQ retry complete: {} messages retried", retried);
        Ok(retried)
    }

    /// Retry a specific message by ID
    pub async fn retry_by_id(&self, event_id: &str) -> NodeResult<()> {
        info!("Retrying specific event: {}", event_id);

        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream = self.env.nats_stream_name("EVENTS_DLQ");

        let stream = js
            .get_stream(&dlq_stream)
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to get DLQ stream: {}", e)))?;

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
                NodeError::Processing(format!("Failed to create specific DLQ consumer: {}", e))
            })?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to get messages: {}", e)))?;

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
                NodeError::Processing(format!("Failed to ack retried message: {}", e))
            })?;

            info!("Successfully retried event: {}", event_id);
        } else {
            return Err(NodeError::Processing(format!(
                "Event not found in DLQ: {}",
                event_id
            )));
        }

        Ok(())
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
            .ok_or_else(|| NodeError::Processing("Missing Original-Subject header".to_string()))?;

        let mut headers = async_nats::HeaderMap::new();
        let retry_count_str = (retry_count + 1).to_string();
        let retried_at_str = chrono::Utc::now().to_rfc3339();
        headers.insert("Retry-Count", retry_count_str.as_str());
        headers.insert("Retried-At", retried_at_str.as_str());

        if let Some(ref original_headers) = msg.headers {
            if let Some(msg_id) = original_headers.get("Nats-Msg-Id") {
                headers.insert("Nats-Msg-Id", msg_id.as_str());
            }
        }

        js.publish_with_headers(original_subject.to_string(), headers, msg.payload.clone())
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to republish message: {}", e)))?
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to await publish ack: {}", e)))?;

        Ok(())
    }

    /// Get DLQ statistics
    pub async fn get_stats(&self) -> NodeResult<DlqStats> {
        let js = jetstream::new(self.nats_client.clone());
        let dlq_stream_name = self.env.nats_stream_name("EVENTS_DLQ");

        let mut stream = js
            .get_stream(&dlq_stream_name)
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to get DLQ stream: {}", e)))?;

        let info = stream
            .info()
            .await
            .map_err(|e| NodeError::Processing(format!("Failed to get stream info: {}", e)))?;

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
    use sinex_core::environment;
    use sinex_test_utils::{sinex_test, EphemeralNats};

    #[sinex_test]
    fn test_dlq_retry_config_defaults() -> TestResult<()> {
        let config = DlqRetryConfig::default();
        assert_eq!(config.consumer_name, "dlq-retry-consumer");
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay, Duration::from_secs(60));
        Ok(())
    }

    #[sinex_test]
    async fn dlq_retry_errors_without_stream() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();
        let handler = DlqRetryHandler::new(client, env, DlqRetryConfig::default());

        let err = handler.get_stats().await.unwrap_err();
        assert!(err.to_string().contains("Failed to get DLQ stream"));

        let err = handler.retry_all().await.unwrap_err();
        assert!(err.to_string().contains("Failed to get DLQ stream"));

        let err = handler.retry_by_id("missing").await.unwrap_err();
        assert!(err.to_string().contains("Failed to get DLQ stream"));
        Ok(())
    }

    #[sinex_test]
    async fn dlq_retry_by_id_reports_missing_event() -> TestResult<()> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let env = environment();
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
