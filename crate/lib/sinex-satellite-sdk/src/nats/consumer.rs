//! NATS JetStream consumer for Sinex events

use super::{
    error::{NatsError, Result},
    jetstream::JetStream,
};
use async_nats::jetstream::{
    self,
    consumer::{
        pull::Config as PullConfig, AckPolicy, DeliverPolicy, PullConsumer,
        ReplayPolicy as JsReplayPolicy,
    },
    AckKind, Message,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Consumer name
    pub name: String,

    /// Consumer group (durable name)
    pub group: String,

    /// Stream name to consume from
    pub stream: String,

    /// Subject filter (optional)
    pub filter_subject: Option<String>,

    /// Acknowledgment timeout
    #[serde(with = "humantime_serde")]
    pub ack_wait: Duration,

    /// Maximum delivery attempts
    pub max_deliver: i64,

    /// Maximum number of messages to process in parallel
    pub max_ack_pending: i64,

    /// Batch size for fetching messages
    pub batch_size: usize,

    /// Replay policy
    pub replay_policy: ConsumerReplayPolicy,

    /// Deliver policy
    pub deliver_policy: ConsumerDeliverPolicy,
}

/// Consumer replay policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsumerReplayPolicy {
    Instant,
    Original,
}

/// Consumer deliver policy
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsumerDeliverPolicy {
    All,
    Last,
    New,
    ByStartSequence(u64),
    ByStartTime(chrono::DateTime<chrono::Utc>),
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            name: "default-consumer".to_string(),
            group: "default-group".to_string(),
            stream: "SINEX_RAW_EVENTS".to_string(),
            filter_subject: None,
            ack_wait: Duration::from_secs(30),
            max_deliver: 3,
            max_ack_pending: 1000,
            batch_size: 100,
            replay_policy: ConsumerReplayPolicy::Instant,
            deliver_policy: ConsumerDeliverPolicy::New,
        }
    }
}

impl ConsumerConfig {
    /// Convert to JetStream consumer config
    ///
    /// # Errors
    /// Returns `Result<PullConfig, NatsError>` if time conversion fails
    pub fn to_jetstream_config(&self) -> Result<PullConfig> {
        let replay_policy = match self.replay_policy {
            ConsumerReplayPolicy::Instant => JsReplayPolicy::Instant,
            ConsumerReplayPolicy::Original => JsReplayPolicy::Original,
        };

        let deliver_policy = match self.deliver_policy {
            ConsumerDeliverPolicy::All => DeliverPolicy::All,
            ConsumerDeliverPolicy::Last => DeliverPolicy::Last,
            ConsumerDeliverPolicy::New => DeliverPolicy::New,
            ConsumerDeliverPolicy::ByStartSequence(seq) => DeliverPolicy::ByStartSequence {
                start_sequence: seq,
            },
            ConsumerDeliverPolicy::ByStartTime(time) => {
                // Convert chrono DateTime to time OffsetDateTime
                let timestamp = time.timestamp();
                let nanos = time.timestamp_subsec_nanos();
                let start_time = time::OffsetDateTime::from_unix_timestamp(timestamp)
                    .map_err(|e| NatsError::Consumer(format!("Invalid unix timestamp: {}", e)))?
                    .replace_nanosecond(nanos)
                    .map_err(|e| NatsError::Consumer(format!("Invalid nanosecond value: {}", e)))?;
                DeliverPolicy::ByStartTime { start_time }
            }
        };

        Ok(PullConfig {
            name: Some(self.name.clone()),
            durable_name: Some(self.group.clone()),
            description: Some(format!("Sinex consumer: {}", self.name)),
            ack_policy: AckPolicy::Explicit,
            ack_wait: self.ack_wait,
            max_deliver: self.max_deliver,
            max_ack_pending: self.max_ack_pending,
            replay_policy,
            deliver_policy,
            filter_subjects: self.filter_subject.clone().into_iter().collect(),
            ..Default::default()
        })
    }
}

/// NATS consumer for processing events
pub struct NatsConsumer {
    config: ConsumerConfig,
    jetstream: JetStream,
    consumer: Option<PullConsumer>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl NatsConsumer {
    /// Create a new consumer
    pub async fn new(jetstream: JetStream, config: ConsumerConfig) -> Result<Self> {
        let consumer = jetstream
            .get_or_create_consumer(&config.stream, config.to_jetstream_config()?)
            .await?;

        info!(
            name = config.name,
            group = config.group,
            stream = config.stream,
            "Created NATS consumer"
        );

        Ok(Self {
            config,
            jetstream,
            consumer: Some(consumer),
            shutdown_tx: None,
        })
    }

    /// Start consuming messages
    pub async fn start<F, Fut>(&mut self, handler: F) -> Result<()>
    where
        F: Fn(RawEvent, Message) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| NatsError::Consumer("Consumer not initialized".to_string()))?;

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let batch_size = self.config.batch_size;
        let consumer_name = self.config.name.clone();

        // In async-nats 0.37, we use fetch() for pull consumers

        info!(
            consumer = consumer_name,
            batch_size = batch_size,
            "Started consuming messages"
        );

        // Create a message stream
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| NatsError::Consumer(format!("Failed to create message stream: {}", e)))?;

        info!(
            consumer = consumer_name,
            batch_size = batch_size,
            "Started consuming messages"
        );

        // Process messages
        loop {
            tokio::select! {
                Some(message) = messages.next() => {
                    match message {
                        Ok(msg) => {
                            debug!(
                                consumer = consumer_name,
                                subject = msg.subject.as_str(),
                                "Received message"
                            );

                            // Extract event ID from headers
                            let event_id = msg.headers
                                .as_ref()
                                .and_then(|h| h.get("Sinex-Event-Id"))
                                .map(|v| v.as_str());

                            // Deserialize event
                            match serde_json::from_slice::<RawEvent>(&msg.payload) {
                                Ok(event) => {
                                    // Process the event
                                    match handler(event, msg.clone()).await {
                                        Ok(()) => {
                                            // Acknowledge the message
                                            if let Err(e) = msg.ack().await {
                                                error!(
                                                    consumer = consumer_name,
                                                    event_id = event_id,
                                                    error = %e,
                                                    "Failed to acknowledge message"
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                consumer = consumer_name,
                                                event_id = event_id,
                                                error = %e,
                                                "Failed to process event"
                                            );

                                            // NAK the message for redelivery
                                            if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                                                error!(
                                                    consumer = consumer_name,
                                                    error = %e,
                                                    "Failed to NAK message"
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        consumer = consumer_name,
                                        error = %e,
                                        "Failed to deserialize event"
                                    );

                                    // Acknowledge to avoid reprocessing corrupt messages
                                    if let Err(e) = msg.ack().await {
                                        error!(
                                            consumer = consumer_name,
                                            error = %e,
                                            "Failed to acknowledge corrupt message"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                consumer = consumer_name,
                                error = %e,
                                "Error receiving message"
                            );
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!(consumer = consumer_name, "Received shutdown signal");
                    break;
                }
            }
        }

        info!(consumer = consumer_name, "Consumer stopped");
        Ok(())
    }

    /// Process messages in batches
    pub async fn process_batch<F, Fut>(
        &mut self,
        batch_size: usize,
        handler: F,
    ) -> Result<Vec<RawEvent>>
    where
        F: Fn(Vec<RawEvent>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| NatsError::Consumer("Consumer not initialized".to_string()))?;

        // Use the batch() method for batch processing in async-nats 0.37
        let mut messages = consumer
            .batch()
            .max_messages(batch_size)
            .expires(Duration::from_secs(5))
            .messages()
            .await
            .map_err(|e| NatsError::Consumer(format!("Failed to create batch stream: {}", e)))?;

        let mut events = Vec::new();
        let mut jet_messages = Vec::new();

        while let Some(msg) = messages.next().await {
            match msg {
                Ok(message) => {
                    match serde_json::from_slice::<RawEvent>(&message.payload) {
                        Ok(event) => {
                            events.push(event);
                            jet_messages.push(message);
                        }
                        Err(e) => {
                            error!("Failed to deserialize event: {}", e);
                            // Acknowledge corrupt message
                            if let Err(e) = message.ack().await {
                                error!("Failed to acknowledge corrupt message: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error fetching message: {}", e);
                }
            }
        }

        if !events.is_empty() {
            // Process the batch
            match handler(events.clone()).await {
                Ok(()) => {
                    // Acknowledge all messages
                    for msg in jet_messages {
                        if let Err(e) = msg.ack().await {
                            error!("Failed to acknowledge message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to process batch: {}", e);
                    // NAK all messages for redelivery
                    for msg in jet_messages {
                        if let Err(e) = msg.ack_with(AckKind::Nak(None)).await {
                            error!("Failed to NAK message: {}", e);
                        }
                    }
                    return Err(e);
                }
            }
        }

        Ok(events)
    }

    /// Stop the consumer
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Get consumer info
    pub async fn info(&mut self) -> Result<jetstream::consumer::Info> {
        let consumer = self
            .consumer
            .as_mut()
            .ok_or_else(|| NatsError::Consumer("Consumer not initialized".to_string()))?;

        let info = consumer
            .info()
            .await
            .map_err(|e| NatsError::Consumer(format!("Failed to get consumer info: {}", e)))?;
        Ok(info.clone())
    }

    /// Delete the consumer
    pub async fn delete(mut self) -> Result<()> {
        self.stop().await;

        if let Some(consumer) = self.consumer.take() {
            // Consumer is automatically deleted when dropped if it's ephemeral
            drop(consumer);
        }

        // For durable consumers, we need to explicitly delete
        self.jetstream
            .delete_consumer(&self.config.stream, &self.config.group)
            .await?;

        info!(
            name = self.config.name,
            group = self.config.group,
            "Deleted consumer"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_consumer_config() -> color_eyre::eyre::Result<()> {
        let config = ConsumerConfig::default();
        assert_eq!(config.name, "default-consumer");
        assert_eq!(config.stream, "SINEX_RAW_EVENTS");

        let js_config = config.to_jetstream_config();
        assert_eq!(js_config.ack_policy, AckPolicy::Explicit);
        Ok(())
    }

    #[sinex_test]
    fn test_consumer_config_serialization() -> color_eyre::eyre::Result<()> {
        let config = ConsumerConfig {
            name: "test-consumer".to_string(),
            group: "test-group".to_string(),
            stream: "TEST_STREAM".to_string(),
            filter_subject: Some("test.>".to_string()),
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ConsumerConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.filter_subject, deserialized.filter_subject);
        Ok(())
    }
}
