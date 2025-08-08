//! NATS JetStream Consumer for Sinex Automata
//!
//! This module provides the NatsStreamConsumer which mirrors the RedisStreamConsumer
//! interface, enabling automata to consume events from NATS JetStream.

use async_nats::jetstream::AckKind;
use async_trait::async_trait;
use color_eyre::eyre::eyre;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sinex_core::db::models::RawEvent;
use std::collections::HashMap;
use tokio::time::Duration;
use tracing::{debug, error, info};

use crate::{SatelliteError, SatelliteResult};

/// Event filter for NATS Stream consumption (mirrors Redis implementation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    /// Source patterns to match (e.g., "terminal.*")
    pub sources: Vec<String>,
    /// Event type patterns to match (e.g., "command.*")
    pub event_types: Vec<String>,
    /// Additional filtering criteria
    pub metadata: HashMap<String, serde_json::Value>,
}

impl EventFilter {
    /// Create a new event filter
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            event_types: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Add a source pattern to the filter
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.sources.push(source.into());
        self
    }

    /// Add an event type pattern to the filter
    pub fn with_event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_types.push(event_type.into());
        self
    }

    /// Check if an event matches this filter
    pub fn matches(&self, event: &RawEvent) -> bool {
        // If no filters specified, match everything
        if self.sources.is_empty() && self.event_types.is_empty() {
            return true;
        }

        // Check source patterns
        let source_match = self.sources.is_empty()
            || self.sources.iter().any(|pattern| {
                if pattern.ends_with('*') {
                    event
                        .source
                        .as_str()
                        .starts_with(&pattern[..pattern.len() - 1])
                } else {
                    event.source.as_str() == *pattern
                }
            });

        // Check event type patterns
        let event_type_match = self.event_types.is_empty()
            || self.event_types.iter().any(|pattern| {
                if pattern.ends_with('*') {
                    event
                        .event_type
                        .as_str()
                        .starts_with(&pattern[..pattern.len() - 1])
                } else {
                    event.event_type.as_str() == *pattern
                }
            });

        source_match && event_type_match
    }

    /// Convert to NATS subject patterns for subscription
    pub fn to_subjects(&self) -> Vec<String> {
        let mut subjects = Vec::new();

        if self.sources.is_empty() && self.event_types.is_empty() {
            subjects.push("events.*.*".to_string());
        } else {
            let sources = if self.sources.is_empty() {
                vec!["*".to_string()]
            } else {
                self.sources.clone()
            };
            let event_types = if self.event_types.is_empty() {
                vec!["*".to_string()]
            } else {
                self.event_types.clone()
            };

            for source in &sources {
                for event_type in &event_types {
                    subjects.push(format!("events.{}.{}", source, event_type));
                }
            }
        }

        subjects
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for NATS Stream consumer
#[derive(Debug, Clone)]
pub struct NatsConsumerConfig {
    /// Consumer group name
    pub group_name: String,
    /// Consumer name (unique within group)
    pub consumer_name: String,
    /// Stream name to consume from
    pub stream_name: String,
    /// Batch size for reading messages
    pub batch_size: usize,
    /// Timeout for blocking reads
    pub block_timeout: Duration,
    /// Event filters to apply
    pub filters: Vec<EventFilter>,
    /// NATS server URLs
    pub nats_servers: Vec<String>,
}

impl Default for NatsConsumerConfig {
    fn default() -> Self {
        Self {
            group_name: "default".to_string(),
            consumer_name: "consumer-1".to_string(),
            stream_name: "SINEX_EVENTS".to_string(),
            batch_size: 100,
            block_timeout: Duration::from_secs(1),
            filters: Vec::new(),
            nats_servers: vec!["nats://localhost:4222".to_string()],
        }
    }
}

/// Result of processing a batch of events (matches Redis implementation)
#[derive(Debug, Clone)]
pub struct BatchProcessingResult {
    /// Number of events processed successfully
    pub processed: usize,
    /// Number of events skipped (filtered out)
    pub skipped: usize,
    /// Number of events that failed processing
    pub failed: usize,
    /// Processing duration
    pub duration: Duration,
    /// Any errors encountered
    pub errors: Vec<String>,
}

impl Default for BatchProcessingResult {
    fn default() -> Self {
        Self {
            processed: 0,
            skipped: 0,
            failed: 0,
            duration: Duration::from_millis(0),
            errors: Vec::new(),
        }
    }
}

/// Trait for processing batches of events from NATS Streams (matches Redis interface)
#[async_trait]
pub trait EventBatchProcessor: Send + Sync {
    /// Process a batch of events
    async fn process_batch(
        &mut self,
        events: Vec<RawEvent>,
    ) -> SatelliteResult<BatchProcessingResult>;

    /// Get the event filters for this processor
    fn event_filters(&self) -> Vec<EventFilter> {
        vec![]
    }

    /// Called when the processor is initialized
    async fn initialize(&mut self) -> SatelliteResult<()> {
        Ok(())
    }

    /// Called when the processor is shutting down
    async fn shutdown(&mut self) -> SatelliteResult<()> {
        Ok(())
    }
}

/// NATS Stream Consumer for automata (mirrors RedisStreamConsumer interface)
pub struct NatsStreamConsumer {
    config: NatsConsumerConfig,
    nats_client: Option<async_nats::Client>,
    jetstream: Option<async_nats::jetstream::Context>,
}

impl NatsStreamConsumer {
    /// Create a new NATS Stream consumer
    pub fn new(config: NatsConsumerConfig) -> Self {
        Self {
            config,
            nats_client: None,
            jetstream: None,
        }
    }

    /// Initialize the consumer with NATS connection
    pub async fn initialize(&mut self, nats_servers: Option<&str>) -> SatelliteResult<()> {
        let servers = if let Some(servers) = nats_servers {
            servers.split(',').map(|s| s.trim().to_string()).collect()
        } else {
            self.config.nats_servers.clone()
        };

        let client = async_nats::connect(&servers[0])
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to connect to NATS: {}", e)))?;

        let jetstream = async_nats::jetstream::new(client.clone());

        // Create or get stream
        let _stream = jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: self.config.stream_name.clone(),
                subjects: vec!["events.*.*".to_string()],
                ..Default::default()
            })
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to create stream: {}", e)))?;

        self.nats_client = Some(client);
        self.jetstream = Some(jetstream);

        info!(
            group = %self.config.group_name,
            consumer = %self.config.consumer_name,
            stream = %self.config.stream_name,
            "NATS Stream consumer initialized"
        );

        Ok(())
    }

    /// Run the consumer with the given processor (mirrors RedisStreamConsumer::run)
    pub async fn run<P>(&mut self, mut processor: P) -> SatelliteResult<()>
    where
        P: EventBatchProcessor,
    {
        let jetstream = self
            .jetstream
            .as_ref()
            .ok_or_else(|| SatelliteError::General(eyre!("Consumer not initialized")))?;

        processor.initialize().await?;

        // Get subjects from filters
        let subjects = if self.config.filters.is_empty() {
            vec!["events.*.*".to_string()]
        } else {
            self.config
                .filters
                .iter()
                .flat_map(|f| f.to_subjects())
                .collect()
        };

        // Create consumer
        let consumer = jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(format!(
                        "{}-{}",
                        self.config.group_name, self.config.consumer_name
                    )),
                    filter_subjects: subjects,
                    ..Default::default()
                },
                &self.config.stream_name,
            )
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to create consumer: {}", e)))?;

        info!(
            group = %self.config.group_name,
            consumer = %self.config.consumer_name,
            "Starting NATS Stream consumption"
        );

        loop {
            match self.read_batch(&consumer).await {
                Ok(events) => {
                    if !events.is_empty() {
                        debug!(count = events.len(), "Received event batch");

                        match processor.process_batch(events).await {
                            Ok(result) => {
                                debug!(
                                    processed = result.processed,
                                    skipped = result.skipped,
                                    failed = result.failed,
                                    "Batch processing completed"
                                );
                            }
                            Err(e) => {
                                error!(error = %e, "Batch processing failed");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to read from NATS Stream");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Read a batch of events from the NATS Stream
    async fn read_batch(
        &self,
        consumer: &async_nats::jetstream::consumer::PullConsumer,
    ) -> SatelliteResult<Vec<RawEvent>> {
        let mut messages = consumer
            .batch()
            .max_messages(self.config.batch_size)
            .expires(self.config.block_timeout)
            .messages()
            .await
            .map_err(|e| SatelliteError::General(eyre!("Failed to fetch messages: {}", e)))?;

        let mut events = Vec::new();
        let mut pending_acks = Vec::new();

        while let Some(msg) = messages.next().await {
            match msg {
                Ok(message) => {
                    match self.parse_message(&message) {
                        Ok(Some(event)) => {
                            // Apply filters
                            if self.config.filters.is_empty()
                                || self.config.filters.iter().any(|f| f.matches(&event))
                            {
                                events.push(event);
                                pending_acks.push(message);
                            } else {
                                // Filtered out, acknowledge immediately
                                if let Err(e) = message.ack().await {
                                    error!("Failed to acknowledge filtered message: {}", e);
                                }
                            }
                        }
                        Ok(None) => {
                            // Invalid message, acknowledge to avoid redelivery
                            if let Err(e) = message.ack().await {
                                error!("Failed to acknowledge invalid message: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse message: {}", e);
                            // NAK to retry later
                            if let Err(e) = message.ack_with(AckKind::Nak(None)).await {
                                error!("Failed to NAK message: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error fetching message: {}", e);
                    break;
                }
            }
        }

        // Acknowledge all successfully parsed messages
        for message in pending_acks {
            if let Err(e) = message.ack().await {
                error!("Failed to acknowledge message: {}", e);
            }
        }

        Ok(events)
    }

    /// Parse a NATS message into an Event
    fn parse_message(
        &self,
        message: &async_nats::jetstream::Message,
    ) -> SatelliteResult<Option<RawEvent>> {
        let payload = &message.payload;

        // Parse the event JSON directly from the message payload
        match serde_json::from_slice::<RawEvent>(payload) {
            Ok(event) => Ok(Some(event)),
            Err(e) => {
                error!("Failed to deserialize event: {}", e);
                Ok(None)
            }
        }
    }
}
