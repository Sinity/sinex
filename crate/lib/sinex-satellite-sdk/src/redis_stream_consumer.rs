//! Redis Stream Consumer for Sinex Automata
//!
//! This module provides the RedisStreamConsumer which is used by automata
//! to consume events from Redis Streams in real-time.

use async_trait::async_trait;
use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use sinex_db::models::Event;
use std::collections::HashMap;
use tokio::time::Duration;
use tracing::{debug, error, info};

use crate::{SatelliteError, SatelliteResult};

/// Event filter for Redis Stream consumption
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
    pub fn matches(&self, event: &Event) -> bool {
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
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for Redis Stream consumer
#[derive(Debug, Clone)]
pub struct RedisConsumerConfig {
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
}

impl Default for RedisConsumerConfig {
    fn default() -> Self {
        Self {
            group_name: "default".to_string(),
            consumer_name: "consumer-1".to_string(),
            stream_name: "sinex:events".to_string(),
            batch_size: 100,
            block_timeout: Duration::from_secs(1),
            filters: Vec::new(),
        }
    }
}

/// Result of processing a batch of events
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

/// Trait for processing batches of events from Redis Streams
#[async_trait]
pub trait EventBatchProcessor: Send + Sync {
    /// Process a batch of events
    async fn process_batch(&mut self, events: Vec<Event>)
        -> SatelliteResult<BatchProcessingResult>;

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

/// Redis Stream Consumer for automata
pub struct RedisStreamConsumer {
    config: RedisConsumerConfig,
    redis_client: Option<redis::Client>,
}

impl RedisStreamConsumer {
    /// Create a new Redis Stream consumer
    pub fn new(config: RedisConsumerConfig) -> Self {
        Self {
            config,
            redis_client: None,
        }
    }

    /// Initialize the consumer with Redis connection
    pub async fn initialize(&mut self, redis_url: &str) -> SatelliteResult<()> {
        let client = redis::Client::open(redis_url).map_err(|e| SatelliteError::Redis(e))?;

        // Test connection
        let mut conn = client
            .get_async_connection()
            .await
            .map_err(|e| SatelliteError::Redis(e))?;

        // Create consumer group if it doesn't exist
        let _: Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.config.stream_name)
            .arg(&self.config.group_name)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        self.redis_client = Some(client);

        info!(
            group = %self.config.group_name,
            consumer = %self.config.consumer_name,
            stream = %self.config.stream_name,
            "Redis Stream consumer initialized"
        );

        Ok(())
    }

    /// Run the consumer with the given processor
    pub async fn run<P>(&mut self, mut processor: P) -> SatelliteResult<()>
    where
        P: EventBatchProcessor,
    {
        let client = self
            .redis_client
            .as_ref()
            .ok_or_else(|| SatelliteError::General(eyre!("Consumer not initialized")))?;

        let mut conn = client
            .get_async_connection()
            .await
            .map_err(|e| SatelliteError::Redis(e))?;

        processor.initialize().await?;

        info!(
            group = %self.config.group_name,
            consumer = %self.config.consumer_name,
            "Starting Redis Stream consumption"
        );

        loop {
            match self.read_batch(&mut conn).await {
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
                    error!(error = %e, "Failed to read from Redis Stream");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Read a batch of events from the Redis Stream
    async fn read_batch(&self, conn: &mut redis::aio::Connection) -> SatelliteResult<Vec<Event>> {
        let result: Vec<redis::Value> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.config.group_name)
            .arg(&self.config.consumer_name)
            .arg("COUNT")
            .arg(self.config.batch_size)
            .arg("BLOCK")
            .arg(self.config.block_timeout.as_millis() as u64)
            .arg("STREAMS")
            .arg(&self.config.stream_name)
            .arg(">")
            .query_async(conn)
            .await
            .map_err(|e| SatelliteError::Redis(e))?;

        let mut events = Vec::new();

        // Parse Redis Stream response format
        // XREADGROUP returns: [[stream_name, [[message_id, [field, value, ...]], ...]]]
        if let Some(redis::Value::Bulk(streams)) = result.first() {
            if let Some(redis::Value::Bulk(stream_data)) = streams.first() {
                if let Some(redis::Value::Bulk(messages)) = stream_data.get(1) {
                    for message in messages {
                        if let redis::Value::Bulk(msg_parts) = message {
                            if let Some(event) = self.parse_message(msg_parts)? {
                                // Apply filters
                                if self.config.filters.is_empty()
                                    || self.config.filters.iter().any(|f| f.matches(&event))
                                {
                                    events.push(event);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    /// Parse a Redis Stream message into an Event
    fn parse_message(&self, msg_parts: &[redis::Value]) -> SatelliteResult<Option<Event>> {
        if msg_parts.len() < 2 {
            return Ok(None);
        }

        // Extract fields from the message
        if let redis::Value::Bulk(fields) = &msg_parts[1] {
            let mut field_map = HashMap::new();

            for chunk in fields.chunks(2) {
                if chunk.len() == 2 {
                    if let (redis::Value::Data(key), redis::Value::Data(value)) =
                        (&chunk[0], &chunk[1])
                    {
                        if let (Ok(key_str), Ok(value_str)) =
                            (std::str::from_utf8(key), std::str::from_utf8(value))
                        {
                            field_map.insert(key_str.to_string(), value_str.to_string());
                        }
                    }
                }
            }

            // Parse the event JSON from the "event" field
            if let Some(event_json) = field_map.get("event") {
                // First validate the JSON structure
                let validated_json = sinex_types::validate_json(event_json)
                    .map_err(|e| SatelliteError::General(eyre!("Invalid event JSON: {}", e)))?;

                // Then deserialize the validated JSON
                let event: Event = serde_json::from_value(validated_json)
                    .map_err(|e| SatelliteError::Serialization(e))?;
                return Ok(Some(event));
            }
        }

        Ok(None)
    }
}
