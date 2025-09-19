//! NATS JetStream publisher for Sinex events

use super::{
    error::{NatsError, Result},
    jetstream::JetStream,
    streams::StreamManager,
};
use async_nats::{jetstream::publish::PublishAck, HeaderMap};
use bytes::Bytes;
use serde::Serialize;
use sinex_core::types::ulid::Ulid;
use sinex_core::{db::models::Event, JsonValue, Provenance};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// NATS publisher for Sinex events
#[derive(Clone, Debug)]
pub struct NatsPublisher {
    jetstream: JetStream,
    buffer: Arc<Mutex<Vec<PendingMessage>>>,
    max_buffer_size: usize,
}

/// Pending message in the buffer
#[derive(Debug, Clone)]
struct PendingMessage {
    subject: String,
    headers: HeaderMap,
    payload: Bytes,
    event_id: Ulid,
}

impl NatsPublisher {
    /// Create a new NATS publisher
    pub fn new(jetstream: JetStream) -> Self {
        Self {
            jetstream,
            buffer: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size: 1000,
        }
    }

    /// Create optimized headers for an event to reduce string allocations
    fn create_event_headers(event: &Event<JsonValue>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        // Pre-allocate headers

        if let Some(id) = &event.id {
            headers.insert("Sinex-Event-Id", id.to_string());
        }

        // Avoid redundant .to_string() calls where possible
        headers.insert("Sinex-Source", event.source.as_str());
        headers.insert("Sinex-Event-Type", event.event_type.as_str());
        headers.insert("Sinex-Host", event.host.as_str());
        headers.insert(
            "Sinex-Timestamp",
            event.ts_orig.unwrap_or_else(chrono::Utc::now).to_rfc3339(),
        );

        // Add provenance information if present
        match &event.provenance {
            Provenance::Material { id, .. } => {
                headers.insert("Sinex-Source-Material-Id", id.to_string());
            }
            Provenance::Synthesis {
                source_event_ids, ..
            } => {
                headers.insert(
                    "Sinex-Source-Event-Count",
                    source_event_ids.len().to_string(),
                );
            }
        }

        headers
    }

    /// Create a new NATS publisher with custom buffer size
    pub fn with_buffer_size(jetstream: JetStream, max_buffer_size: usize) -> Self {
        Self {
            jetstream,
            buffer: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size,
        }
    }

    /// Publish a raw event
    pub async fn publish_event(&self, event: &Event<JsonValue>) -> Result<PublishAck> {
        let subject = StreamManager::event_subject(&event.source, &event.event_type);

        // Create optimized headers
        let headers = Self::create_event_headers(event);

        // Serialize event to JSON
        let payload =
            serde_json::to_vec(event).map_err(|e| NatsError::Serialization(Arc::new(e)))?;

        // Publish with headers
        self.publish_with_headers(&subject, headers, payload).await
    }

    /// Publish a message with headers
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: HeaderMap,
        payload: impl Into<Bytes>,
    ) -> Result<PublishAck> {
        let payload = payload.into();

        debug!(
            subject = subject,
            payload_size = payload.len(),
            "Publishing message to NATS"
        );

        match self
            .jetstream
            .publish_with_headers(subject, headers.clone(), payload.clone())
            .await
        {
            Ok(ack) => {
                debug!(
                    subject = subject,
                    stream = ack.stream,
                    sequence = ack.sequence,
                    "Message published successfully"
                );
                Ok(ack)
            }
            Err(e) => {
                warn!(
                    subject = subject,
                    error = %e,
                    "Failed to publish message, adding to buffer"
                );

                // Buffer the message for retry
                if let Some(event_id) = headers.get("Sinex-Event-Id") {
                    if let Ok(id) = event_id.as_str().parse::<Ulid>() {
                        self.buffer_message(subject, headers, payload, id).await?;
                    }
                }

                Err(e)
            }
        }
    }

    /// Publish a serializable message
    pub async fn publish<T: Serialize>(&self, subject: &str, message: &T) -> Result<PublishAck> {
        let payload =
            serde_json::to_vec(message).map_err(|e| NatsError::Serialization(Arc::new(e)))?;

        self.jetstream.publish(subject, payload).await
    }

    /// Publish an alert
    pub async fn publish_alert(
        &self,
        severity: &str,
        component: &str,
        message: &str,
        details: Option<serde_json::Value>,
    ) -> Result<PublishAck> {
        let subject = StreamManager::alert_subject(severity, component);

        let alert = serde_json::json!({
            "severity": severity,
            "component": component,
            "message": message,
            "details": details.unwrap_or(serde_json::json!({})),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        self.publish(&subject, &alert).await
    }

    /// Batch publish raw events for improved performance
    pub async fn publish_events_batch(
        &self,
        events: &[Event<JsonValue>],
    ) -> Result<Vec<PublishAck>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare all messages for batch publishing
        let mut batch_messages = Vec::with_capacity(events.len());

        for event in events {
            let subject = StreamManager::event_subject(&event.source, &event.event_type);

            // Use optimized header creation
            let headers = Self::create_event_headers(event);

            // Serialize event to JSON
            let payload =
                serde_json::to_vec(event).map_err(|e| NatsError::Serialization(Arc::new(e)))?;

            batch_messages.push((subject, headers, payload));
        }

        debug!(
            batch_size = batch_messages.len(),
            "Publishing event batch to NATS"
        );

        // Use the JetStream batch publishing method
        match self
            .jetstream
            .publish_batch_with_headers(batch_messages)
            .await
        {
            Ok(acks) => {
                debug!(
                    batch_size = acks.len(),
                    "Event batch published successfully"
                );
                Ok(acks)
            }
            Err(e) => {
                warn!(
                    batch_size = events.len(),
                    error = %e,
                    "Failed to publish event batch"
                );
                Err(e)
            }
        }
    }

    /// Batch publish serializable messages
    pub async fn publish_batch<T: Serialize>(
        &self,
        messages: &[(String, T)],
    ) -> Result<Vec<PublishAck>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize all messages
        let mut batch_messages = Vec::with_capacity(messages.len());
        for (subject, message) in messages {
            let payload =
                serde_json::to_vec(message).map_err(|e| NatsError::Serialization(Arc::new(e)))?;
            batch_messages.push((subject.clone(), payload));
        }

        debug!(
            batch_size = batch_messages.len(),
            "Publishing message batch to NATS"
        );

        self.jetstream.publish_batch(batch_messages).await
    }

    /// Buffer a message for later retry
    async fn buffer_message(
        &self,
        subject: &str,
        headers: HeaderMap,
        payload: Bytes,
        event_id: Ulid,
    ) -> Result<()> {
        let mut buffer = self.buffer.lock().await;

        if buffer.len() >= self.max_buffer_size {
            warn!(
                buffer_size = buffer.len(),
                max_size = self.max_buffer_size,
                "Buffer full, dropping oldest message"
            );
            buffer.remove(0);
        }

        buffer.push(PendingMessage {
            subject: subject.to_string(),
            headers,
            payload,
            event_id,
        });

        debug!(
            subject = subject,
            event_id = %event_id,
            buffer_size = buffer.len(),
            "Message buffered for retry"
        );

        Ok(())
    }

    /// Flush buffered messages using batch publishing for improved performance
    pub async fn flush_buffer(&self) -> Result<Vec<Ulid>> {
        let mut buffer = self.buffer.lock().await;
        let messages: Vec<PendingMessage> = buffer.drain(..).collect();
        drop(buffer);

        if messages.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            buffer_size = messages.len(),
            "Flushing buffered messages using batch publishing"
        );

        // Prepare batch messages for publishing
        let batch_messages: Vec<(String, HeaderMap, bytes::Bytes)> = messages
            .iter()
            .map(|msg| {
                (
                    msg.subject.clone(),
                    msg.headers.clone(),
                    msg.payload.clone(),
                )
            })
            .collect();

        let mut failed_ids = Vec::new();

        // Use batch publishing for much better performance
        match self
            .jetstream
            .publish_batch_with_headers(batch_messages)
            .await
        {
            Ok(acks) => {
                debug!(
                    batch_size = acks.len(),
                    "Batch flush completed successfully"
                );

                // All messages succeeded - no failed IDs
            }
            Err(e) => {
                error!(
                    batch_size = messages.len(),
                    error = %e,
                    "Batch flush failed, falling back to individual publishes"
                );

                // Fall back to individual publishes to identify which ones failed
                for msg in messages {
                    match self
                        .jetstream
                        .publish_with_headers(&msg.subject, msg.headers, msg.payload)
                        .await
                    {
                        Ok(ack) => {
                            debug!(
                                subject = msg.subject,
                                event_id = %msg.event_id,
                                sequence = ack.sequence,
                                "Buffered message published successfully (fallback)"
                            );
                        }
                        Err(e) => {
                            error!(
                                subject = msg.subject,
                                event_id = %msg.event_id,
                                error = %e,
                                "Failed to publish buffered message (fallback)"
                            );
                            failed_ids.push(msg.event_id);
                        }
                    }
                }
            }
        }

        Ok(failed_ids)
    }

    /// Get current buffer size
    pub async fn buffer_size(&self) -> usize {
        self.buffer.lock().await.len()
    }

    /// Clear the buffer without publishing
    pub async fn clear_buffer(&self) {
        self.buffer.lock().await.clear();
    }
}

/// Configuration for buffered publishing
#[derive(Debug, Clone)]
pub struct BufferedPublisherConfig {
    /// Maximum number of messages to batch together
    pub batch_size: usize,
    /// Maximum time to wait before flushing a partial batch
    pub flush_timeout: Duration,
    /// Maximum number of messages to buffer before applying backpressure
    pub max_buffer_size: usize,
}

impl Default for BufferedPublisherConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_timeout: Duration::from_millis(100),
            max_buffer_size: 10000,
        }
    }
}

/// Automatically batching publisher that provides optimal performance
#[derive(Clone)]
pub struct BufferedPublisher {
    sender: mpsc::UnboundedSender<BufferedMessage>,
}

/// Message queued for buffered publishing
#[derive(Debug)]
enum BufferedMessage {
    Event(
        Event<JsonValue>,
        tokio::sync::oneshot::Sender<Result<PublishAck>>,
    ),
    Message(
        String,
        Bytes,
        tokio::sync::oneshot::Sender<Result<PublishAck>>,
    ),
    Flush,
    Shutdown,
}

impl BufferedPublisher {
    /// Create a new buffered publisher with default configuration
    pub fn new(publisher: NatsPublisher) -> Self {
        Self::with_config(publisher, BufferedPublisherConfig::default())
    }

    /// Create a new buffered publisher with custom configuration
    pub fn with_config(publisher: NatsPublisher, config: BufferedPublisherConfig) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        // Spawn background task to handle batching
        tokio::spawn(Self::batch_worker(publisher, config, receiver));

        Self { sender }
    }

    /// Publish an event (returns immediately, batching happens in background)
    pub async fn publish_event(&self, event: Event<JsonValue>) -> Result<PublishAck> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(BufferedMessage::Event(event, response_tx))
            .map_err(|_| NatsError::Connection("BufferedPublisher receiver dropped".to_string()))?;

        response_rx
            .await
            .map_err(|_| NatsError::Connection("Response channel dropped".to_string()))?
    }

    /// Publish a message (returns immediately, batching happens in background)
    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<PublishAck> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(BufferedMessage::Message(subject, payload, response_tx))
            .map_err(|_| NatsError::Connection("BufferedPublisher receiver dropped".to_string()))?;

        response_rx
            .await
            .map_err(|_| NatsError::Connection("Response channel dropped".to_string()))?
    }

    /// Force flush all buffered messages immediately
    pub async fn flush(&self) -> Result<()> {
        self.sender
            .send(BufferedMessage::Flush)
            .map_err(|_| NatsError::Connection("BufferedPublisher receiver dropped".to_string()))?;

        // Give some time for flush to complete
        sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    /// Shutdown the buffered publisher, flushing all pending messages
    pub async fn shutdown(&self) -> Result<()> {
        self.sender
            .send(BufferedMessage::Shutdown)
            .map_err(|_| NatsError::Connection("BufferedPublisher receiver dropped".to_string()))?;

        // Give time for graceful shutdown
        sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    /// Background worker that handles batching and publishing
    async fn batch_worker(
        publisher: NatsPublisher,
        config: BufferedPublisherConfig,
        mut receiver: mpsc::UnboundedReceiver<BufferedMessage>,
    ) {
        let mut event_batch = Vec::new();
        let mut message_batch = Vec::new();
        let mut pending_responses: Vec<Option<tokio::sync::oneshot::Sender<Result<PublishAck>>>> =
            Vec::new();

        let mut flush_timer = tokio::time::interval(config.flush_timeout);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Process incoming messages
                msg = receiver.recv() => {
                    match msg {
                        Some(BufferedMessage::Event(event, response_tx)) => {
                            event_batch.push(event);
                            pending_responses.push(Some(response_tx));

                            // Flush if batch is full
                            if event_batch.len() >= config.batch_size {
                                Self::flush_events(&publisher, &mut event_batch, &mut pending_responses).await;
                            }
                        }
                        Some(BufferedMessage::Message(subject, payload, response_tx)) => {
                            message_batch.push((subject, payload));
                            pending_responses.push(Some(response_tx));

                            // Flush if batch is full
                            if message_batch.len() >= config.batch_size {
                                Self::flush_messages(&publisher, &mut message_batch, &mut pending_responses).await;
                            }
                        }
                        Some(BufferedMessage::Flush) => {
                            // Force flush all batches
                            Self::flush_events(&publisher, &mut event_batch, &mut pending_responses).await;
                            Self::flush_messages(&publisher, &mut message_batch, &mut pending_responses).await;
                        }
                        Some(BufferedMessage::Shutdown) => {
                            // Flush everything and exit
                            Self::flush_events(&publisher, &mut event_batch, &mut pending_responses).await;
                            Self::flush_messages(&publisher, &mut message_batch, &mut pending_responses).await;
                            break;
                        }
                        None => {
                            // Channel closed, flush and exit
                            Self::flush_events(&publisher, &mut event_batch, &mut pending_responses).await;
                            Self::flush_messages(&publisher, &mut message_batch, &mut pending_responses).await;
                            break;
                        }
                    }
                }

                // Periodic flush on timeout
                _ = flush_timer.tick() => {
                    if !event_batch.is_empty() || !message_batch.is_empty() {
                        Self::flush_events(&publisher, &mut event_batch, &mut pending_responses).await;
                        Self::flush_messages(&publisher, &mut message_batch, &mut pending_responses).await;
                    }
                }
            }
        }
    }

    async fn flush_events(
        publisher: &NatsPublisher,
        event_batch: &mut Vec<Event<JsonValue>>,
        pending_responses: &mut Vec<Option<tokio::sync::oneshot::Sender<Result<PublishAck>>>>,
    ) {
        if event_batch.is_empty() {
            return;
        }

        debug!(batch_size = event_batch.len(), "Flushing event batch");

        match publisher.publish_events_batch(event_batch).await {
            Ok(acks) => {
                // Send successful responses
                for (i, ack) in acks.into_iter().enumerate() {
                    if i < pending_responses.len() {
                        if let Some(response_tx) = pending_responses[i].take() {
                            let _ = response_tx.send(Ok(ack));
                        }
                    }
                }
                // Clear responses for this batch
                pending_responses.drain(0..event_batch.len());
            }
            Err(e) => {
                // Send error to all pending responses
                for response_tx in pending_responses.drain(0..event_batch.len()).flatten() {
                    let _ = response_tx.send(Err(e.clone()));
                }
            }
        }

        event_batch.clear();
    }

    async fn flush_messages(
        publisher: &NatsPublisher,
        message_batch: &mut Vec<(String, Bytes)>,
        pending_responses: &mut Vec<Option<tokio::sync::oneshot::Sender<Result<PublishAck>>>>,
    ) {
        if message_batch.is_empty() {
            return;
        }

        debug!(batch_size = message_batch.len(), "Flushing message batch");

        // For messages, we need to convert Bytes to a serializable type
        // For now, we'll fall back to individual publishing for mixed message types
        // TODO: Optimize this further if needed
        let responses: Vec<Result<PublishAck>> = {
            let mut results = Vec::with_capacity(message_batch.len());
            for (subject, payload) in message_batch.iter() {
                match publisher.jetstream.publish(subject, payload.clone()).await {
                    Ok(ack) => results.push(Ok(ack)),
                    Err(e) => results.push(Err(e)),
                }
            }
            results
        };

        // Send responses back
        for (i, result) in responses.into_iter().enumerate() {
            if i < pending_responses.len() {
                if let Some(response_tx) = pending_responses[i].take() {
                    let _ = response_tx.send(result);
                }
            }
        }

        pending_responses.drain(0..message_batch.len());
        message_batch.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::{client::NatsClient, config::NatsConfig};
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    #[ignore] // Requires NATS server
    async fn test_publisher_creation() -> color_eyre::eyre::Result<()> {
        let config = NatsConfig::test();
        let client = NatsClient::new(config.clone()).await.unwrap();
        let jetstream = JetStream::new(&client, config.jetstream).await.unwrap();

        let publisher = NatsPublisher::new(jetstream);
        assert_eq!(publisher.buffer_size().await, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_metric_publishing() -> color_eyre::eyre::Result<()> {
        // This test doesn't require a real NATS server
        let metric = serde_json::json!({
            "component": "test",
            "type": "counter",
            "value": 42.0,
            "labels": {},
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        // Just verify serialization works
        let serialized = serde_json::to_vec(&metric).unwrap();
        assert!(!serialized.is_empty());
        Ok(())
    }
}
