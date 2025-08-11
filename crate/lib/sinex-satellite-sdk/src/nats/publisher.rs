//! NATS JetStream publisher for Sinex events

use super::{
    error::{NatsError, Result},
    jetstream::JetStream,
    streams::StreamManager,
};
use async_nats::{jetstream::publish::PublishAck, HeaderMap};
use bytes::Bytes;
use serde::Serialize;
use sinex_core::db::models::{Provenance, RawEvent};
use sinex_core::types::domain::ServiceName;
use sinex_core::types::ulid::Ulid;
use std::sync::Arc;
use tokio::sync::Mutex;
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

    /// Create a new NATS publisher with custom buffer size
    pub fn with_buffer_size(jetstream: JetStream, max_buffer_size: usize) -> Self {
        Self {
            jetstream,
            buffer: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size,
        }
    }

    /// Publish a raw event
    pub async fn publish_event(&self, event: &RawEvent) -> Result<PublishAck> {
        let subject = StreamManager::event_subject(&event.source, &event.event_type);

        // Create headers
        let mut headers = HeaderMap::new();
        if let Some(id) = &event.id {
            headers.insert("Sinex-Event-Id", id.to_string());
        }
        headers.insert("Sinex-Source", event.source.as_str().to_string());
        headers.insert("Sinex-Event-Type", event.event_type.as_str().to_string());
        headers.insert("Sinex-Host", event.host.as_str().to_string());
        headers.insert(
            "Sinex-Timestamp",
            event.ts_orig.unwrap_or_else(chrono::Utc::now).to_rfc3339(),
        );

        // Add provenance information if present
        if let Some(provenance) = &event.provenance {
            match provenance {
                Provenance::Events(ids) => {
                    if !ids.is_empty() {
                        let ids_str = ids
                            .iter()
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        headers.insert("Sinex-Source-Event-Ids", ids_str);
                    }
                }
                Provenance::Material { id, .. } => {
                    headers.insert("Sinex-Source-Material-Id", id.to_string());
                }
            }
        }

        // Serialize event to JSON
        let payload = serde_json::to_vec(event).map_err(|e| NatsError::Serialization(e))?;

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
        let payload = serde_json::to_vec(message).map_err(|e| NatsError::Serialization(e))?;

        self.jetstream.publish(subject, payload).await
    }

    /// Publish a metric event
    pub async fn publish_metric(
        &self,
        component: &str,
        metric_type: &str,
        value: f64,
        labels: Option<serde_json::Value>,
    ) -> Result<PublishAck> {
        let service_name = ServiceName::from(component);
        let subject = StreamManager::metrics_subject(&service_name, metric_type);

        let metric = serde_json::json!({
            "component": component,
            "type": metric_type,
            "value": value,
            "labels": labels.unwrap_or(serde_json::json!({})),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        self.publish(&subject, &metric).await
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

    /// Flush buffered messages
    pub async fn flush_buffer(&self) -> Result<Vec<Ulid>> {
        let mut buffer = self.buffer.lock().await;
        let messages: Vec<PendingMessage> = buffer.drain(..).collect();
        drop(buffer);

        let mut failed_ids = Vec::new();

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
                        "Buffered message published successfully"
                    );
                }
                Err(e) => {
                    error!(
                        subject = msg.subject,
                        event_id = %msg.event_id,
                        error = %e,
                        "Failed to publish buffered message"
                    );
                    failed_ids.push(msg.event_id);
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
