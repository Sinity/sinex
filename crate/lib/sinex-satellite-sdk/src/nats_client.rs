//! NATS JetStream client for message bus communication
//!
//! This module provides the NATS JetStream implementation for event distribution,
//! replacing the Redis Streams implementation as per ADR-009.

use async_trait::async_trait;
use color_eyre::eyre::{Context, Result};
// use bytes::Bytes; // Not needed, use Vec<u8> instead
use crate::nats::NatsClient;
use std::sync::Arc;
use tracing::{debug, info, instrument};

/// NATS-based message bus client for event distribution
#[derive(Clone)]
pub struct NatsMessageBus {
    client: Arc<NatsClient>,
    stream_name: String,
}

impl NatsMessageBus {
    /// Create a new NATS message bus client
    #[instrument(skip(client))]
    pub async fn new(client: NatsClient, stream_name: String) -> Result<Self> {
        info!("Creating NATS message bus for stream: {}", stream_name);

        Ok(Self {
            client: Arc::new(client),
            stream_name,
        })
    }

    /// Publish an event to the stream
    #[instrument(skip(self, event_data))]
    pub async fn publish_event(&self, event_type: &str, event_data: &[u8]) -> Result<()> {
        let subject = format!("{}.{}", self.stream_name.to_lowercase(), event_type);

        self.client
            .publish(&subject, event_data.to_vec())
            .await
            .wrap_err("Failed to publish event to NATS")?;

        debug!("Published event to subject: {}", subject);
        Ok(())
    }

    /// Subscribe to events
    #[instrument(skip(self))]
    pub async fn subscribe(&self, filter_subject: Option<&str>) -> Result<()> {
        let subject = match filter_subject {
            Some(s) => s.to_string(),
            None => format!("{}.>", self.stream_name.to_lowercase()),
        };

        let _subscriber = self
            .client
            .subscribe(&subject)
            .await
            .wrap_err("Failed to subscribe to NATS")?;

        // TODO: Return the subscriber or handle messages here
        Ok(())
    }

    /// Check if connected to NATS
    pub async fn is_connected(&self) -> bool {
        self.client.is_connected().await
    }
}

/// Event message from NATS
#[derive(Debug, Clone)]
pub struct EventMessage {
    pub subject: String,
    pub data: Vec<u8>,
    pub message_id: Option<String>,
}

/// Trait for backwards compatibility with Redis-based code
#[async_trait]
pub trait MessageBusClient: Send + Sync {
    /// Publish an event
    async fn publish(&self, event_type: &str, data: &[u8]) -> Result<()>;

    /// Check connection status
    async fn is_connected(&self) -> bool;
}

#[async_trait]
impl MessageBusClient for NatsMessageBus {
    async fn publish(&self, event_type: &str, data: &[u8]) -> Result<()> {
        self.publish_event(event_type, data).await
    }

    async fn is_connected(&self) -> bool {
        self.is_connected().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::config::NatsConfig;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    #[ignore] // Requires NATS server
    async fn test_nats_message_bus() -> color_eyre::eyre::Result<()> {
        let config = NatsConfig::test();
        let client = NatsClient::new(config).await.unwrap();
        let bus = NatsMessageBus::new(client, "TEST_EVENTS".to_string())
            .await
            .unwrap();

        assert!(bus.is_connected().await);

        // Test publishing
        bus.publish_event("test.event", b"test data").await.unwrap();
        Ok(())
    }
}
