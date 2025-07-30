//! NATS JetStream integration

use crate::{
    client::NatsClient,
    config::JetStreamConfig,
    error::{NatsError, Result},
};
use async_nats::jetstream::{
    self,
    consumer::{pull::Config as PullConfig, PullConsumer},
    Context,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// JetStream context wrapper
#[derive(Clone)]
pub struct JetStream {
    context: Arc<RwLock<Context>>,
    config: Arc<JetStreamConfig>,
}

impl JetStream {
    /// Create a new JetStream context
    pub async fn new(client: &NatsClient, config: JetStreamConfig) -> Result<Self> {
        if !config.enabled {
            return Err(NatsError::JetStream("JetStream is disabled".to_string()));
        }

        let nats_client = client.client().await;

        let context = if let Some(domain) = &config.domain {
            jetstream::with_domain(nats_client.clone(), domain)
        } else if config.api_prefix != "$JS.API" {
            jetstream::with_prefix(nats_client.clone(), &config.api_prefix)
        } else {
            jetstream::new(nats_client.clone())
        };

        info!("Created JetStream context");

        Ok(Self {
            context: Arc::new(RwLock::new(context)),
            config: Arc::new(config),
        })
    }

    /// Get the JetStream context
    pub async fn context(&self) -> tokio::sync::RwLockReadGuard<Context> {
        self.context.read().await
    }

    /// Get account info
    pub async fn account_info(&self) -> Result<()> {
        // Account info not available in async-nats 0.37
        Ok(())
    }

    /// Create or get a stream
    pub async fn get_or_create_stream(
        &self,
        config: jetstream::stream::Config,
    ) -> Result<jetstream::stream::Stream> {
        let context = self.context().await;

        // Try to get existing stream first
        match context.get_stream(&config.name).await {
            Ok(stream) => {
                debug!("Found existing stream: {}", config.name);
                Ok(stream)
            }
            Err(_) => {
                // Create new stream
                let stream = context
                    .create_stream(config.clone())
                    .await
                    .map_err(|e| NatsError::JetStream(format!("Failed to create stream: {}", e)))?;

                info!("Created new stream: {}", config.name);
                Ok(stream)
            }
        }
    }

    /// Get a stream by name
    pub async fn get_stream(&self, name: &str) -> Result<jetstream::stream::Stream> {
        let context = self.context().await;
        context
            .get_stream(name)
            .await
            .map_err(|e| NatsError::JetStream(format!("Stream not found: {}", e)))
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        let context = self.context().await;
        context
            .delete_stream(name)
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to delete stream: {}", e)))?;

        info!("Deleted stream: {}", name);
        Ok(())
    }

    /// List all streams
    pub async fn list_streams(&self) -> Result<Vec<String>> {
        let context = self.context().await;
        let mut names = Vec::new();

        let mut streams = context.streams();
        while let Some(stream) = streams
            .try_next()
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to list streams: {}", e)))?
        {
            names.push(stream.config.name);
        }

        Ok(names)
    }

    /// Get stream info
    pub async fn stream_info(&self, name: &str) -> Result<jetstream::stream::Info> {
        let mut stream = self.get_stream(name).await?;
        let info = stream
            .info()
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to get stream info: {}", e)))?;
        Ok(info.clone())
    }

    /// Create or get a consumer
    pub async fn get_or_create_consumer(
        &self,
        stream_name: &str,
        config: jetstream::consumer::pull::Config,
    ) -> Result<PullConsumer> {
        let stream = self.get_stream(stream_name).await?;

        // Try to get existing consumer first
        match stream
            .get_consumer(
                &config
                    .name
                    .as_ref()
                    .unwrap_or(&config.durable_name.as_ref().unwrap_or(&"".to_string())),
            )
            .await
        {
            Ok(consumer) => {
                debug!("Found existing consumer: {:?}", config.name);
                Ok(consumer)
            }
            Err(_) => {
                // Create new consumer
                let consumer = stream.create_consumer(config.clone()).await.map_err(|e| {
                    NatsError::JetStream(format!("Failed to create consumer: {}", e))
                })?;

                info!("Created new consumer: {:?}", config.name);
                Ok(consumer)
            }
        }
    }

    /// Get a consumer by name
    pub async fn get_consumer(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<PullConsumer> {
        let stream = self.get_stream(stream_name).await?;
        stream
            .get_consumer(consumer_name)
            .await
            .map_err(|e| NatsError::JetStream(format!("Consumer not found: {}", e)))
    }

    /// Delete a consumer
    pub async fn delete_consumer(&self, stream_name: &str, consumer_name: &str) -> Result<()> {
        let stream = self.get_stream(stream_name).await?;
        stream
            .delete_consumer(consumer_name)
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to delete consumer: {}", e)))?;

        info!(
            "Deleted consumer: {} from stream: {}",
            consumer_name, stream_name
        );
        Ok(())
    }

    /// List consumers for a stream
    pub async fn list_consumers(&self, stream_name: &str) -> Result<Vec<String>> {
        let stream = self.get_stream(stream_name).await?;
        let mut names = Vec::new();

        let mut consumers = stream.consumer_names();
        while let Some(name) = consumers
            .try_next()
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to list consumers: {}", e)))?
        {
            names.push(name);
        }

        Ok(names)
    }

    /// Get consumer info
    pub async fn consumer_info(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<jetstream::consumer::Info> {
        let mut consumer = self.get_consumer(stream_name, consumer_name).await?;
        let info = consumer
            .info()
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to get consumer info: {}", e)))?;
        Ok(info.clone())
    }

    /// Publish a message to a stream
    pub async fn publish(
        &self,
        subject: &str,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<jetstream::publish::PublishAck> {
        let context = self.context.read().await;
        let context_clone = context.clone();
        drop(context); // Explicitly drop the guard

        let ack_future = context_clone
            .publish(subject.to_string(), payload.into())
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to publish: {}", e)))?;

        ack_future
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to get publish ack: {}", e)))
    }

    /// Publish with headers
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: async_nats::HeaderMap,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<jetstream::publish::PublishAck> {
        let context = self.context.read().await;
        let context_clone = context.clone();
        drop(context); // Explicitly drop the guard

        let ack_future = context_clone
            .publish_with_headers(subject.to_string(), headers, payload.into())
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to publish with headers: {}", e)))?;

        ack_future
            .await
            .map_err(|e| NatsError::JetStream(format!("Failed to get publish ack: {}", e)))
    }
}

use futures::TryStreamExt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NatsConfig;

    #[tokio::test]
    #[ignore] // Requires NATS server with JetStream
    async fn test_jetstream_context() {
        let config = NatsConfig::test();
        let client = NatsClient::new(config.clone()).await.unwrap();
        let js = JetStream::new(&client, config.jetstream).await;
        assert!(js.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires NATS server with JetStream
    async fn test_stream_operations() {
        let config = NatsConfig::test();
        let client = NatsClient::new(config.clone()).await.unwrap();
        let js = JetStream::new(&client, config.jetstream).await.unwrap();

        // Create a test stream
        let stream_config = jetstream::stream::Config {
            name: "TEST_STREAM".to_string(),
            subjects: vec!["test.>".to_string()],
            ..Default::default()
        };

        let mut stream = js.get_or_create_stream(stream_config).await.unwrap();
        let stream_info = stream.info().await.unwrap();
        assert_eq!(stream_info.config.name, "TEST_STREAM");

        // Clean up
        js.delete_stream("TEST_STREAM").await.unwrap();
    }
}
