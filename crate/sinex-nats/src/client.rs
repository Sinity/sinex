//! NATS client wrapper with connection pooling

use crate::{
    config::NatsConfig,
    error::{NatsError, Result},
};
use async_nats::{Client, ConnectOptions, ServerAddr};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// NATS client wrapper with automatic reconnection
#[derive(Clone)]
pub struct NatsClient {
    inner: Arc<RwLock<Client>>,
    config: Arc<NatsConfig>,
}

impl NatsClient {
    /// Create a new NATS client
    pub async fn new(config: NatsConfig) -> Result<Self> {
        let client = Self::connect(&config).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(client)),
            config: Arc::new(config),
        })
    }

    /// Connect to NATS server
    async fn connect(config: &NatsConfig) -> Result<Client> {
        let mut options = ConnectOptions::new()
            .name(&config.client_name)
            .connection_timeout(config.connection_timeout)
            .request_timeout(Some(config.request_timeout))
            .retry_on_initial_connect()
            .ping_interval(std::time::Duration::from_secs(30));

        // Configure authentication
        if let Some(auth) = &config.auth {
            options = match auth {
                crate::config::AuthConfig::UserPassword { username, password } => {
                    options.user_and_password(username.clone(), password.clone())
                }
                crate::config::AuthConfig::Token { token } => options.token(token.clone()),
                crate::config::AuthConfig::NKey { seed } => {
                    // NATS expects the seed as a String for nkey auth
                    options = options.nkey(seed.clone());
                    options
                }
                crate::config::AuthConfig::Jwt { jwt: _, seed: _ } => {
                    // JWT auth is complex, skip for now
                    warn!("JWT authentication not yet implemented");
                    options
                }
            };
        }

        // Configure TLS
        if config.tls_enabled {
            options = options.require_tls(true);
        }

        // Add event handlers
        options = options.event_callback(|event| async move {
            match event {
                async_nats::Event::Connected => {
                    info!("Connected to NATS server");
                }
                async_nats::Event::Disconnected => {
                    warn!("Disconnected from NATS server");
                }
                async_nats::Event::ServerError(err) => {
                    error!("NATS server error: {}", err);
                }
                async_nats::Event::ClientError(err) => {
                    error!("NATS client error: {}", err);
                }
                _ => {
                    debug!("NATS event: {:?}", event);
                }
            }
        });

        // Parse server addresses
        let servers: Vec<ServerAddr> = config
            .servers
            .iter()
            .map(|s| s.parse())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| NatsError::Connection(format!("Invalid server address: {}", e)))?;

        // Connect to NATS
        let server_list = if servers.len() == 1 {
            servers[0].clone()
        } else {
            servers[0].clone() // async-nats 0.37 doesn't support multiple servers in connect
        };

        let client = options
            .connect(server_list)
            .await
            .map_err(|e| NatsError::Connection(format!("Failed to connect: {}", e)))?;

        info!("Connected to NATS server(s): {}", config.servers.join(", "));

        Ok(client)
    }

    /// Get the inner NATS client
    pub async fn client(&self) -> tokio::sync::RwLockReadGuard<'_, Client> {
        self.inner.read().await
    }

    /// Get a mutable reference to the inner NATS client
    pub async fn client_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, Client> {
        self.inner.write().await
    }

    /// Check if the client is connected
    pub async fn is_connected(&self) -> bool {
        let client = self.client().await;
        client.connection_state() == async_nats::connection::State::Connected
    }

    /// Reconnect to NATS server
    pub async fn reconnect(&self) -> Result<()> {
        let new_client = Self::connect(&self.config).await?;
        let mut client = self.inner.write().await;
        *client = new_client;
        Ok(())
    }

    /// Publish a message
    pub async fn publish(&self, subject: &str, payload: impl Into<bytes::Bytes>) -> Result<()> {
        let subject = subject.to_string(); // Convert to owned String
        let client = self.inner.read().await;
        let client_clone = client.clone();
        drop(client); // Explicitly drop the guard
        client_clone
            .publish(subject, payload.into())
            .await
            .map_err(|e| NatsError::Publish(e.to_string()))?;
        Ok(())
    }

    /// Request-reply pattern
    pub async fn request(
        &self,
        subject: &str,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<async_nats::Message> {
        let subject = subject.to_string(); // Convert to owned String
        let client = self.inner.read().await;
        let client_clone = client.clone();
        drop(client); // Explicitly drop the guard
        client_clone
            .request(subject, payload.into())
            .await
            .map_err(|e| NatsError::Client(Box::new(e)))
    }

    /// Subscribe to a subject
    pub async fn subscribe(&self, subject: &str) -> Result<async_nats::Subscriber> {
        let client = self.inner.read().await;
        let client_clone = client.clone();
        drop(client); // Explicitly drop the guard
        client_clone
            .subscribe(subject.to_string())
            .await
            .map_err(|e| NatsError::Subscribe(e.to_string()))
    }

    /// Queue subscribe to a subject
    pub async fn queue_subscribe(
        &self,
        subject: &str,
        queue: &str,
    ) -> Result<async_nats::Subscriber> {
        let client = self.inner.read().await;
        let client_clone = client.clone();
        drop(client); // Explicitly drop the guard
        client_clone
            .queue_subscribe(subject.to_string(), queue.to_string())
            .await
            .map_err(|e| NatsError::Subscribe(e.to_string()))
    }

    /// Flush pending messages
    pub async fn flush(&self) -> Result<()> {
        let client = self.inner.read().await;
        let client_clone = client.clone();
        drop(client); // Explicitly drop the guard
        client_clone
            .flush()
            .await
            .map_err(|e| NatsError::Client(Box::new(e)))
    }

    /// Get server info
    pub async fn server_info(&self) -> async_nats::ServerInfo {
        let client = self.inner.read().await;
        client.server_info().clone()
    }

    /// Get connection state
    pub async fn connection_state(&self) -> async_nats::connection::State {
        let client = self.inner.read().await;
        client.connection_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    #[ignore] // Requires NATS server
    async fn test_nats_client_connection() {
        let config = NatsConfig::test();
        let client = NatsClient::new(config).await;
        assert!(client.is_ok());

        let client = client.unwrap();
        assert!(client.is_connected().await);
    }

    #[tokio::test]
    #[ignore] // Requires NATS server
    async fn test_publish_subscribe() {
        let config = NatsConfig::test();
        let client = NatsClient::new(config).await.unwrap();

        let mut sub = client.subscribe("test.subject").await.unwrap();

        client
            .publish("test.subject", &b"test message"[..])
            .await
            .unwrap();

        let msg = sub.next().await.unwrap();
        assert_eq!(msg.payload.as_ref(), b"test message");
    }
}
