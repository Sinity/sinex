//! The Tether - Connect to production for real test data
//!
//! This module enables `cargo xtask dev run --tether prod` functionality,
//! allowing developers to receive real production events while developing locally.
//!
//! The Tether works by:
//! 1. Creating a shadow consumer on the production gateway
//! 2. Subscribing to the shadow consumer's events
//! 3. Forwarding events to the local development process
//!
//! Shadow consumers use fan-out delivery, so they don't affect production
//! consumers - they receive copies of all matching events.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sinex_primitives::temporal::{format_rfc3339, Timestamp};
use std::time::Duration;
use tokio::sync::mpsc;

/// Configuration for The Tether connection
#[derive(Debug, Clone)]
pub struct TetherConfig {
    /// Target environment (e.g., "prod", "staging")
    pub target: String,
    /// Gateway RPC URL (e.g., "<https://gateway.sinex.io:9999>")
    pub gateway_url: String,
    /// RPC authentication token
    pub auth_token: String,
    /// Subject filter for events (optional)
    pub subject_filter: Option<String>,
    /// Consumer name prefix (will be combined with timestamp)
    pub consumer_prefix: String,
    /// Start from beginning of stream
    pub from_beginning: bool,
    /// NATS connection URL
    pub nats_url: String,
    /// NATS credentials (optional)
    pub nats_creds: Option<String>,
    /// NATS TLS CA certificate (optional)
    pub nats_ca: Option<String>,
    /// NATS TLS client certificate (optional)
    pub nats_cert: Option<String>,
    /// NATS TLS client key (optional)
    pub nats_key: Option<String>,
}

impl TetherConfig {
    /// Create a new tether config from environment
    pub fn from_env(target: &str) -> Result<Self> {
        let gateway_url = std::env::var("SINEX_GATEWAY_URL")
            .or_else(|_| std::env::var(format!("SINEX_{}_GATEWAY_URL", target.to_uppercase())))
            .unwrap_or_else(|_| format!("https://gateway.{target}.sinex.io:9999"));

        let auth_token = std::env::var("SINEX_RPC_TOKEN")
            .or_else(|_| std::env::var(format!("SINEX_{}_RPC_TOKEN", target.to_uppercase())))
            .context("SINEX_RPC_TOKEN or SINEX_{TARGET}_RPC_TOKEN must be set for tether")?;

        let consumer_prefix = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "dev".to_string());

        let nats_url = std::env::var("SINEX_TETHER_NATS_URL")
            .or_else(|_| std::env::var(format!("SINEX_{}_NATS_URL", target.to_uppercase())))
            .unwrap_or_else(|_| format!("nats://nats.{target}.sinex.io:4222"));

        let nats_creds = std::env::var("SINEX_TETHER_NATS_CREDS")
            .or_else(|_| std::env::var(format!("SINEX_{}_NATS_CREDS", target.to_uppercase())))
            .ok();

        let nats_ca = std::env::var("SINEX_TETHER_NATS_CA")
            .or_else(|_| std::env::var(format!("SINEX_{}_NATS_CA", target.to_uppercase())))
            .ok();

        let nats_cert = std::env::var("SINEX_TETHER_NATS_CERT")
            .or_else(|_| std::env::var(format!("SINEX_{}_NATS_CERT", target.to_uppercase())))
            .ok();

        let nats_key = std::env::var("SINEX_TETHER_NATS_KEY")
            .or_else(|_| std::env::var(format!("SINEX_{}_NATS_KEY", target.to_uppercase())))
            .ok();

        Ok(Self {
            target: target.to_string(),
            gateway_url,
            auth_token,
            subject_filter: None,
            consumer_prefix: format!("dev-{consumer_prefix}"),
            from_beginning: false,
            nats_url,
            nats_creds,
            nats_ca,
            nats_cert,
            nats_key,
        })
    }

    /// Generate a unique consumer name for this session
    #[must_use]
    pub fn consumer_name(&self) -> String {
        let timestamp = format_rfc3339(Timestamp::now())
            .replace([':', '-', '.'], "") // Compact format: YYYYMMDDTHHMMSSmmmZ
            .chars()
            .take(15) // YYYYMMDDTHHMMSS
            .collect::<String>();

        format!("{}-{timestamp}", self.consumer_prefix)
    }
}

/// JSON-RPC request structure
#[derive(Debug, Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    method: String,
    params: serde_json::Value,
    id: u64,
}

/// JSON-RPC response structure
#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    #[allow(dead_code)]
    jsonrpc: String,
    result: Option<serde_json::Value>,
    error: Option<JsonRpcError>,
    #[allow(dead_code)]
    id: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

/// Shadow consumer creation response
#[derive(Debug, Deserialize)]
pub struct ShadowConsumerInfo {
    pub consumer_name: String,
    pub stream_name: String,
    pub subject_filter: String,
    pub num_pending: u64,
    #[allow(dead_code)]
    pub first_sequence: u64,
}

/// The Tether client for connecting to production
pub struct TetherClient {
    config: TetherConfig,
    http_client: reqwest::Client,
    request_id: std::sync::atomic::AtomicU64,
}

impl TetherClient {
    /// Create a new tether client
    pub fn new(config: TetherConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(true) // For development - proper certs in production
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            config,
            http_client,
            request_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Make an RPC call to the gateway
    async fn rpc_call(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let request_id = self
            .request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method: method.to_string(),
            params,
            id: request_id,
        };

        let response = self
            .http_client
            .post(&self.config.gateway_url)
            .header("Content-Type", "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", &self.config.auth_token),
            )
            .json(&request)
            .send()
            .await
            .context("Failed to send RPC request")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("RPC request failed with status {status}: {body}");
        }

        let rpc_response: JsonRpcResponse = response
            .json()
            .await
            .context("Failed to parse RPC response")?;

        if let Some(error) = rpc_response.error {
            bail!("RPC error {}: {}", error.code, error.message);
        }

        rpc_response
            .result
            .ok_or_else(|| anyhow::anyhow!("RPC response missing result"))
    }

    /// Create a shadow consumer for this development session
    pub async fn create_shadow_consumer(&self) -> Result<ShadowConsumerInfo> {
        let consumer_name = self.config.consumer_name();

        println!(
            "[tether] Creating shadow consumer '{}' on {}...",
            consumer_name, self.config.target
        );

        let mut params = serde_json::json!({
            "consumer_name": consumer_name,
            "from_beginning": self.config.from_beginning,
        });

        if let Some(ref filter) = self.config.subject_filter {
            params["subject_filter"] = serde_json::json!(filter);
        }

        let result = self.rpc_call("shadow.create", params).await?;
        let info: ShadowConsumerInfo =
            serde_json::from_value(result).context("Failed to parse shadow consumer info")?;

        println!(
            "[tether] Connected: stream={}, filter={}, pending={}",
            info.stream_name, info.subject_filter, info.num_pending
        );

        Ok(info)
    }

    /// List active shadow consumers
    #[allow(dead_code)]
    pub async fn list_shadow_consumers(&self) -> Result<Vec<ShadowConsumerInfo>> {
        let result = self.rpc_call("shadow.list", serde_json::json!({})).await?;

        let consumers: Vec<ShadowConsumerInfo> =
            serde_json::from_value(result["consumers"].clone())
                .context("Failed to parse shadow consumers list")?;

        Ok(consumers)
    }

    /// Delete a shadow consumer
    #[allow(dead_code)]
    pub async fn delete_shadow_consumer(&self, consumer_name: &str) -> Result<()> {
        println!("[tether] Deleting shadow consumer '{consumer_name}'...");

        self.rpc_call(
            "shadow.delete",
            serde_json::json!({
                "consumer_name": consumer_name
            }),
        )
        .await?;

        Ok(())
    }
}

/// Event received via The Tether
#[derive(Debug, Clone, serde::Serialize)]
#[allow(dead_code)]
pub struct TetheredEvent {
    /// The event subject
    pub subject: String,
    /// The event payload (JSON)
    pub payload: serde_json::Value,
    /// Stream sequence number
    pub sequence: u64,
}

/// Tether session that manages the shadow consumer lifecycle
#[allow(dead_code)]
pub struct TetherSession {
    config: TetherConfig,
    client: TetherClient,
    consumer_info: Option<ShadowConsumerInfo>,
    nats_client: Option<async_nats::Client>,
}

impl TetherSession {
    /// Start a new tether session
    pub async fn start(config: TetherConfig) -> Result<Self> {
        let client = TetherClient::new(config.clone())?;
        let consumer_info = client.create_shadow_consumer().await?;

        Ok(Self {
            config,
            client,
            consumer_info: Some(consumer_info),
            nats_client: None,
        })
    }

    /// Get the consumer info
    pub fn consumer_info(&self) -> Option<&ShadowConsumerInfo> {
        self.consumer_info.as_ref()
    }

    /// Clean up the shadow consumer on shutdown
    #[allow(dead_code)]
    pub async fn cleanup(&mut self) {
        if let Some(ref info) = self.consumer_info.take() {
            match self
                .client
                .delete_shadow_consumer(&info.consumer_name)
                .await
            {
                Ok(()) => {
                    println!(
                        "[tether] Shadow consumer '{}' cleaned up",
                        info.consumer_name
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[tether] Failed to clean up shadow consumer '{}': {}",
                        info.consumer_name, e
                    );
                }
            }
        }
    }

    /// Stream events to a channel
    pub async fn stream_events(
        &mut self,
        tx: tokio::sync::mpsc::Sender<TetheredEvent>,
    ) -> Result<()> {
        let info = self.consumer_info.as_ref().context("No active consumer")?;

        // 1. Connect to NATS if not already connected
        if self.nats_client.is_none() {
            let mut options = async_nats::ConnectOptions::new();

            if let Some(ref creds) = self.config.nats_creds {
                options = options
                    .credentials_file(creds)
                    .await
                    .context("Failed to load NATS creds")?;
            }

            // Note: In a real implementation, we would set up TLS here using config.nats_ca/cert/key
            // For now, we'll assume basic connection or pre-configured environment

            let nats = async_nats::connect_with_options(&self.config.nats_url, options)
                .await
                .context("Failed to connect to NATS")?;
            self.nats_client = Some(nats);
        }

        let nats = self.nats_client.as_ref().unwrap();
        let jetstream = async_nats::jetstream::new(nats.clone());

        // 2. Get the stream and consumer
        let stream = jetstream
            .get_stream(&info.stream_name)
            .await
            .context("Failed to get stream")?;
        let consumer: async_nats::jetstream::consumer::PullConsumer = stream
            .get_consumer(&info.consumer_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get consumer: {e}"))?;

        // 3. Start pull loop
        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get messages: {e}"))?;

        while let Some(msg) = tokio::select! {
            next = futures::StreamExt::next(&mut messages) => next,
        } {
            let msg = msg.map_err(|e| anyhow::anyhow!("Error in message stream: {e}"))?;

            let payload: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap_or_else(
                |_| serde_json::json!({"raw_data": String::from_utf8_lossy(&msg.payload)}),
            );

            let event = TetheredEvent {
                subject: msg.subject.to_string(),
                payload,
                sequence: msg
                    .info()
                    .map_err(|e| anyhow::anyhow!("No message info: {e}"))?
                    .stream_sequence,
            };

            if tx.send(event).await.is_err() {
                break; // Channel closed
            }

            // Acknowledge the message
            msg.ack().await.ok();
        }

        Ok(())
    }

    /// Get session statistics (stub)
    #[allow(dead_code)]
    pub fn stats(&self) -> TetherStats {
        TetherStats::default()
    }
}

/// Statistics for a tether session
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct TetherStats {
    events_received: u64,
    events_forwarded: u64,
    errors: u64,
}

impl TetherStats {
    /// Number of events received
    #[must_use]
    pub fn events_received(&self) -> u64 {
        self.events_received
    }

    /// Number of events forwarded
    #[must_use]
    pub fn events_forwarded(&self) -> u64 {
        self.events_forwarded
    }

    /// Number of errors
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors
    }
}

impl Drop for TetherSession {
    fn drop(&mut self) {
        // Note: Can't do async cleanup in Drop
        // The cleanup() method should be called explicitly before dropping
        if self.consumer_info.is_some() {
            eprintln!("[tether] Warning: TetherSession dropped without cleanup - shadow consumer may be orphaned");
        }
    }
}

/// Connect to production via The Tether and forward events
///
/// This is the main entry point for `cargo xtask dev run --tether <target>`.
/// It creates a shadow consumer and starts receiving events.
#[allow(dead_code)]
pub async fn connect_tether(
    target: &str,
    _event_tx: mpsc::Sender<TetheredEvent>,
) -> Result<TetherSession> {
    let config = TetherConfig::from_env(target)?;
    let session = TetherSession::start(config).await?;

    // Log connection info
    if let Some(info) = session.consumer_info() {
        println!(
            "[tether] Connected to {} via shadow consumer '{}'",
            target, info.consumer_name
        );

        if info.num_pending > 0 {
            println!(
                "[tether] Catching up on {} pending events...",
                info.num_pending
            );
        }
    }

    // Note: Actual event streaming requires NATS connection to production
    // This is a placeholder - full implementation would:
    // 1. Connect to production NATS via mTLS tunnel
    // 2. Pull messages from the shadow consumer
    // 3. Forward them to event_tx

    Ok(session)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_name_format() {
        let config = TetherConfig {
            target: "prod".to_string(),
            gateway_url: "https://localhost:9999".to_string(),
            auth_token: "test-token".to_string(),
            subject_filter: None,
            consumer_prefix: "dev-testuser".to_string(),
            from_beginning: false,
            nats_url: "nats://localhost:4222".to_string(),
            nats_creds: None,
            nats_ca: None,
            nats_cert: None,
            nats_key: None,
        };

        let name = config.consumer_name();
        assert!(name.starts_with("dev-testuser-"));
        // Should have timestamp suffix
        // Compact format is 15 chars (YYYYMMDDTHHMMSS)
        let suffix = name.trim_start_matches("dev-testuser-");
        assert_eq!(suffix.len(), 15);
        assert!(suffix.chars().all(|c| c.is_ascii_digit() || c == 'T'));
    }
}
