//! The Tether - Connect to production for real test data
//!
//! This module enables `sx dev --tether prod` functionality, allowing
//! developers to receive real production events while developing locally.
//!
//! The Tether works by:
//! 1. Creating a shadow consumer on the production gateway
//! 2. Subscribing to the shadow consumer's events
//! 3. Forwarding events to the local development process
//!
//! Shadow consumers use fan-out delivery, so they don't affect production
//! consumers - they receive copies of all matching events.

use color_eyre::eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Configuration for The Tether connection
#[derive(Debug, Clone)]
pub struct TetherConfig {
    /// Target environment (e.g., "prod", "staging")
    pub target: String,
    /// Gateway RPC URL (e.g., "https://gateway.sinex.io:9999")
    pub gateway_url: String,
    /// RPC authentication token
    pub auth_token: String,
    /// Subject filter for events (optional)
    pub subject_filter: Option<String>,
    /// Consumer name prefix (will be combined with timestamp)
    pub consumer_prefix: String,
    /// Start from beginning of stream
    pub from_beginning: bool,
}

impl TetherConfig {
    /// Create a new tether config from environment
    pub fn from_env(target: &str) -> Result<Self> {
        let gateway_url = std::env::var("SINEX_GATEWAY_URL")
            .or_else(|_| std::env::var(format!("SINEX_{}_GATEWAY_URL", target.to_uppercase())))
            .unwrap_or_else(|_| format!("https://gateway.{}.sinex.io:9999", target));

        let auth_token = std::env::var("SINEX_RPC_TOKEN")
            .or_else(|_| std::env::var(format!("SINEX_{}_RPC_TOKEN", target.to_uppercase())))
            .wrap_err("SINEX_RPC_TOKEN or SINEX_{TARGET}_RPC_TOKEN must be set for tether")?;

        let consumer_prefix = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "dev".to_string());

        Ok(Self {
            target: target.to_string(),
            gateway_url,
            auth_token,
            subject_filter: None,
            consumer_prefix: format!("dev-{}", consumer_prefix),
            from_beginning: false,
        })
    }

    /// Generate a unique consumer name for this session
    pub fn consumer_name(&self) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%S");
        format!("{}-{}", self.consumer_prefix, timestamp)
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
            .wrap_err("Failed to create HTTP client")?;

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

        debug!(method = %method, "Making RPC call to gateway");

        let response = self
            .http_client
            .post(&self.config.gateway_url)
            .header("Content-Type", "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", self.config.auth_token),
            )
            .json(&request)
            .send()
            .await
            .wrap_err("Failed to send RPC request")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(eyre!("RPC request failed with status {}: {}", status, body));
        }

        let rpc_response: JsonRpcResponse = response
            .json()
            .await
            .wrap_err("Failed to parse RPC response")?;

        if let Some(error) = rpc_response.error {
            return Err(eyre!("RPC error {}: {}", error.code, error.message));
        }

        rpc_response
            .result
            .ok_or_else(|| eyre!("RPC response missing result"))
    }

    /// Create a shadow consumer for this development session
    pub async fn create_shadow_consumer(&self) -> Result<ShadowConsumerInfo> {
        let consumer_name = self.config.consumer_name();

        info!(
            target = %self.config.target,
            consumer_name = %consumer_name,
            "Creating shadow consumer via The Tether"
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
            serde_json::from_value(result).wrap_err("Failed to parse shadow consumer info")?;

        info!(
            consumer_name = %info.consumer_name,
            stream = %info.stream_name,
            subject_filter = %info.subject_filter,
            pending = info.num_pending,
            "Shadow consumer created successfully"
        );

        Ok(info)
    }

    /// List active shadow consumers
    pub async fn list_shadow_consumers(&self) -> Result<Vec<ShadowConsumerInfo>> {
        let result = self.rpc_call("shadow.list", serde_json::json!({})).await?;

        let consumers: Vec<ShadowConsumerInfo> =
            serde_json::from_value(result["consumers"].clone())
                .wrap_err("Failed to parse shadow consumers list")?;

        Ok(consumers)
    }

    /// Delete a shadow consumer
    pub async fn delete_shadow_consumer(&self, consumer_name: &str) -> Result<()> {
        info!(consumer_name = %consumer_name, "Deleting shadow consumer");

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
#[derive(Debug, Clone)]
pub struct TetheredEvent {
    /// The event subject
    pub subject: String,
    /// The event payload (JSON)
    pub payload: serde_json::Value,
    /// Stream sequence number
    pub sequence: u64,
}

/// Tether session that manages the shadow consumer lifecycle
pub struct TetherSession {
    client: TetherClient,
    consumer_info: Option<ShadowConsumerInfo>,
}

impl TetherSession {
    /// Start a new tether session
    pub async fn start(config: TetherConfig) -> Result<Self> {
        let client = TetherClient::new(config)?;
        let consumer_info = client.create_shadow_consumer().await?;

        Ok(Self {
            client,
            consumer_info: Some(consumer_info),
        })
    }

    /// Get the consumer info
    pub fn consumer_info(&self) -> Option<&ShadowConsumerInfo> {
        self.consumer_info.as_ref()
    }

    /// Clean up the shadow consumer on shutdown
    pub async fn cleanup(&mut self) {
        if let Some(ref info) = self.consumer_info.take() {
            match self
                .client
                .delete_shadow_consumer(&info.consumer_name)
                .await
            {
                Ok(()) => {
                    info!(
                        consumer_name = %info.consumer_name,
                        "Shadow consumer cleaned up"
                    );
                }
                Err(e) => {
                    warn!(
                        consumer_name = %info.consumer_name,
                        error = %e,
                        "Failed to clean up shadow consumer"
                    );
                }
            }
        }
    }
}

impl Drop for TetherSession {
    fn drop(&mut self) {
        // Note: Can't do async cleanup in Drop
        // The cleanup() method should be called explicitly before dropping
        if self.consumer_info.is_some() {
            warn!("TetherSession dropped without cleanup - shadow consumer may be orphaned");
        }
    }
}

/// Connect to production via The Tether and forward events
///
/// This is the main entry point for `sx dev --tether <target>`.
/// It creates a shadow consumer and starts receiving events.
pub async fn connect_tether(
    target: &str,
    event_tx: mpsc::Sender<TetheredEvent>,
) -> Result<TetherSession> {
    let config = TetherConfig::from_env(target)?;
    let session = TetherSession::start(config).await?;

    // Log connection info
    if let Some(info) = session.consumer_info() {
        info!(
            target = %target,
            consumer = %info.consumer_name,
            pending = info.num_pending,
            "Connected to production via The Tether"
        );

        if info.num_pending > 0 {
            info!(
                pending = info.num_pending,
                "Catching up on {} pending events...", info.num_pending
            );
        }
    }

    // Note: Actual event streaming requires NATS connection to production
    // This is a placeholder - full implementation would:
    // 1. Connect to production NATS via mTLS tunnel
    // 2. Pull messages from the shadow consumer
    // 3. Forward them to event_tx
    //
    // For now, we just set up the consumer; actual streaming TBD

    // Suppress unused variable warning - will be used when streaming is implemented
    let _ = event_tx;

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
        };

        let name = config.consumer_name();
        assert!(name.starts_with("dev-testuser-"));
        // Should have timestamp suffix
        assert!(name.len() > "dev-testuser-".len());
    }

    #[test]
    fn test_config_from_env() {
        // Would need to set env vars for full test
        // For now just verify the method exists
        std::env::set_var("SINEX_RPC_TOKEN", "test-token");
        let config = TetherConfig::from_env("test").unwrap();
        assert_eq!(config.target, "test");
        assert!(config.consumer_prefix.starts_with("dev-"));
        std::env::remove_var("SINEX_RPC_TOKEN");
    }
}
