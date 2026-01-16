use std::path::Path;
use std::time::Duration;

use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::auth::{load_client_cert, load_root_ca, load_token};
use crate::client::retry::RetryConfig;
use crate::model::nodes::{NodeHealth, NodeInfo};
use crate::model::replay::{DlqInfo, DlqMessage, ReplayOperation, ReplayPlan};
use crate::model::search::{SearchQuery, SearchResult};
use crate::model::NodeRole;
use crate::Result;

/// Gateway RPC client
#[derive(Clone)]
pub struct GatewayClient {
    client: reqwest::Client,
    base_url: String,
    token: String,
    retry_config: RetryConfig,
}

/// Client configuration
pub struct ClientConfig {
    /// Gateway URL (e.g., https://127.0.0.1:9999)
    pub url: String,
    /// Authentication token (optional, will try env/file)
    pub token: Option<String>,
    /// Token file path (optional)
    pub token_file: Option<String>,
    /// Root CA certificate path (for custom CA)
    pub ca_cert: Option<String>,
    /// Client certificate path (for mTLS)
    pub client_cert: Option<String>,
    /// Client private key path (for mTLS)
    pub client_key: Option<String>,
    /// Accept invalid certificates (dev only!)
    pub insecure: bool,
    /// Request timeout in seconds
    pub timeout: u64,
    /// Retry configuration for transient failures
    pub retry_config: RetryConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("SINEX_RPC_URL")
                .unwrap_or_else(|_| "https://127.0.0.1:9999".to_string()),
            token: None,
            token_file: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            insecure: false,
            timeout: 30,
            retry_config: RetryConfig::default(),
        }
    }
}

impl GatewayClient {
    /// Create a new gateway client
    pub fn new(config: ClientConfig) -> Result<Self> {
        // Load authentication token
        let token_file_path = config.token_file.as_ref().map(Path::new);
        let token = load_token(config.token.as_deref(), token_file_path)?;

        // Build HTTP client
        let mut client_builder = ClientBuilder::new()
            .user_agent("sinexctl/1.0")
            .timeout(Duration::from_secs(config.timeout));

        // Configure TLS
        if let Some(ca_path) = &config.ca_cert {
            let root_store = load_root_ca(Path::new(ca_path))?;
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            client_builder = client_builder.use_preconfigured_tls(tls_config);
        }

        // Configure mTLS client certificate
        if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
            let (certs, key) = load_client_cert(Path::new(cert_path), Path::new(key_path))?;
            let mut root_store = rustls::RootCertStore::empty();

            // Add system roots if no custom CA specified
            if config.ca_cert.is_none() {
                let native_certs = rustls_native_certs::load_native_certs();
                for cert in native_certs.certs {
                    root_store.add(cert).ok();
                }
            }

            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)?;
            client_builder = client_builder.use_preconfigured_tls(tls_config);
        }

        // Dev mode: accept invalid certs
        if config.insecure {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        let client = client_builder.build()?;

        Ok(Self {
            client,
            base_url: config.url,
            token,
            retry_config: config.retry_config,
        })
    }

    /// Call a JSON-RPC method with retry logic
    async fn call_rpc(&self, method: &str, params: Value) -> Result<Value> {
        let mut attempt = 0;

        loop {
            attempt += 1;

            match self.call_rpc_once(method, params.clone()).await {
                Ok(result) => return Ok(result),
                Err(e) if Self::is_retryable_error(&e) && attempt < self.retry_config.max_attempts => {
                    let backoff = self.retry_config.backoff_for_attempt(attempt);
                    tracing::debug!(
                        "RPC call to {} failed (attempt {}/{}), retrying after {:?}: {}",
                        method,
                        attempt,
                        self.retry_config.max_attempts,
                        backoff,
                        e
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Perform a single RPC call attempt (without retry)
    async fn call_rpc_once(&self, method: &str, params: Value) -> Result<Value> {
        #[derive(Serialize)]
        struct JsonRpcRequest<'a> {
            jsonrpc: &'a str,
            method: &'a str,
            params: Value,
            id: u64,
        }

        #[derive(Deserialize)]
        struct JsonRpcResponse {
            #[allow(dead_code)]
            jsonrpc: String,
            result: Option<Value>,
            error: Option<JsonRpcError>,
            #[allow(dead_code)]
            id: u64,
        }

        #[derive(Deserialize)]
        struct JsonRpcError {
            code: i32,
            message: String,
            data: Option<Value>,
        }

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id: 1,
        };

        let response = self
            .client
            .post(&self.base_url)
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&request)
            .send()
            .await?;

        // Check HTTP status
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            return Err(color_eyre::eyre::eyre!(
                "HTTP {} from gateway: {}",
                status,
                body
            ));
        }

        let rpc_response: JsonRpcResponse = response.json().await?;

        // Check for JSON-RPC error
        if let Some(error) = rpc_response.error {
            return Err(color_eyre::eyre::eyre!(
                "RPC error {}: {}{}",
                error.code,
                error.message,
                error
                    .data
                    .map(|d| format!("\nDetails: {}", d))
                    .unwrap_or_default()
            ));
        }

        rpc_response
            .result
            .ok_or_else(|| color_eyre::eyre::eyre!("RPC response missing result field"))
    }

    /// Determine if an error is retryable (transient network/server issues)
    fn is_retryable_error(err: &color_eyre::Report) -> bool {
        let err_str = err.to_string().to_lowercase();

        // Retry connection errors
        if err_str.contains("connection refused")
            || err_str.contains("connection reset")
            || err_str.contains("broken pipe")
            || err_str.contains("network unreachable")
            || err_str.contains("host unreachable")
            || err_str.contains("timeout")
            || err_str.contains("timed out")
        {
            return true;
        }

        // Retry 5xx server errors (but not 4xx client errors)
        if err_str.contains("http 5") {
            return true;
        }

        // Retry rate limit errors
        if err_str.contains("http 429") || err_str.contains("rate limit") {
            return true;
        }

        // Don't retry authentication errors, not found, bad request, etc.
        false
    }

    // ==================== Gateway Commands ====================

    /// Ping the gateway
    pub async fn ping(&self) -> Result<String> {
        let result = self.call_rpc("ping", json!({})).await?;
        Ok(result
            .as_str()
            .unwrap_or("pong")
            .to_string())
    }

    /// Get gateway version
    pub async fn version(&self) -> Result<String> {
        let result = self.call_rpc("version", json!({})).await?;
        Ok(result
            .as_str()
            .unwrap_or("unknown")
            .to_string())
    }

    // ==================== Core Commands ====================

    /// Get system health status
    pub async fn health(&self) -> Result<NodeHealth> {
        let result = self
            .call_rpc("coordination.instance_health", json!({}))
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Node Commands ====================

    /// List all nodes
    pub async fn list_nodes(&self, role: Option<NodeRole>) -> Result<Vec<NodeInfo>> {
        let params = if let Some(r) = role {
            json!({ "role": r })
        } else {
            json!({})
        };

        let result = self.call_rpc("coordination.list_instances", params).await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Get node status
    pub async fn node_status(&self, node_id: &str) -> Result<NodeInfo> {
        let result = self
            .call_rpc("coordination.instance_health", json!({ "id": node_id }))
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Drain a node for maintenance
    pub async fn drain_node(&self, node_id: &str) -> Result<()> {
        self.call_rpc("nodes.drain", json!({ "id": node_id }))
            .await?;
        Ok(())
    }

    /// Resume a drained node
    pub async fn resume_node(&self, node_id: &str) -> Result<()> {
        self.call_rpc("nodes.resume", json!({ "id": node_id }))
            .await?;
        Ok(())
    }

    /// Set node horizon (cutoff time for event processing)
    pub async fn set_node_horizon(&self, node_id: &str, horizon: &str) -> Result<()> {
        self.call_rpc(
            "nodes.set_horizon",
            json!({ "node_id": node_id, "horizon": horizon }),
        )
        .await?;
        Ok(())
    }

    // ==================== Replay Commands ====================

    /// Create a replay plan
    pub async fn replay_plan(&self, query: &str) -> Result<ReplayPlan> {
        let result = self
            .call_rpc("replay.create_operation", json!({ "query": query }))
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Submit a replay plan for execution
    pub async fn replay_submit(&self, plan_id: &str) -> Result<ReplayOperation> {
        // First approve
        self.call_rpc("replay.approve_operation", json!({ "operation_id": plan_id }))
            .await?;

        // Then execute
        let result = self
            .call_rpc("replay.execute_operation", json!({ "operation_id": plan_id }))
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Get replay operation status
    pub async fn replay_status(&self, operation_id: &str) -> Result<ReplayOperation> {
        let result = self
            .call_rpc(
                "replay.operation_status",
                json!({ "operation_id": operation_id }),
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// List all replay operations
    pub async fn replay_list(&self) -> Result<Vec<ReplayOperation>> {
        let result = self.call_rpc("replay.list_operations", json!({})).await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== DLQ Commands ====================

    /// List dead letter queues
    pub async fn dlq_list(&self) -> Result<Vec<DlqInfo>> {
        let result = self.call_rpc("dlq.list", json!({})).await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Peek at messages in a DLQ
    pub async fn dlq_peek(&self, subject: &str, limit: Option<u32>) -> Result<Vec<DlqMessage>> {
        let params = json!({
            "subject": subject,
            "limit": limit.unwrap_or(10)
        });
        let result = self.call_rpc("dlq.peek", params).await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Requeue messages from DLQ
    pub async fn dlq_requeue(&self, event_id: Option<String>, all: bool) -> Result<()> {
        let params = json!({
            "event_id": event_id,
            "all": all
        });
        self.call_rpc("dlq.requeue", params).await?;
        Ok(())
    }

    /// Purge all messages from DLQ
    pub async fn dlq_purge(&self, confirm: bool) -> Result<()> {
        let params = json!({ "confirm": confirm });
        self.call_rpc("dlq.purge", params).await?;
        Ok(())
    }

    // ==================== Search Commands ====================

    /// Search events
    pub async fn search_events(&self, query: SearchQuery) -> Result<Vec<SearchResult>> {
        let result = self
            .call_rpc("search.search_events", serde_json::to_value(&query)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Operations Log Commands ====================

    /// Start a new operation
    pub async fn ops_start(
        &self,
        operation_type: &str,
        operator: &str,
        scope: Option<Value>,
    ) -> Result<String> {
        let params = json!({
            "operation_type": operation_type,
            "operator": operator,
            "scope": scope
        });
        let result = self.call_rpc("ops.start", params).await?;
        Ok(result
            .get("operation_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string())
    }

    /// List operations
    pub async fn ops_list(
        &self,
        operation_type: Option<String>,
        status: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<Value>> {
        let params = json!({
            "operation_type": operation_type,
            "status": status,
            "limit": limit.unwrap_or(50)
        });
        let result = self.call_rpc("ops.list", params).await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Get operation details
    pub async fn ops_get(&self, operation_id: &str) -> Result<Value> {
        let params = json!({ "operation_id": operation_id });
        self.call_rpc("ops.get", params).await
    }

    /// Cancel an operation
    pub async fn ops_cancel(&self, operation_id: &str, reason: Option<String>) -> Result<()> {
        let params = json!({
            "operation_id": operation_id,
            "reason": reason
        });
        self.call_rpc("ops.cancel", params).await?;
        Ok(())
    }

    // ==================== Audit Commands ====================

    /// Get audit trail for an operation
    pub async fn audit_get(&self, operation_id: &str) -> Result<Value> {
        let params = json!({ "operation_id": operation_id });
        self.call_rpc("audit.get", params).await
    }
}
