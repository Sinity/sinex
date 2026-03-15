use std::path::Path;
use std::time::Duration;

use reqwest::{ClientBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_primitives::domain::EventSource;
use sinex_primitives::rpc::{
    JsonRpcError,
    coordination::{
        InstanceHealthRequest, InstanceHealthResponse, InstanceInfo, ListInstancesRequest,
        ListInstancesResponse,
    },
    dlq::{
        DlqListRequest, DlqListResponse, DlqPeekRequest, DlqPeekResponse, DlqPurgeRequest,
        DlqPurgeResponse, DlqRequeueRequest, DlqRequeueResponse,
    },
    gitops::{
        GitOpsCreateSourceRequest, GitOpsCreateSourceResponse, GitOpsDeleteSourceRequest,
        GitOpsDeleteSourceResponse, GitOpsListSourcesRequest, GitOpsListSourcesResponse,
        GitOpsSourceInfo, GitOpsTriggerSyncRequest, GitOpsTriggerSyncResponse,
    },
    lifecycle::{
        LifecycleArchiveRequest, LifecycleArchiveResponse, LifecycleRestoreRequest,
        LifecycleRestoreResponse, LifecycleStatusRequest, LifecycleStatusResponse,
        TombstoneApproveRequest, TombstoneApproveResponse, TombstoneCancelRequest,
        TombstoneCancelResponse, TombstoneCreateRequest, TombstoneCreateResponse,
        TombstoneListRequest, TombstoneListResponse, TombstoneOperationState,
        TombstonePreviewRequest, TombstonePreviewResponse, TombstoneStatusRequest,
        TombstoneStatusResponse,
    },
    methods,
    nodes::{NodeDrainRequest, NodeResumeRequest, NodeSetHorizonRequest},
    replay::{
        ReplayApproveRequest, ReplayApproveResponse, ReplayCancelRequest,
        ReplayCancelResponse, ReplayCreateRequest, ReplayCreateResponse,
        ReplayExecuteRequest, ReplayExecuteResponse, ReplayListRequest,
        ReplayListResponse, ReplayOperation, ReplayPreviewRequest,
        ReplayPreviewResponse, ReplayScope, ReplayState, ReplayStatusRequest,
        ReplayStatusResponse,
    },
    system::{SystemHealthRequest, SystemHealthResponse},
};
use sinex_primitives::temporal::Timestamp;

use crate::Result;
use crate::auth::{load_client_cert, load_root_ca, load_token};
use crate::client::RetryConfig;
use crate::model::NodeRole;
use sinex_primitives::query::{
    EventQuery, EventQueryResult, LineageQuery, LineageResult, SubscriptionFilter,
};

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
    /// Gateway URL (e.g., <https://127.0.0.1:9999>)
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum GatewayRpcError {
    #[error("HTTP {status} from gateway: {body}")]
    HttpStatus { status: StatusCode, body: String },
    #[error("RPC error {code}: {message}{details}")]
    JsonRpc {
        code: i32,
        message: String,
        details: String,
    },
    #[error("RPC response missing result field")]
    MissingResult,
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
            // Use 10s max delay for network retries (longer than core's default 1s)
            retry_config: RetryConfig::builder()
                .max_delay(Duration::from_secs(10))
                .build(),
        }
    }
}

impl From<&crate::config::Config> for ClientConfig {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            url: config.rpc_url.clone(),
            token: config.token.clone(),
            token_file: config.token_file.clone(),
            ca_cert: config.ca_cert.clone(),
            client_cert: config.client_cert.clone(),
            client_key: config.client_key.clone(),
            insecure: config.insecure,
            timeout: config.timeout,
            // Use 10s max delay for network retries
            retry_config: RetryConfig::builder()
                .max_delay(Duration::from_secs(10))
                .build(),
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
                Err(e)
                    if Self::is_retryable_error(&e) && attempt < self.retry_config.max_attempts =>
                {
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
            return Err(GatewayRpcError::HttpStatus { status, body }.into());
        }

        let rpc_response: JsonRpcResponse = response.json().await?;

        // Check for JSON-RPC error
        if let Some(error) = rpc_response.error {
            let details = error
                .data
                .map(|d| format!("\nDetails: {d}"))
                .unwrap_or_default();
            return Err(GatewayRpcError::JsonRpc {
                code: error.code,
                message: error.message,
                details,
            }
            .into());
        }

        rpc_response
            .result
            .ok_or_else(|| GatewayRpcError::MissingResult.into())
    }

    /// Determine if an error is retryable (transient network/server issues)
    fn is_retryable_error(err: &color_eyre::Report) -> bool {
        if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
            if reqwest_err.is_connect() || reqwest_err.is_timeout() {
                return true;
            }
            if let Some(status) = reqwest_err.status() {
                return status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
            }
        }

        if let Some(gateway_err) = err.downcast_ref::<GatewayRpcError>() {
            return match gateway_err {
                GatewayRpcError::HttpStatus { status, .. } => {
                    status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
                }
                // JSON-RPC reserved server error range (-32099..=-32000)
                GatewayRpcError::JsonRpc { code, .. } => (-32099..=-32000).contains(code),
                GatewayRpcError::MissingResult => false,
            };
        }

        let err_str = err.to_string().to_ascii_lowercase();

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

        // Don't retry authentication errors, not found, bad request, etc.
        false
    }

    /// List configured gitops sources
    pub async fn gitops_list(&self, include_disabled: bool) -> Result<Vec<GitOpsSourceInfo>> {
        let req = GitOpsListSourcesRequest { include_disabled };
        let result = self
            .call_rpc(methods::GITOPS_LIST_SOURCES, serde_json::to_value(&req)?)
            .await?;
        let response: GitOpsListSourcesResponse = serde_json::from_value(result)?;
        Ok(response.sources)
    }

    /// Create a new gitops source
    pub async fn gitops_create(
        &self,
        repository_url: String,
        branch: Option<String>,
        path_pattern: Option<String>,
        sync_frequency_minutes: Option<i32>,
    ) -> Result<GitOpsCreateSourceResponse> {
        let req = GitOpsCreateSourceRequest {
            repository_url,
            branch: branch.unwrap_or_else(|| "main".to_string()),
            path_pattern: path_pattern.unwrap_or_else(|| "schemas/**/*.json".to_string()),
            sync_frequency_minutes: sync_frequency_minutes.unwrap_or(60),
        };
        let result = self
            .call_rpc(methods::GITOPS_CREATE_SOURCE, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Delete a gitops source
    pub async fn gitops_delete(&self, id: String) -> Result<bool> {
        let req = GitOpsDeleteSourceRequest {
            id: id
                .parse()
                .map_err(|e| color_eyre::eyre::eyre!("Invalid UUID: {}", e))?,
        };
        let result = self
            .call_rpc(methods::GITOPS_DELETE_SOURCE, serde_json::to_value(&req)?)
            .await?;
        let response: GitOpsDeleteSourceResponse = serde_json::from_value(result)?;
        Ok(response.deleted)
    }

    /// Trigger manual sync for a gitops source
    pub async fn gitops_sync(&self, id: String) -> Result<GitOpsTriggerSyncResponse> {
        let req = GitOpsTriggerSyncRequest {
            id: id
                .parse()
                .map_err(|e| color_eyre::eyre::eyre!("Invalid UUID: {}", e))?,
        };
        let result = self
            .call_rpc(methods::GITOPS_TRIGGER_SYNC, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Gateway Commands ====================

    /// Ping the gateway
    pub async fn ping(&self) -> Result<String> {
        let result = self.call_rpc("ping", json!({})).await?;
        Ok(result.as_str().unwrap_or("pong").to_string())
    }

    /// Get gateway version
    pub async fn version(&self) -> Result<String> {
        let result = self.call_rpc("version", json!({})).await?;
        Ok(result.as_str().unwrap_or("unknown").to_string())
    }

    // ==================== Core Commands ====================

    /// Get system health status
    pub async fn health(&self) -> Result<SystemHealthResponse> {
        let req = SystemHealthRequest {};
        let result = self
            .call_rpc(methods::SYSTEM_HEALTH, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Node Commands ====================

    /// List all nodes
    pub async fn list_nodes(&self, _role: Option<NodeRole>) -> Result<Vec<InstanceInfo>> {
        let req = ListInstancesRequest::default();
        let result = self
            .call_rpc(
                methods::COORDINATION_LIST_INSTANCES,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ListInstancesResponse = serde_json::from_value(result)?;
        Ok(response.instances)
    }

    /// Get node status
    pub async fn node_status(&self, node_id: &str) -> Result<InstanceHealthResponse> {
        let req = InstanceHealthRequest {
            instance_id: node_id.into(),
        };
        let result = self
            .call_rpc(
                methods::COORDINATION_INSTANCE_HEALTH,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Drain a node for maintenance
    pub async fn drain_node(&self, node_id: &str, reason: Option<&str>) -> Result<()> {
        let req = NodeDrainRequest {
            node_id: node_id.into(),
            reason: reason.map(String::from),
        };
        self.call_rpc(methods::NODES_DRAIN, serde_json::to_value(&req)?)
            .await?;
        Ok(())
    }

    /// Resume a drained node
    pub async fn resume_node(&self, node_id: &str) -> Result<()> {
        let req = NodeResumeRequest {
            node_id: node_id.into(),
        };
        self.call_rpc(methods::NODES_RESUME, serde_json::to_value(&req)?)
            .await?;
        Ok(())
    }

    /// Set node horizon (cutoff time for event processing)
    pub async fn set_node_horizon(&self, node_id: &str, horizon: &str) -> Result<()> {
        // Parse horizon string to Timestamp
        let horizon_ts = Timestamp::parse_rfc3339(horizon).or_else(|_| {
            // Try parsing as unix timestamp
            horizon
                .parse::<i64>()
                .ok()
                .and_then(Timestamp::from_unix_timestamp)
                .ok_or_else(|| color_eyre::eyre::eyre!("Invalid horizon format"))
        })?;

        let req = NodeSetHorizonRequest {
            node_id: node_id.into(),
            horizon: horizon_ts,
        };
        self.call_rpc(methods::NODES_SET_HORIZON, serde_json::to_value(&req)?)
            .await?;
        Ok(())
    }

    // ==================== Replay Commands ====================

    /// Create a replay plan
    pub async fn replay_plan(
        &self,
        node_id: &str,
        since: Option<&str>,
        until: Option<&str>,
        materials: &[String],
        event_types: &[String],
    ) -> Result<ReplayOperation> {
        // Build time window from relative or absolute times
        let time_window = if since.is_some() || until.is_some() {
            let now = Timestamp::now();
            let start = since
                .map(|s| Self::parse_time(s, now))
                .transpose()?
                .unwrap_or_else(|| (now - time::Duration::hours(24)).format_rfc3339());
            let end = until
                .map(|u| Self::parse_time(u, now))
                .transpose()?
                .unwrap_or_else(|| now.format_rfc3339());
            Some((start, end))
        } else {
            None
        };

        let material_filter = if materials.is_empty() {
            None
        } else {
            Some(materials.to_vec())
        };

        let mut filters = std::collections::HashMap::new();
        if !event_types.is_empty() {
            filters.insert(
                "event_types".to_string(),
                serde_json::Value::Array(
                    event_types.iter().map(|t| serde_json::Value::String(t.clone())).collect(),
                ),
            );
        }

        let req = ReplayCreateRequest {
            scope: ReplayScope {
                node_id: node_id.to_string(),
                time_window,
                material_filter,
                filters,
            },
            actor: Some("service:sinexctl".to_string()),
        };

        let result = self
            .call_rpc(
                methods::REPLAY_CREATE_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;

        // Gateway returns { "operation": ReplayOperation }
        let response: ReplayCreateResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// Parse relative time (e.g., "1h", "24h") or RFC3339 timestamp
    fn parse_time(input: &str, now: Timestamp) -> Result<String> {
        // Try relative format first (e.g., "1h", "24h", "7d")
        if let Some(hours) = input.strip_suffix('h')
            && let Ok(h) = hours.parse::<i64>()
        {
            return Ok((now - time::Duration::hours(h)).format_rfc3339());
        }
        if let Some(days) = input.strip_suffix('d')
            && let Ok(d) = days.parse::<i64>()
        {
            return Ok((now - time::Duration::days(d)).format_rfc3339());
        }
        if let Some(mins) = input.strip_suffix('m')
            && let Ok(m) = mins.parse::<i64>()
        {
            return Ok((now - time::Duration::minutes(m)).format_rfc3339());
        }

        // Try RFC3339 format
        if Timestamp::parse_rfc3339(input).is_ok() {
            return Ok(input.to_string());
        }

        Err(color_eyre::eyre::eyre!(
            "Invalid time format '{}': use relative (1h, 24h, 7d) or RFC3339",
            input
        ))
    }

    /// Submit a replay plan for execution
    pub async fn replay_submit(&self, operation_id: &str) -> Result<ReplayOperation> {
        // First approve
        let approve_req = ReplayApproveRequest {
            operation_id: operation_id.to_string(),
            approver: Some("service:sinexctl".to_string()),
        };
        self.call_rpc(
            methods::REPLAY_APPROVE_OPERATION,
            serde_json::to_value(&approve_req)?,
        )
        .await?;

        // Then execute
        let exec_req = ReplayExecuteRequest {
            operation_id: operation_id.to_string(),
            executor: Some("service:sinexctl".to_string()),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_EXECUTE_OPERATION,
                serde_json::to_value(&exec_req)?,
            )
            .await?;

        let response: ReplayExecuteResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// Get replay operation status
    pub async fn replay_status(&self, operation_id: &str) -> Result<ReplayOperation> {
        let req = ReplayStatusRequest {
            operation_id: operation_id.to_string(),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_OPERATION_STATUS,
                serde_json::to_value(&req)?,
            )
            .await?;

        let response: ReplayStatusResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// List all replay operations
    pub async fn replay_list(&self) -> Result<Vec<ReplayOperation>> {
        let req = ReplayListRequest::default();
        let result = self
            .call_rpc(methods::REPLAY_LIST_OPERATIONS, serde_json::to_value(&req)?)
            .await?;

        let response: ReplayListResponse = serde_json::from_value(result)?;
        Ok(response.operations)
    }

    /// List replay operations with optional state filter
    pub async fn replay_list_filtered(&self, state: Option<ReplayState>) -> Result<Vec<ReplayOperation>> {
        let req = ReplayListRequest {
            state,
            ..Default::default()
        };
        let result = self
            .call_rpc(methods::REPLAY_LIST_OPERATIONS, serde_json::to_value(&req)?)
            .await?;

        let response: ReplayListResponse = serde_json::from_value(result)?;
        Ok(response.operations)
    }

    /// Preview a replay operation
    pub async fn replay_preview(&self, operation_id: &str) -> Result<(ReplayOperation, serde_json::Value)> {
        let req = ReplayPreviewRequest {
            operation_id: operation_id.to_string(),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_PREVIEW_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplayPreviewResponse = serde_json::from_value(result)?;
        Ok((response.operation, response.preview))
    }

    /// Approve a replay operation for execution
    pub async fn replay_approve(&self, operation_id: &str) -> Result<ReplayOperation> {
        let req = ReplayApproveRequest {
            operation_id: operation_id.to_string(),
            approver: Some("service:sinexctl".to_string()),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_APPROVE_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplayApproveResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// Execute an approved replay operation
    pub async fn replay_execute(&self, operation_id: &str) -> Result<ReplayOperation> {
        let req = ReplayExecuteRequest {
            operation_id: operation_id.to_string(),
            executor: Some("service:sinexctl".to_string()),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_EXECUTE_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplayExecuteResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// Cancel a replay operation
    pub async fn replay_cancel(&self, operation_id: &str, reason: Option<&str>) -> Result<String> {
        let req = ReplayCancelRequest {
            operation_id: operation_id.to_string(),
            canceller: Some("service:sinexctl".to_string()),
            reason: reason.map(String::from),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_CANCEL_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplayCancelResponse = serde_json::from_value(result)?;
        Ok(response.status)
    }

    // ==================== DLQ Commands ====================

    /// List dead letter queues
    pub async fn dlq_list(&self) -> Result<DlqListResponse> {
        let req = DlqListRequest {};
        let result = self
            .call_rpc(methods::DLQ_LIST, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Peek at messages in a DLQ
    pub async fn dlq_peek(&self, limit: Option<usize>) -> Result<DlqPeekResponse> {
        let req = DlqPeekRequest {
            limit: limit.unwrap_or(10),
        };
        let result = self
            .call_rpc(methods::DLQ_PEEK, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Requeue messages from DLQ
    pub async fn dlq_requeue(
        &self,
        event_id: Option<String>,
        all: bool,
    ) -> Result<DlqRequeueResponse> {
        let req = DlqRequeueRequest { event_id, all };
        let result = self
            .call_rpc(methods::DLQ_REQUEUE, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Purge all messages from DLQ
    pub async fn dlq_purge(&self, confirm: bool) -> Result<DlqPurgeResponse> {
        let req = DlqPurgeRequest { confirm };
        let result = self
            .call_rpc(methods::DLQ_PURGE, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Event Query Commands ====================

    /// Query events using the composable query engine
    pub async fn query_events(&self, query: EventQuery) -> Result<EventQueryResult> {
        let result = self
            .call_rpc(methods::EVENTS_QUERY, serde_json::to_value(&query)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Trace provenance lineage for an event
    pub async fn trace_lineage(&self, query: LineageQuery) -> Result<LineageResult> {
        let result = self
            .call_rpc(methods::EVENTS_LINEAGE, serde_json::to_value(&query)?)
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
    pub async fn audit_get(
        &self,
        operation_id: &str,
    ) -> Result<sinex_primitives::rpc::audit::AuditGetResponse> {
        use sinex_primitives::Id;
        use sinex_primitives::rpc::audit::{AuditGetRequest, AuditGetResponse};
        use sinex_primitives::rpc::ops::Operation;

        let op_id = operation_id
            .parse::<Id<Operation>>()
            .map_err(|e| color_eyre::eyre::eyre!("Invalid operation ID: {}", e))?;

        let request = AuditGetRequest {
            operation_id: op_id,
            after_id: None,
            limit: 100,
        };
        let result = self
            .call_rpc("audit.get", serde_json::to_value(&request)?)
            .await?;
        let response: AuditGetResponse = serde_json::from_value(result)?;
        Ok(response)
    }

    // ==================== Lifecycle Commands ====================

    /// Get lifecycle tier status
    pub async fn lifecycle_status(&self) -> Result<LifecycleStatusResponse> {
        let req = LifecycleStatusRequest { by_source: false };
        let result = self
            .call_rpc(methods::LIFECYCLE_STATUS, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Archive live events (move to `audit.archived_events`)
    pub async fn lifecycle_archive(
        &self,
        source: Option<String>,
        before: Option<String>,
        event_ids: Option<Vec<String>>,
        limit: i64,
        dry_run: bool,
    ) -> Result<LifecycleArchiveResponse> {
        let req = LifecycleArchiveRequest {
            source: source.map(EventSource::new).transpose()?,
            before,
            event_ids,
            limit,
            reason: None,
            dry_run,
        };
        let result = self
            .call_rpc(methods::LIFECYCLE_ARCHIVE, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Restore archived events back to live
    pub async fn lifecycle_restore(
        &self,
        event_ids: Vec<String>,
        dry_run: bool,
    ) -> Result<LifecycleRestoreResponse> {
        let req = LifecycleRestoreRequest { event_ids, dry_run };
        let result = self
            .call_rpc(methods::LIFECYCLE_RESTORE, serde_json::to_value(&req)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== Two-Step Tombstone Commands (SEC-003) ====================

    /// Create a tombstone operation (Step 1)
    pub async fn tombstone_create(
        &self,
        source: Option<String>,
        before: Option<String>,
        event_ids: Option<Vec<String>>,
        limit: i64,
        reason: String,
    ) -> Result<TombstoneCreateResponse> {
        let req = TombstoneCreateRequest {
            source: source.map(EventSource::new).transpose()?,
            before,
            event_ids,
            limit,
            reason,
        };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_CREATE,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Preview cascade analysis for a tombstone operation
    pub async fn tombstone_preview(
        &self,
        operation_id: String,
    ) -> Result<TombstonePreviewResponse> {
        let req = TombstonePreviewRequest { operation_id };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_PREVIEW,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Approve and execute a tombstone operation (Step 2 - PERMANENT!)
    pub async fn tombstone_approve(
        &self,
        operation_id: String,
        confirm: bool,
    ) -> Result<TombstoneApproveResponse> {
        let req = TombstoneApproveRequest {
            operation_id,
            yes_i_understand_data_is_gone: confirm,
        };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_APPROVE,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Cancel a pending tombstone operation
    pub async fn tombstone_cancel(
        &self,
        operation_id: String,
        reason: Option<String>,
    ) -> Result<TombstoneCancelResponse> {
        let req = TombstoneCancelRequest {
            operation_id,
            reason,
        };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_CANCEL,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// List tombstone operations
    pub async fn tombstone_list(
        &self,
        state: Option<TombstoneOperationState>,
        limit: Option<i64>,
    ) -> Result<TombstoneListResponse> {
        let req = TombstoneListRequest { state, limit };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_LIST,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    /// Get status of a specific tombstone operation
    pub async fn tombstone_status(&self, operation_id: String) -> Result<TombstoneStatusResponse> {
        let req = TombstoneStatusRequest { operation_id };
        let result = self
            .call_rpc(
                methods::LIFECYCLE_TOMBSTONE_STATUS,
                serde_json::to_value(&req)?,
            )
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    // ==================== SSE Event Stream ====================

    /// Subscribe to real-time events via SSE.
    ///
    /// Returns a stream of [`SseClientMessage`] values. The stream ends when the
    /// server closes the connection or an error occurs.
    pub async fn subscribe_events(
        &self,
        filter: SubscriptionFilter,
    ) -> Result<impl futures::Stream<Item = Result<SseClientMessage>>> {
        let filter_json = serde_json::to_string(&filter)?;
        let url = format!("{}/events/stream", self.base_url);

        let response = self
            .client
            .get(&url)
            .query(&[("filter", &filter_json)])
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", "text/event-stream")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            return Err(color_eyre::eyre::eyre!(
                "SSE stream error HTTP {}: {}",
                status,
                body
            ));
        }

        Ok(SseFrameParser::new(response))
    }
}

// ─────────────────────────────────────────────────────────────────────
// SSE client types
// ─────────────────────────────────────────────────────────────────────

/// Parsed SSE messages received by the CLI client.
#[derive(Debug)]
pub enum SseClientMessage {
    Event {
        event: sinex_primitives::events::Event<serde_json::Value>,
    },
    Gap {
        from_seq: u64,
        to_seq: u64,
        dropped: u64,
    },
    Heartbeat,
}

/// Streaming SSE frame parser over a reqwest response.
struct SseFrameParser {
    stream: reqwest::Response,
    buffer: String,
    current_event: Option<String>,
    current_data: String,
}

impl SseFrameParser {
    fn new(response: reqwest::Response) -> Self {
        Self {
            stream: response,
            buffer: String::new(),
            current_event: None,
            current_data: String::new(),
        }
    }
}

impl futures::Stream for SseFrameParser {
    type Item = Result<SseClientMessage>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let this = self.get_mut();

        loop {
            // Try to parse a complete SSE frame from buffer
            if let Some(msg) = this.try_parse_frame() {
                return Poll::Ready(Some(Ok(msg)));
            }

            // Read more data from the response stream
            let chunk = {
                let chunk_future = this.stream.chunk();
                tokio::pin!(chunk_future);
                match chunk_future.poll(cx) {
                    Poll::Ready(Ok(Some(bytes))) => bytes,
                    Poll::Ready(Ok(None)) => return Poll::Ready(None), // Stream ended
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(color_eyre::eyre::eyre!(
                            "SSE stream read error: {}",
                            e
                        ))));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            };

            this.buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    }
}

impl SseFrameParser {
    /// Try to parse one complete SSE frame from the internal buffer.
    fn try_parse_frame(&mut self) -> Option<SseClientMessage> {
        loop {
            // Find the next newline
            let newline_pos = self.buffer.find('\n')?;
            let line = self.buffer[..newline_pos]
                .trim_end_matches('\r')
                .to_string();
            self.buffer.drain(..=newline_pos);

            if line.is_empty() {
                // Empty line = dispatch event
                if !self.current_data.is_empty() {
                    let msg = self.dispatch_frame();
                    self.current_event = None;
                    self.current_data.clear();
                    if let Some(msg) = msg {
                        return Some(msg);
                    }
                }
                continue;
            }

            if let Some(value) = line.strip_prefix("event:") {
                self.current_event = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                if !self.current_data.is_empty() {
                    self.current_data.push('\n');
                }
                self.current_data.push_str(value.trim());
            }
            // Ignore `id:`, `retry:`, and comment lines (`:`)
        }
    }

    fn dispatch_frame(&self) -> Option<SseClientMessage> {
        let event_type = self.current_event.as_deref().unwrap_or("message");

        match event_type {
            "event" => {
                #[derive(Deserialize)]
                struct EventWrapper {
                    event: sinex_primitives::events::Event<serde_json::Value>,
                }
                let wrapper: EventWrapper = serde_json::from_str(&self.current_data).ok()?;
                Some(SseClientMessage::Event {
                    event: wrapper.event,
                })
            }
            "gap" => {
                #[derive(Deserialize)]
                struct GapWrapper {
                    from_seq: u64,
                    to_seq: u64,
                    dropped: u64,
                }
                let gap: GapWrapper = serde_json::from_str(&self.current_data).ok()?;
                Some(SseClientMessage::Gap {
                    from_seq: gap.from_seq,
                    to_seq: gap.to_seq,
                    dropped: gap.dropped,
                })
            }
            "heartbeat" => Some(SseClientMessage::Heartbeat),
            _ => None, // Unknown event type
        }
    }
}
