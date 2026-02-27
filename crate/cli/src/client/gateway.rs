use std::path::Path;
use std::time::Duration;

use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_primitives::domain::EventSource;
use sinex_primitives::rpc::{
    JsonRpcError, coordination::*, dlq::*, gitops::*, lifecycle::*, methods, nodes::*, replay::*,
    system::*,
};
use sinex_primitives::temporal::Timestamp;

use crate::Result;
use crate::auth::{load_client_cert, load_root_ca, load_token};
use crate::client::RetryConfig;
use crate::model::NodeRole;
use crate::model::search::{SearchQuery, SearchResult};

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
                    .map(|d| format!("\nDetails: {d}"))
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
                .map_err(|e| color_eyre::eyre::eyre!("Invalid ULID: {}", e))?,
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
                .map_err(|e| color_eyre::eyre::eyre!("Invalid ULID: {}", e))?,
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

        let req = ReplayCreateRequest {
            scope: ReplayScope {
                node_id: node_id.to_string(),
                time_window,
                material_filter: None,
                filters: std::collections::HashMap::new(),
            },
            actor: Some("service:sinexctl".to_string()),
        };

        let result = self
            .call_rpc(methods::REPLAY_CREATE, serde_json::to_value(&req)?)
            .await?;

        // Gateway returns { "operation": ReplayOperation }
        let response: ReplayCreateResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// Parse relative time (e.g., "1h", "24h") or RFC3339 timestamp
    fn parse_time(input: &str, now: Timestamp) -> Result<String> {
        // Try relative format first (e.g., "1h", "24h", "7d")
        if let Some(hours) = input.strip_suffix('h') {
            if let Ok(h) = hours.parse::<i64>() {
                return Ok((now - time::Duration::hours(h)).format_rfc3339());
            }
        }
        if let Some(days) = input.strip_suffix('d') {
            if let Ok(d) = days.parse::<i64>() {
                return Ok((now - time::Duration::days(d)).format_rfc3339());
            }
        }
        if let Some(mins) = input.strip_suffix('m') {
            if let Ok(m) = mins.parse::<i64>() {
                return Ok((now - time::Duration::minutes(m)).format_rfc3339());
            }
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
        self.call_rpc(methods::REPLAY_APPROVE, serde_json::to_value(&approve_req)?)
            .await?;

        // Then execute
        let exec_req = ReplayExecuteRequest {
            operation_id: operation_id.to_string(),
            executor: Some("service:sinexctl".to_string()),
        };
        let result = self
            .call_rpc(methods::REPLAY_EXECUTE, serde_json::to_value(&exec_req)?)
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
            .call_rpc(methods::REPLAY_STATUS, serde_json::to_value(&req)?)
            .await?;

        let response: ReplayStatusResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    /// List all replay operations
    pub async fn replay_list(&self) -> Result<Vec<ReplayOperation>> {
        let req = ReplayListRequest::default();
        let result = self
            .call_rpc(methods::REPLAY_LIST, serde_json::to_value(&req)?)
            .await?;

        let response: ReplayListResponse = serde_json::from_value(result)?;
        Ok(response.operations)
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

    /// Archive live events (move to audit.archived_events)
    pub async fn lifecycle_archive(
        &self,
        source: Option<String>,
        before: Option<String>,
        event_ids: Option<Vec<String>>,
        limit: i64,
        dry_run: bool,
    ) -> Result<LifecycleArchiveResponse> {
        let req = LifecycleArchiveRequest {
            source: source.map(EventSource::new),
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
            source: source.map(EventSource::new),
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
        state: Option<String>,
        limit: Option<i64>,
    ) -> Result<TombstoneListResponse> {
        // Parse state string to enum if provided
        let state_enum = state.map(|s| match s.to_lowercase().as_str() {
            "pending" => TombstoneOperationState::Pending,
            "previewed" => TombstoneOperationState::Previewed,
            "approved" => TombstoneOperationState::Approved,
            "executing" => TombstoneOperationState::Executing,
            "completed" => TombstoneOperationState::Completed,
            "cancelled" => TombstoneOperationState::Cancelled,
            "failed" => TombstoneOperationState::Failed,
            "expired" => TombstoneOperationState::Expired,
            _ => TombstoneOperationState::Pending, // Default fallback
        });

        let req = TombstoneListRequest {
            state: state_enum,
            limit,
        };
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
}
