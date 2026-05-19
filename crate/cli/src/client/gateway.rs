use std::path::Path;
use std::time::Duration;

use color_eyre::eyre::eyre;
use reqwest::{ClientBuilder, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sinex_primitives::constants::env_vars;
use sinex_primitives::domain::{EventSource, NodeType};
use sinex_primitives::rpc::{
    JsonRpcError, RpcMethod,
    automata::{AUTOMATA_STATUS_METHOD, AutomataStatusRequest, AutomataStatusResponse},
    coordination::{
        InstanceHealthRequest, InstanceHealthResponse, InstanceInfo, ListInstancesRequest,
        ListInstancesResponse,
    },
    dlq::{
        DLQ_LIST_METHOD, DLQ_PEEK_METHOD, DLQ_PURGE_METHOD, DLQ_REQUEUE_METHOD, DlqListRequest,
        DlqListResponse, DlqPeekRequest, DlqPeekResponse, DlqPurgeRequest, DlqPurgeResponse,
        DlqRequeueRequest, DlqRequeueResponse,
    },
    documents::{
        DOCUMENTS_GET_CHUNKS_METHOD, DOCUMENTS_GET_METHOD, DOCUMENTS_SEARCH_METHOD,
        DocumentsGetChunksRequest, DocumentsGetChunksResponse, DocumentsGetRequest,
        DocumentsGetResponse, DocumentsSearchRequest, DocumentsSearchResponse,
    },
    events::{
        EVENTS_ANNOTATE_METHOD, EVENTS_LINEAGE_METHOD, EVENTS_QUERY_METHOD, EventsAnnotateRequest,
        EventsAnnotateResponse,
    },
    gitops::{
        DEFAULT_GITOPS_BRANCH, DEFAULT_GITOPS_PATH_PATTERN, DEFAULT_GITOPS_SYNC_FREQUENCY_MINUTES,
        GitOpsCreateSourceRequest, GitOpsCreateSourceResponse, GitOpsDeleteSourceRequest,
        GitOpsDeleteSourceResponse, GitOpsListSourcesRequest, GitOpsListSourcesResponse,
        GitOpsSourceInfo, GitOpsTriggerSyncRequest, GitOpsTriggerSyncResponse,
    },
    ingestors::{INGESTORS_STATUS_METHOD, IngestorsStatusRequest, IngestorsStatusResponse},
    lifecycle::{
        LIFECYCLE_ARCHIVE_METHOD, LIFECYCLE_RESTORE_METHOD, LIFECYCLE_STATUS_METHOD,
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
    ops::{Operation as OpsOperation, OpsGetResponse, OpsListResponse, OpsStartResponse},
    replay::{
        ReplayApproveRequest, ReplayApproveResponse, ReplayCancelRequest, ReplayCancelResponse,
        ReplayCreateRequest, ReplayCreateResponse, ReplayExecuteRequest, ReplayExecuteResponse,
        ReplayListRequest, ReplayListResponse, ReplayOperation, ReplayPreviewRequest,
        ReplayPreviewResponse, ReplayScope, ReplayState, ReplayStatusRequest, ReplayStatusResponse,
        ReplaySubmitRequest, ReplaySubmitResponse,
    },
    sources::{
        SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD, SOURCES_CONTINUITY_GET_METHOD,
        SOURCES_CONTINUITY_LIST_METHOD, SOURCES_CONTINUITY_METHOD, SOURCES_COVERAGE_METHOD,
        SOURCES_LIST_METHOD, SOURCES_READINESS_GET_METHOD, SOURCES_READINESS_LIST_METHOD,
        SOURCES_SHOW_METHOD,
        SourcesAnnotateRequest, SourcesAnnotateResponse, SourcesArchiveRequest,
        SourcesArchiveResponse, SourcesContinuityRequest, SourcesContinuityResponse,
        SourcesCoverageRequest, SourcesCoverageResponse, SourcesListRequest, SourcesListResponse,
        SourcesReadinessGetRequest, SourcesReadinessGetResponse, SourcesReadinessListRequest,
        SourcesReadinessListResponse, SourcesShowRequest, SourcesShowResponse, SourcesStageRequest,
        SourcesStageResponse,
    },
    system::{
        SYSTEM_HEALTH_METHOD, SYSTEM_PING_METHOD, SYSTEM_VERSION_METHOD, SystemHealthRequest,
        SystemHealthResponse, SystemPingRequest, SystemVersionRequest,
    },
    tasks::{
        TASKS_COMPLETE_METHOD, TASKS_CREATE_METHOD, TASKS_STATE_GET_METHOD, TaskCompleteRequest,
        TaskCompleteResponse, TaskCreateRequest, TaskCreateResponse, TaskStateGetRequest,
        TaskStateResponse,
    },
    telemetry::{
        AssemblyStatsBucket, CommandFrequencyEntry, CurrentDeviceStateEntry, CurrentHealthEntry,
        FileActivityEntry, GatewayStatsBucket, IngestdBatchStatsBucket, IngestdValidationSnapshot,
        MetricCounterBucket, NodeStatsBucket, RecentActivityEntry, StreamStatsBucket,
        SystemStateBucket, TELEMETRY_ASSEMBLY_STATS_METHOD, TELEMETRY_COMMAND_FREQUENCY_METHOD,
        TELEMETRY_CURRENT_DEVICE_STATE_METHOD, TELEMETRY_CURRENT_HEALTH_METHOD,
        TELEMETRY_FILE_ACTIVITY_METHOD, TELEMETRY_GATEWAY_STATS_METHOD,
        TELEMETRY_INGESTD_BATCH_STATS_METHOD, TELEMETRY_INGESTD_VALIDATION_METHOD,
        TELEMETRY_METRIC_COUNTERS_METHOD, TELEMETRY_NODE_STATS_METHOD,
        TELEMETRY_RECENT_ACTIVITY_METHOD, TELEMETRY_STREAM_STATS_METHOD,
        TELEMETRY_SYSTEM_STATE_METHOD, TELEMETRY_THROUGHPUT_METHOD, TELEMETRY_WINDOW_FOCUS_METHOD,
        TelemetryAssemblyStatsRequest, TelemetryAssemblyStatsResponse,
        TelemetryCommandFrequencyRequest, TelemetryCommandFrequencyResponse,
        TelemetryCurrentDeviceStateRequest, TelemetryCurrentDeviceStateResponse,
        TelemetryCurrentHealthRequest, TelemetryCurrentHealthResponse,
        TelemetryFileActivityRequest, TelemetryFileActivityResponse, TelemetryGatewayStatsRequest,
        TelemetryGatewayStatsResponse, TelemetryIngestdBatchStatsRequest,
        TelemetryIngestdBatchStatsResponse, TelemetryIngestdValidationRequest,
        TelemetryIngestdValidationResponse, TelemetryMetricCountersRequest,
        TelemetryMetricCountersResponse, TelemetryNodeStatsRequest, TelemetryNodeStatsResponse,
        TelemetryRecentActivityRequest, TelemetryRecentActivityResponse,
        TelemetryStreamStatsRequest, TelemetryStreamStatsResponse, TelemetrySystemStateRequest,
        TelemetrySystemStateResponse, TelemetryThroughputRequest, TelemetryThroughputResponse,
        TelemetryTimeRange, TelemetryWindowFocusRequest, TelemetryWindowFocusResponse,
        WindowFocusBucket,
    },
};
use sinex_primitives::sources::continuity::{
    SourcesContinuityGetRequest, SourcesContinuityGetResponse, SourcesContinuityListRequest,
    SourcesContinuityListResponse, SourcesExplainGapRequest, SourcesExplainGapResponse,
};
use sinex_primitives::temporal::Timestamp;

use crate::Result;
use crate::auth::load_token;
use crate::client::RetryConfig;
use crate::model::NodeRole;
use crate::validation::{parse_time_input, parse_time_input_with_now, validate_time_range};
use sinex_primitives::RuntimeTargetGatewayTokenRole;
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
    /// Role suffix to apply to a raw runtime token.
    pub token_role: Option<RuntimeTargetGatewayTokenRole>,
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
    #[error("RPC protocol violation: {0}")]
    ProtocolViolation(String),
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: Value,
    id: u64,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    error: Option<JsonRpcError>,
    id: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: std::env::var(env_vars::RPC_URL)
                .unwrap_or_else(|_| "https://127.0.0.1:9999".to_string()),
            token: None,
            token_file: None,
            token_role: None,
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
            token_role: config.token_role,
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
        let token = load_token(config.token.as_deref(), token_file_path, config.token_role)?;

        // Build HTTP client
        let mut client_builder = ClientBuilder::new()
            .user_agent("sinexctl/1.0")
            .timeout(Duration::from_secs(config.timeout))
            .use_rustls_tls();

        // Configure TLS
        if let Some(ca_path) = &config.ca_cert {
            let certs = reqwest::Certificate::from_pem_bundle(&std::fs::read(Path::new(ca_path))?)?;
            for cert in certs {
                client_builder = client_builder.add_root_certificate(cert);
            }
        }

        // Configure mTLS client certificate
        if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
            let mut identity_pem = std::fs::read(Path::new(cert_path))?;
            if !identity_pem.ends_with(b"\n") {
                identity_pem.push(b'\n');
            }
            identity_pem.extend(std::fs::read(Path::new(key_path))?);
            let identity = reqwest::Identity::from_pem(&identity_pem)?;
            client_builder = client_builder.identity(identity);
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

    /// Call a JSON-RPC method by name with retry logic.
    ///
    /// This is the public escape hatch for commands whose RPC contract has not
    /// yet been promoted to a typed client method (e.g. `sources.stage`).
    pub async fn call_raw_rpc(&self, method: &str, params: Value) -> Result<Value> {
        self.call_rpc(method, params).await
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

    /// Call a typed JSON-RPC method with retry logic.
    async fn call_typed<Req, Resp>(
        &self,
        method: RpcMethod<Req, Resp>,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let params = serde_json::to_value(request)?;
        let result = self.call_rpc(method.name, params).await?;
        serde_path_to_error::deserialize(result).map_err(|error| {
            eyre!(
                "failed to decode {} response at {}: {}",
                method.name,
                error.path(),
                error.inner()
            )
        })
    }

    /// Perform a single RPC call attempt (without retry)
    async fn call_rpc_once(&self, method: &str, params: Value) -> Result<Value> {
        const REQUEST_ID: u64 = 1;

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id: REQUEST_ID,
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
        Self::validate_rpc_response(&rpc_response, REQUEST_ID)?;

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

    fn validate_rpc_response(rpc_response: &JsonRpcResponse, expected_id: u64) -> Result<()> {
        if rpc_response.jsonrpc != "2.0" {
            return Err(GatewayRpcError::ProtocolViolation(format!(
                "expected jsonrpc=2.0, got {}",
                rpc_response.jsonrpc
            ))
            .into());
        }
        if rpc_response.id != expected_id {
            return Err(GatewayRpcError::ProtocolViolation(format!(
                "expected response id {expected_id}, got {}",
                rpc_response.id
            ))
            .into());
        }
        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn expect_string_result(method: &str, result: Value) -> Result<String> {
        result.as_str().map(ToOwned::to_owned).ok_or_else(|| {
            GatewayRpcError::ProtocolViolation(format!(
                "{method} returned non-string result: {result}"
            ))
            .into()
        })
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
                GatewayRpcError::MissingResult | GatewayRpcError::ProtocolViolation(_) => false,
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
            branch: branch.unwrap_or_else(|| DEFAULT_GITOPS_BRANCH.to_string()),
            path_pattern: path_pattern.unwrap_or_else(|| DEFAULT_GITOPS_PATH_PATTERN.to_string()),
            sync_frequency_minutes: sync_frequency_minutes
                .unwrap_or(DEFAULT_GITOPS_SYNC_FREQUENCY_MINUTES),
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
        self.call_typed(SYSTEM_PING_METHOD, &SystemPingRequest {})
            .await
    }

    /// Get gateway version
    pub async fn version(&self) -> Result<String> {
        self.call_typed(SYSTEM_VERSION_METHOD, &SystemVersionRequest {})
            .await
    }

    // ==================== Core Commands ====================

    /// Get system health status
    pub async fn health(&self) -> Result<SystemHealthResponse> {
        let req = SystemHealthRequest {};
        self.call_typed(SYSTEM_HEALTH_METHOD, &req).await
    }

    // ==================== Node Commands ====================

    /// List derived-node/automata status.
    pub async fn automata_status(
        &self,
        stale_after_secs: u64,
        recent_window_secs: u64,
    ) -> Result<AutomataStatusResponse> {
        let req = AutomataStatusRequest {
            stale_after_secs,
            recent_window_secs,
        };
        self.call_typed(AUTOMATA_STATUS_METHOD, &req).await
    }

    /// List ingestor status (manifest, run, latest health.status, recent emissions).
    pub async fn ingestors_status(
        &self,
        stale_after_secs: u64,
        recent_window_secs: u64,
    ) -> Result<IngestorsStatusResponse> {
        let req = IngestorsStatusRequest {
            stale_after_secs,
            recent_window_secs,
        };
        self.call_typed(INGESTORS_STATUS_METHOD, &req).await
    }

    /// List all nodes, optionally filtered by role.
    pub async fn list_nodes(&self, role: Option<NodeRole>) -> Result<Vec<InstanceInfo>> {
        let req = ListInstancesRequest {
            node_type: role.map(|r| match r {
                NodeRole::Capture => NodeType::Ingestor,
                NodeRole::Synthesis => NodeType::Automaton,
                NodeRole::Core => NodeType::Service,
                NodeRole::Gateway => NodeType::Service,
            }),
            ..Default::default()
        };
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
        let horizon_ts = parse_time_input(horizon)?;

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
        let time_window = Self::build_replay_time_window(since, until, Timestamp::now())?
            .map(|(start, end)| (start.format_rfc3339(), end.format_rfc3339()));

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
                    event_types
                        .iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                ),
            );
        }

        let req = ReplayCreateRequest {
            scope: ReplayScope {
                node_id: node_id.to_string(),
                time_window,
                material_filter,
                filters,
                source_unit_id: None,
                source_material_id: None,
                parser_id: None,
                parser_version: None,
            },
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

    fn build_replay_time_window(
        since: Option<&str>,
        until: Option<&str>,
        now: Timestamp,
    ) -> Result<Option<(Timestamp, Timestamp)>> {
        let start = since
            .map(|input| parse_time_input_with_now(input, now))
            .transpose()?;
        let end = until
            .map(|input| parse_time_input_with_now(input, now))
            .transpose()?;

        let Some((start, end)) = (match (start, end) {
            (None, None) => None,
            (Some(start), Some(end)) => Some((start, end)),
            (Some(start), None) => Some((start, now)),
            (None, Some(end)) => Some((end - time::Duration::hours(24), end)),
        }) else {
            return Ok(None);
        };

        validate_time_range(Some(start), Some(end))?;
        Ok(Some((start, end)))
    }

    /// Submit a replay plan for execution
    pub async fn replay_submit(&self, operation_id: &str) -> Result<ReplayOperation> {
        match self.replay_status(operation_id).await?.state {
            ReplayState::Planning => {
                self.replay_preview(operation_id).await?;
            }
            ReplayState::Previewed => {}
            ReplayState::Approved => return self.replay_execute(operation_id).await,
            ReplayState::Executing | ReplayState::Committing | ReplayState::Cancelling => {
                return Err(color_eyre::eyre::eyre!(
                    "Replay operation {operation_id} is already in progress"
                ));
            }
            ReplayState::Completed | ReplayState::Failed | ReplayState::Cancelled => {
                return Err(color_eyre::eyre::eyre!(
                    "Replay operation {operation_id} is already in terminal state"
                ));
            }
        }

        let req = ReplaySubmitRequest {
            operation_id: operation_id.to_string(),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_SUBMIT_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplaySubmitResponse = serde_json::from_value(result)?;
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
        self.replay_list_filtered(None, None, None).await
    }

    /// List replay operations with optional filters
    pub async fn replay_list_filtered(
        &self,
        state: Option<ReplayState>,
        node: Option<&str>,
        limit: Option<i64>,
    ) -> Result<Vec<ReplayOperation>> {
        let req = ReplayListRequest {
            state,
            node: node.map(String::from),
            limit,
        };
        let result = self
            .call_rpc(methods::REPLAY_LIST_OPERATIONS, serde_json::to_value(&req)?)
            .await?;

        let response: ReplayListResponse = serde_json::from_value(result)?;
        Ok(response.operations)
    }

    /// Preview a replay operation
    pub async fn replay_preview(
        &self,
        operation_id: &str,
    ) -> Result<(ReplayOperation, serde_json::Value)> {
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

    /// Execute an approved replay operation.
    pub async fn replay_execute(&self, operation_id: &str) -> Result<ReplayOperation> {
        let req = ReplayExecuteRequest {
            operation_id: operation_id.to_string(),
            dry_run: false,
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
    pub async fn replay_cancel(
        &self,
        operation_id: &str,
        reason: Option<&str>,
    ) -> Result<ReplayOperation> {
        let req = ReplayCancelRequest {
            operation_id: operation_id.to_string(),
            reason: reason.map(String::from),
        };
        let result = self
            .call_rpc(
                methods::REPLAY_CANCEL_OPERATION,
                serde_json::to_value(&req)?,
            )
            .await?;
        let response: ReplayCancelResponse = serde_json::from_value(result)?;
        Ok(response.operation)
    }

    // ==================== DLQ Commands ====================

    /// List raw-ingest dead-letter queue statistics
    pub async fn dlq_list(&self) -> Result<DlqListResponse> {
        let req = DlqListRequest {};
        self.call_typed(DLQ_LIST_METHOD, &req).await
    }

    /// Peek at messages in a DLQ
    pub async fn dlq_peek(&self, limit: Option<usize>) -> Result<DlqPeekResponse> {
        let req = DlqPeekRequest {
            limit: limit.unwrap_or(10),
        };
        self.call_typed(DLQ_PEEK_METHOD, &req).await
    }

    /// Requeue messages from DLQ
    pub async fn dlq_requeue(
        &self,
        event_id: Option<String>,
        all: bool,
    ) -> Result<DlqRequeueResponse> {
        let req = DlqRequeueRequest { event_id, all };
        self.call_typed(DLQ_REQUEUE_METHOD, &req).await
    }

    /// Purge all messages from DLQ
    pub async fn dlq_purge(&self, confirm: bool) -> Result<DlqPurgeResponse> {
        let req = DlqPurgeRequest { confirm };
        self.call_typed(DLQ_PURGE_METHOD, &req).await
    }

    // ==================== Event Query Commands ====================

    /// Query events using the composable query engine
    pub async fn query_events(&self, query: EventQuery) -> Result<EventQueryResult> {
        self.call_typed(EVENTS_QUERY_METHOD, &query).await
    }

    /// Trace provenance lineage for an event
    pub async fn trace_lineage(&self, query: LineageQuery) -> Result<LineageResult> {
        self.call_typed(EVENTS_LINEAGE_METHOD, &query).await
    }

    /// Record an annotation against an event.
    pub async fn events_annotate(
        &self,
        request: EventsAnnotateRequest,
    ) -> Result<EventsAnnotateResponse> {
        self.call_typed(EVENTS_ANNOTATE_METHOD, &request).await
    }

    // ==================== Task Domain Commands ====================

    /// Create a manual task declaration.
    pub async fn tasks_create(&self, request: TaskCreateRequest) -> Result<TaskCreateResponse> {
        self.call_typed(TASKS_CREATE_METHOD, &request).await
    }

    /// Mark a task complete.
    pub async fn tasks_complete(
        &self,
        request: TaskCompleteRequest,
    ) -> Result<TaskCompleteResponse> {
        self.call_typed(TASKS_COMPLETE_METHOD, &request).await
    }

    /// Fetch current task state.
    pub async fn tasks_state_get(&self, request: TaskStateGetRequest) -> Result<TaskStateResponse> {
        self.call_typed(TASKS_STATE_GET_METHOD, &request).await
    }

    // ==================== Source Material Commands ====================

    pub async fn sources_stage(
        &self,
        request: SourcesStageRequest,
    ) -> Result<SourcesStageResponse> {
        let result = self
            .call_rpc(methods::SOURCES_STAGE, serde_json::to_value(&request)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    pub async fn sources_list(&self, request: SourcesListRequest) -> Result<SourcesListResponse> {
        self.call_typed(SOURCES_LIST_METHOD, &request).await
    }

    pub async fn sources_show(&self, request: SourcesShowRequest) -> Result<SourcesShowResponse> {
        self.call_typed(SOURCES_SHOW_METHOD, &request).await
    }

    pub async fn sources_coverage(
        &self,
        request: SourcesCoverageRequest,
    ) -> Result<SourcesCoverageResponse> {
        self.call_typed(SOURCES_COVERAGE_METHOD, &request).await
    }

    pub async fn sources_annotate(
        &self,
        request: SourcesAnnotateRequest,
    ) -> Result<SourcesAnnotateResponse> {
        let result = self
            .call_rpc(methods::SOURCES_ANNOTATE, serde_json::to_value(&request)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    pub async fn sources_archive(
        &self,
        request: SourcesArchiveRequest,
    ) -> Result<SourcesArchiveResponse> {
        let result = self
            .call_rpc(methods::SOURCES_ARCHIVE, serde_json::to_value(&request)?)
            .await?;
        serde_json::from_value(result).map_err(Into::into)
    }

    pub async fn sources_continuity(
        &self,
        request: SourcesContinuityRequest,
    ) -> Result<SourcesContinuityResponse> {
        self.call_typed(SOURCES_CONTINUITY_METHOD, &request).await
    }

    pub async fn sources_continuity_get(
        &self,
        request: SourcesContinuityGetRequest,
    ) -> Result<SourcesContinuityGetResponse> {
        self.call_typed(SOURCES_CONTINUITY_GET_METHOD, &request)
            .await
    }

    pub async fn sources_continuity_list(
        &self,
        request: SourcesContinuityListRequest,
    ) -> Result<SourcesContinuityListResponse> {
        self.call_typed(SOURCES_CONTINUITY_LIST_METHOD, &request)
            .await
    }

    pub async fn sources_continuity_explain_gap(
        &self,
        request: SourcesExplainGapRequest,
    ) -> Result<SourcesExplainGapResponse> {
        self.call_typed(SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD, &request)
            .await
    }

    pub async fn sources_readiness_get(
        &self,
        request: SourcesReadinessGetRequest,
    ) -> Result<SourcesReadinessGetResponse> {
        self.call_typed(SOURCES_READINESS_GET_METHOD, &request).await
    }

    pub async fn sources_readiness_list(
        &self,
        request: SourcesReadinessListRequest,
    ) -> Result<SourcesReadinessListResponse> {
        self.call_typed(SOURCES_READINESS_LIST_METHOD, &request)
            .await
    }

    // ==================== Document Commands ====================

    pub async fn documents_search(
        &self,
        request: DocumentsSearchRequest,
    ) -> Result<DocumentsSearchResponse> {
        self.call_typed(DOCUMENTS_SEARCH_METHOD, &request).await
    }

    pub async fn documents_get(
        &self,
        request: DocumentsGetRequest,
    ) -> Result<DocumentsGetResponse> {
        self.call_typed(DOCUMENTS_GET_METHOD, &request).await
    }

    pub async fn documents_get_chunks(
        &self,
        request: DocumentsGetChunksRequest,
    ) -> Result<DocumentsGetChunksResponse> {
        self.call_typed(DOCUMENTS_GET_CHUNKS_METHOD, &request)
            .await
    }

    // ==================== Operations Log Commands ====================

    /// Start a new operation
    pub async fn ops_start(
        &self,
        operation_type: &str,
        scope: Option<Value>,
    ) -> Result<OpsStartResponse> {
        let request = sinex_primitives::rpc::ops::OpsStartRequest {
            operation_type: operation_type.to_string(),
            scope,
        };
        self.call_typed(sinex_primitives::rpc::ops::OPS_START_METHOD, &request)
            .await
    }

    /// List operations
    pub async fn ops_list(
        &self,
        operation_type: Option<String>,
        status: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<OpsOperation>> {
        let status = status
            .map(serde_json::Value::String)
            .map(serde_json::from_value)
            .transpose()?;
        let request = sinex_primitives::rpc::ops::OpsListRequest {
            operation_type,
            status,
            limit: limit.unwrap_or(50),
        };
        let response: OpsListResponse = self
            .call_typed(sinex_primitives::rpc::ops::OPS_LIST_METHOD, &request)
            .await?;
        Ok(response.operations)
    }

    /// Get operation details
    pub async fn ops_get(&self, operation_id: &str) -> Result<OpsOperation> {
        let request = sinex_primitives::rpc::ops::OpsGetRequest {
            operation_id: operation_id.to_string(),
        };
        let response: OpsGetResponse = self
            .call_typed(sinex_primitives::rpc::ops::OPS_GET_METHOD, &request)
            .await?;
        Ok(response.operation)
    }

    /// Cancel an operation
    pub async fn ops_cancel(&self, operation_id: &str, reason: Option<String>) -> Result<()> {
        let request = sinex_primitives::rpc::ops::OpsCancelRequest {
            operation_id: operation_id.to_string(),
            reason,
        };
        self.call_typed(sinex_primitives::rpc::ops::OPS_CANCEL_METHOD, &request)
        .await?;
        Ok(())
    }

    // ==================== Audit Commands ====================

    /// Get audit trail for an operation
    pub async fn audit_get(
        &self,
        operation_id: &str,
    ) -> Result<sinex_primitives::rpc::audit::AuditGetResponse> {
        use sinex_primitives::Id;
        use sinex_primitives::events::builder::OperationMarker;
        use sinex_primitives::rpc::audit::{AUDIT_GET_METHOD, AuditGetRequest};

        let op_id = operation_id
            .parse::<Id<OperationMarker>>()
            .map_err(|e| color_eyre::eyre::eyre!("Invalid operation ID: {}", e))?;

        let request = AuditGetRequest {
            operation_id: op_id,
            after_id: None,
            limit: 100,
        };
        self.call_typed(AUDIT_GET_METHOD, &request).await
    }

    // ==================== Lifecycle Commands ====================

    /// Get lifecycle tier status
    pub async fn lifecycle_status(&self) -> Result<LifecycleStatusResponse> {
        let req = LifecycleStatusRequest::default();
        self.call_typed(LIFECYCLE_STATUS_METHOD, &req).await
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
        self.call_typed(LIFECYCLE_ARCHIVE_METHOD, &req).await
    }

    /// Restore archived events back to live
    pub async fn lifecycle_restore(
        &self,
        event_ids: Vec<String>,
        dry_run: bool,
    ) -> Result<LifecycleRestoreResponse> {
        let req = LifecycleRestoreRequest { event_ids, dry_run };
        self.call_typed(LIFECYCLE_RESTORE_METHOD, &req).await
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

    // ==================== Telemetry Commands ====================

    /// Query current health telemetry rows.
    pub async fn telemetry_current_health(
        &self,
        limit: Option<i64>,
    ) -> Result<Vec<CurrentHealthEntry>> {
        let req = TelemetryCurrentHealthRequest { limit };
        let response: TelemetryCurrentHealthResponse =
            self.call_typed(TELEMETRY_CURRENT_HEALTH_METHOD, &req).await?;
        Ok(response.entries)
    }

    /// Query current device-state telemetry rows.
    pub async fn telemetry_current_device_state(
        &self,
        limit: Option<i64>,
    ) -> Result<Vec<CurrentDeviceStateEntry>> {
        let req = TelemetryCurrentDeviceStateRequest { limit };
        let response: TelemetryCurrentDeviceStateResponse = self
            .call_typed(TELEMETRY_CURRENT_DEVICE_STATE_METHOD, &req)
            .await?;
        Ok(response.entries)
    }

    /// Query window focus telemetry aggregates.
    pub async fn telemetry_window_focus(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<WindowFocusBucket>> {
        let req = TelemetryWindowFocusRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryWindowFocusResponse =
            self.call_typed(TELEMETRY_WINDOW_FOCUS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query command frequency telemetry aggregates.
    pub async fn telemetry_command_frequency(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<CommandFrequencyEntry>> {
        let req = TelemetryCommandFrequencyRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryCommandFrequencyResponse = self
            .call_typed(TELEMETRY_COMMAND_FREQUENCY_METHOD, &req)
            .await?;
        Ok(response.entries)
    }

    /// Query file activity telemetry aggregates.
    pub async fn telemetry_file_activity(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<FileActivityEntry>> {
        let req = TelemetryFileActivityRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryFileActivityResponse =
            self.call_typed(TELEMETRY_FILE_ACTIVITY_METHOD, &req).await?;
        Ok(response.entries)
    }

    /// Query the per-source/component throughput summary (#1172 AC-8).
    pub async fn telemetry_throughput(&self) -> Result<TelemetryThroughputResponse> {
        let req = TelemetryThroughputRequest::default();
        self.call_typed(TELEMETRY_THROUGHPUT_METHOD, &req).await
    }

    /// Query recent activity summary (hardcoded lookback window, no time params).
    pub async fn telemetry_recent_activity(
        &self,
        limit: Option<i64>,
    ) -> Result<Vec<RecentActivityEntry>> {
        let req = TelemetryRecentActivityRequest { limit };
        let response: TelemetryRecentActivityResponse =
            self.call_typed(TELEMETRY_RECENT_ACTIVITY_METHOD, &req).await?;
        Ok(response.entries)
    }

    /// Query system state telemetry aggregates.
    pub async fn telemetry_system_state(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<SystemStateBucket>> {
        let req = TelemetrySystemStateRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetrySystemStateResponse =
            self.call_typed(TELEMETRY_SYSTEM_STATE_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query gateway hourly operator telemetry.
    pub async fn telemetry_gateway_stats(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<GatewayStatsBucket>> {
        let req = TelemetryGatewayStatsRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryGatewayStatsResponse =
            self.call_typed(TELEMETRY_GATEWAY_STATS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query stream hourly operator telemetry.
    pub async fn telemetry_stream_stats(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<StreamStatsBucket>> {
        let req = TelemetryStreamStatsRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryStreamStatsResponse =
            self.call_typed(TELEMETRY_STREAM_STATS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query assembly hourly operator telemetry.
    pub async fn telemetry_assembly_stats(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<AssemblyStatsBucket>> {
        let req = TelemetryAssemblyStatsRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryAssemblyStatsResponse =
            self.call_typed(TELEMETRY_ASSEMBLY_STATS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query node hourly operator telemetry.
    pub async fn telemetry_node_stats(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<NodeStatsBucket>> {
        let req = TelemetryNodeStatsRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryNodeStatsResponse =
            self.call_typed(TELEMETRY_NODE_STATS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query metric-counter hourly operator telemetry.
    pub async fn telemetry_metric_counters(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<MetricCounterBucket>> {
        let req = TelemetryMetricCountersRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryMetricCountersResponse =
            self.call_typed(TELEMETRY_METRIC_COUNTERS_METHOD, &req).await?;
        Ok(response.buckets)
    }

    /// Query ingestd hourly batch-stat telemetry.
    pub async fn telemetry_ingestd_batch_stats(
        &self,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i64>,
    ) -> Result<Vec<IngestdBatchStatsBucket>> {
        let req = TelemetryIngestdBatchStatsRequest {
            time_range: TelemetryTimeRange { from, to },
            limit,
        };
        let response: TelemetryIngestdBatchStatsResponse = self
            .call_typed(TELEMETRY_INGESTD_BATCH_STATS_METHOD, &req)
            .await?;
        Ok(response.buckets)
    }

    /// Query the latest ingestd validation snapshot.
    pub async fn telemetry_ingestd_validation(&self) -> Result<Option<IngestdValidationSnapshot>> {
        let req = TelemetryIngestdValidationRequest::default();
        let response: TelemetryIngestdValidationResponse = self
            .call_typed(TELEMETRY_INGESTD_VALIDATION_METHOD, &req)
            .await?;
        Ok(response.snapshot)
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

#[cfg(test)]
mod tests {
    // Tests unwrap optional windows after asserting inputs that must construct
    // a replay range.
    #![allow(clippy::expect_used)]
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn test_build_replay_time_window_supports_relative_inputs() -> TestResult<()> {
        let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
        let window =
            GatewayClient::build_replay_time_window(Some("24h"), None, now)?.expect("window");

        assert_eq!(window.0.format_rfc3339(), "2025-01-14T12:00:00Z");
        assert_eq!(window.1.format_rfc3339(), "2025-01-15T12:00:00Z");
        Ok(())
    }

    #[sinex_test]
    async fn test_build_replay_time_window_rejects_inverted_range() -> TestResult<()> {
        let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
        let err = GatewayClient::build_replay_time_window(
            Some("2025-01-16T00:00:00Z"),
            Some("2025-01-15T00:00:00Z"),
            now,
        )
        .expect_err("inverted replay window must fail");

        assert!(err.to_string().contains("Invalid time range"));
        Ok(())
    }

    #[sinex_test]
    async fn test_build_replay_time_window_defaults_since_from_until() -> TestResult<()> {
        let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
        let window =
            GatewayClient::build_replay_time_window(None, Some("2025-01-10T08:30:00Z"), now)?
                .expect("window");

        assert_eq!(window.0.format_rfc3339(), "2025-01-09T08:30:00Z");
        assert_eq!(window.1.format_rfc3339(), "2025-01-10T08:30:00Z");
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_preserves_split_utf8_scalars() -> TestResult<()> {
        let mut state = SseFrameState::default();
        let bytes = "data: ż\n".as_bytes();
        state.push_chunk(&bytes[..7]);
        assert!(state.try_parse_frame().is_none());
        state.push_chunk(&bytes[7..]);
        assert!(state.try_parse_frame().is_none());

        assert_eq!(state.current_data, "ż");
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_strips_only_single_optional_value_space() -> TestResult<()> {
        assert_eq!(
            parse_sse_field("data:  leading-space-preserved"),
            ("data", " leading-space-preserved")
        );
        assert_eq!(
            parse_sse_field("data: trailing-space-preserved "),
            ("data", "trailing-space-preserved ")
        );
        assert_eq!(parse_sse_field("data:"), ("data", ""));
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_dispatches_empty_data_frame() -> TestResult<()> {
        let mut state = SseFrameState::default();
        state.push_chunk(b"event: heartbeat\ndata:\n\n");

        assert!(matches!(
            state.try_parse_frame().transpose()?,
            Some(SseClientMessage::Heartbeat)
        ));
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_handles_crlf_multiline_data() -> TestResult<()> {
        let mut state = SseFrameState::default();
        state.push_chunk(b"data: first\r\ndata: second\r\n");
        assert!(state.try_parse_frame().is_none());

        assert_eq!(state.current_data, "first\nsecond");
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_ignores_control_and_unknown_fields() -> TestResult<()> {
        let mut state = SseFrameState::default();
        state.push_chunk(
            b": comment\nid: abc\nretry: 1000\nfoo: bar\nevent: unknown\ndata: {}\n\n\
              event: heartbeat\ndata: {}\n\n",
        );

        assert!(matches!(
            state.try_parse_frame().transpose()?,
            Some(SseClientMessage::Heartbeat)
        ));
        Ok(())
    }

    #[sinex_test]
    async fn sse_parser_returns_structured_error_frames() -> TestResult<()> {
        let mut state = SseFrameState::default();
        state.push_chunk(
            br#"event: error
data: {"code":"serialization_error","message":"failed to serialize SSE event payload"}

"#,
        );

        assert!(matches!(
            state.try_parse_frame().transpose()?,
            Some(SseClientMessage::Error { code, message })
                if code == "serialization_error"
                    && message == "failed to serialize SSE event payload"
        ));
        Ok(())
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
    Error {
        code: String,
        message: String,
    },
}

/// Streaming SSE frame parser over a reqwest response.
struct SseFrameParser {
    stream: reqwest::Response,
    state: SseFrameState,
}

#[derive(Default)]
struct SseFrameState {
    buffer: Vec<u8>,
    current_event: Option<String>,
    current_data: String,
    saw_data_field: bool,
}

impl SseFrameParser {
    fn new(response: reqwest::Response) -> Self {
        Self {
            stream: response,
            state: SseFrameState::default(),
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
            if let Some(msg) = this.state.try_parse_frame() {
                return Poll::Ready(Some(msg));
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

            this.state.push_chunk(&chunk);
        }
    }
}

impl SseFrameState {
    fn push_chunk(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    /// Try to parse one complete SSE frame from the internal buffer.
    fn try_parse_frame(&mut self) -> Option<Result<SseClientMessage>> {
        loop {
            // Find the next newline
            let newline_pos = self.buffer.iter().position(|byte| *byte == b'\n')?;
            let mut line_bytes = self.buffer[..newline_pos].to_vec();
            self.buffer.drain(..=newline_pos);
            if line_bytes.last() == Some(&b'\r') {
                line_bytes.pop();
            }
            let line = match String::from_utf8(line_bytes) {
                Ok(line) => line,
                Err(error) => {
                    return Some(Err(color_eyre::eyre::eyre!(
                        "SSE stream contained invalid UTF-8: {}",
                        error
                    )));
                }
            };

            if line.is_empty() {
                // Empty line = dispatch event
                if self.saw_data_field {
                    let msg = self.dispatch_frame();
                    self.reset_frame();
                    if let Some(msg) = msg {
                        return Some(Ok(msg));
                    }
                }
                continue;
            }

            if line.starts_with(':') {
                continue;
            }

            let (field, value) = parse_sse_field(&line);

            if field == "event" {
                self.current_event = Some(value.to_string());
            } else if field == "data" {
                if !self.current_data.is_empty() {
                    self.current_data.push('\n');
                }
                self.current_data.push_str(value);
                self.saw_data_field = true;
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
            "error" => {
                #[derive(Deserialize)]
                struct ErrorWrapper {
                    code: String,
                    message: String,
                }
                let error: ErrorWrapper = serde_json::from_str(&self.current_data).ok()?;
                Some(SseClientMessage::Error {
                    code: error.code,
                    message: error.message,
                })
            }
            _ => None, // Unknown event type
        }
    }

    fn reset_frame(&mut self) {
        self.current_event = None;
        self.current_data.clear();
        self.saw_data_field = false;
    }
}

fn parse_sse_field(line: &str) -> (&str, &str) {
    let (field, value) = line.split_once(':').unwrap_or((line, ""));
    let value = value.strip_prefix(' ').unwrap_or(value);
    (field, value)
}
