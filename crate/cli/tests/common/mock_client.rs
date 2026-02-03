//! Mock `GatewayClient` for testing sinex-cli commands

#![allow(dead_code)]

use serde_json::Value;
use sinex_primitives::rpc::{
    coordination::InstanceInfo, dlq::*, nodes::*, replay::*, system::SystemHealthResponse,
};
use sinex_primitives::temporal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sinexctl::model::search::{SearchQuery, SearchResult};
use sinexctl::Result;

/// Mock gateway client that records method calls and returns preset responses
#[derive(Clone)]
pub struct MockGatewayClient {
    inner: Arc<Mutex<MockClientInner>>,
}

struct MockClientInner {
    /// Recorded method calls (`method_name`, args)
    calls: Vec<(String, Vec<String>)>,
    /// Preset responses for specific methods
    responses: HashMap<String, MockResponse>,
}

#[derive(Clone)]
pub enum MockResponse {
    String(String),
    Health(SystemHealthResponse),
    Nodes(Vec<InstanceInfo>),
    NodeStatus(NodeStatus),
    ReplayOperation(ReplayOperation),
    ReplayOperations(Vec<ReplayOperation>),
    DlqList(DlqListResponse),
    DlqPeek(DlqPeekResponse),
    DlqRequeue(DlqRequeueResponse),
    DlqPurge(DlqPurgeResponse),
    SearchResults(Vec<SearchResult>),
    Value(Value),
}

impl MockGatewayClient {
    /// Create a new mock client with default responses
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockClientInner {
                calls: Vec::new(),
                responses: HashMap::new(),
            })),
        }
    }

    /// Set a mock response for a specific method
    pub(crate) fn set_response(&self, method: &str, response: MockResponse) {
        let mut inner = self
            .inner
            .lock()
            .expect("failed to acquire lock on mock client");
        inner.responses.insert(method.to_string(), response);
    }

    /// Get the list of recorded method calls
    pub(crate) fn get_calls(&self) -> Vec<(String, Vec<String>)> {
        let inner = self
            .inner
            .lock()
            .expect("failed to acquire lock on mock client");
        inner.calls.clone()
    }

    /// Clear all recorded calls
    pub(crate) fn clear_calls(&self) {
        let mut inner = self
            .inner
            .lock()
            .expect("failed to acquire lock on mock client");
        inner.calls.clear();
    }

    /// Record a method call
    fn record_call(&self, method: &str, args: Vec<String>) {
        let mut inner = self
            .inner
            .lock()
            .expect("failed to acquire lock on mock client");
        inner.calls.push((method.to_string(), args));
    }

    /// Get a preset response or return a default
    fn get_response(&self, method: &str) -> Option<MockResponse> {
        let inner = self
            .inner
            .lock()
            .expect("failed to acquire lock on mock client");
        inner.responses.get(method).cloned()
    }

    // Mock implementations of GatewayClient methods

    pub(crate) async fn ping(&self) -> Result<String> {
        self.record_call("ping", vec![]);
        Ok(self
            .get_response("ping")
            .and_then(|r| {
                if let MockResponse::String(s) = r {
                    Some(s)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "pong".to_string()))
    }

    pub(crate) async fn version(&self) -> Result<String> {
        self.record_call("version", vec![]);
        Ok(self
            .get_response("version")
            .and_then(|r| {
                if let MockResponse::String(s) = r {
                    Some(s)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "0.4.2".to_string()))
    }

    pub(crate) async fn health(&self) -> Result<SystemHealthResponse> {
        use sinex_primitives::rpc::system::{
            ComponentHealth, ComponentsHealth, ReplayControlHealth,
        };

        self.record_call("health", vec![]);
        Ok(self
            .get_response("health")
            .and_then(|r| {
                if let MockResponse::Health(h) = r {
                    Some(h)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| SystemHealthResponse {
                status: "healthy".to_string(),
                components: ComponentsHealth {
                    database: ComponentHealth {
                        status: "healthy".to_string(),
                        connected: true,
                    },
                    nats: ComponentHealth {
                        status: "healthy".to_string(),
                        connected: true,
                    },
                    replay_control: ReplayControlHealth {
                        status: "healthy".to_string(),
                        enabled: true,
                        bypass_allowed: false,
                        bypass_active: false,
                        connected: true,
                        last_error: None,
                    },
                },
            }))
    }

    pub(crate) async fn list_nodes(&self) -> Result<Vec<InstanceInfo>> {
        self.record_call("list_nodes", vec![]);
        Ok(self
            .get_response("list_nodes")
            .and_then(|r| {
                if let MockResponse::Nodes(nodes) = r {
                    Some(nodes)
                } else {
                    None
                }
            })
            .unwrap_or_default())
    }

    pub(crate) async fn node_status(&self, node_id: &str) -> Result<NodeStatus> {
        use sinex_primitives::domain::{NodeId, NodeState};

        self.record_call("node_status", vec![node_id.to_string()]);
        Ok(self
            .get_response("node_status")
            .and_then(|r| {
                if let MockResponse::NodeStatus(status) = r {
                    Some(status)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| NodeStatus {
                node_id: NodeId::new(node_id),
                state: NodeState::Running,
                last_heartbeat: None,
                processing_horizon: None,
            }))
    }

    pub(crate) async fn drain_node(&self, node_id: &str, reason: Option<&str>) -> Result<()> {
        self.record_call(
            "drain_node",
            vec![node_id.to_string(), reason.unwrap_or("").to_string()],
        );
        Ok(())
    }

    pub(crate) async fn resume_node(&self, node_id: &str) -> Result<()> {
        self.record_call("resume_node", vec![node_id.to_string()]);
        Ok(())
    }

    pub(crate) async fn set_node_horizon(&self, node_id: &str, horizon: &str) -> Result<()> {
        self.record_call(
            "set_node_horizon",
            vec![node_id.to_string(), horizon.to_string()],
        );
        Ok(())
    }

    pub(crate) async fn replay_list(&self) -> Result<Vec<ReplayOperation>> {
        self.record_call("replay_list", vec![]);
        Ok(self
            .get_response("replay_list")
            .and_then(|r| {
                if let MockResponse::ReplayOperations(ops) = r {
                    Some(ops)
                } else {
                    None
                }
            })
            .unwrap_or_default())
    }

    pub(crate) async fn replay_status(&self, operation_id: &str) -> Result<ReplayOperation> {
        use sinex_primitives::rpc::replay::{ReplayCheckpoint, ReplayScope, ReplayState};
        use std::collections::HashMap;

        self.record_call("replay_status", vec![operation_id.to_string()]);
        Ok(self
            .get_response("replay_status")
            .and_then(|r| {
                if let MockResponse::ReplayOperation(op) = r {
                    Some(op)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ReplayOperation {
                operation_id: operation_id.to_string(),
                state: ReplayState::Planning,
                scope: ReplayScope {
                    processor_id: "test-processor".to_string(),
                    time_window: None,
                    material_filter: None,
                    filters: HashMap::new(),
                },
                preview_summary: None,
                checkpoint: ReplayCheckpoint {
                    processed_events: 0,
                    total_events: 0,
                    last_event_id: None,
                    batch_number: 0,
                    savepoint_id: None,
                    updated_at: temporal::now().format_rfc3339(),
                },
                actor: "test-actor".to_string(),
                created_at: temporal::now().format_rfc3339(),
                approved_by: None,
                approved_at: None,
                executor_node: None,
                started_at: None,
                finished_at: None,
                outcome: None,
                error_details: None,
            }))
    }

    pub(crate) async fn dlq_list(&self) -> Result<DlqListResponse> {
        self.record_call("dlq_list", vec![]);
        Ok(self
            .get_response("dlq_list")
            .and_then(|r| {
                if let MockResponse::DlqList(resp) = r {
                    Some(resp)
                } else {
                    None
                }
            })
            .unwrap_or(DlqListResponse {
                total_messages: 0,
                total_bytes: 0,
                first_seq: 0,
                last_seq: 0,
            }))
    }

    pub(crate) async fn dlq_peek(&self, limit: Option<usize>) -> Result<DlqPeekResponse> {
        self.record_call("dlq_peek", vec![format!("{limit:?}")]);
        Ok(self
            .get_response("dlq_peek")
            .and_then(|r| {
                if let MockResponse::DlqPeek(resp) = r {
                    Some(resp)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| DlqPeekResponse {
                messages: Vec::new(),
            }))
    }

    pub(crate) async fn dlq_requeue(&self, event_ids: Vec<String>) -> Result<DlqRequeueResponse> {
        self.record_call("dlq_requeue", event_ids);
        Ok(self
            .get_response("dlq_requeue")
            .and_then(|r| {
                if let MockResponse::DlqRequeue(resp) = r {
                    Some(resp)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| DlqRequeueResponse {
                status: "success".to_string(),
                requeued_count: 0,
            }))
    }

    pub(crate) async fn dlq_purge(&self, confirm: bool) -> Result<DlqPurgeResponse> {
        self.record_call("dlq_purge", vec![confirm.to_string()]);
        Ok(self
            .get_response("dlq_purge")
            .and_then(|r| {
                if let MockResponse::DlqPurge(resp) = r {
                    Some(resp)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| DlqPurgeResponse {
                status: "success".to_string(),
                purged_count: 0,
            }))
    }

    pub(crate) async fn search_events(&self, query: SearchQuery) -> Result<Vec<SearchResult>> {
        self.record_call("search_events", vec![format!("{query:?}")]);
        Ok(self
            .get_response("search_events")
            .and_then(|r| {
                if let MockResponse::SearchResults(results) = r {
                    Some(results)
                } else {
                    None
                }
            })
            .unwrap_or_default())
    }
}

impl Default for MockGatewayClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_client_ping() {
        let client = MockGatewayClient::new();
        let result = client.ping().await.expect("ping request failed");
        assert_eq!(result, "pong");

        let calls = client.get_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "ping");
    }

    #[tokio::test]
    async fn test_mock_client_custom_response() {
        let client = MockGatewayClient::new();
        client.set_response("ping", MockResponse::String("custom_pong".to_string()));

        let result = client.ping().await.expect("ping request failed");
        assert_eq!(result, "custom_pong");
    }

    #[tokio::test]
    async fn test_mock_client_records_calls() {
        let client = MockGatewayClient::new();

        client.ping().await.expect("ping request failed");
        client.version().await.expect("version request failed");
        client.health().await.expect("health request failed");

        let calls = client.get_calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].0, "ping");
        assert_eq!(calls[1].0, "version");
        assert_eq!(calls[2].0, "health");
    }

    #[tokio::test]
    async fn test_mock_client_clear_calls() {
        let client = MockGatewayClient::new();

        client.ping().await.expect("ping request failed");
        assert_eq!(client.get_calls().len(), 1);

        client.clear_calls();
        assert_eq!(client.get_calls().len(), 0);
    }

    #[tokio::test]
    async fn test_mock_client_node_operations() {
        let client = MockGatewayClient::new();

        client
            .drain_node("node-1", Some("maintenance"))
            .await
            .expect("drain_node request failed");
        client
            .resume_node("node-1")
            .await
            .expect("resume_node request failed");
        client
            .set_node_horizon("node-1", "2024-01-01T00:00:00Z")
            .await
            .expect("set_node_horizon request failed");

        let calls = client.get_calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].0, "drain_node");
        assert_eq!(calls[1].0, "resume_node");
        assert_eq!(calls[2].0, "set_node_horizon");
    }
}
