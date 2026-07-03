//! Mock `GatewayClient` for testing sinex-cli commands

#![allow(dead_code, clippy::expect_used, clippy::unused_async)]

use serde_json::Value;
use sinex_primitives::domain::HealthStatus;
use sinex_primitives::rpc::{
    coordination::InstanceInfo, dlq::*, replay::*, runtime::*, system::SystemHealthResponse,
};
use sinex_primitives::temporal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sinex_primitives::query::{EventQuery, EventQueryResult};
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
    RuntimeModules(Vec<InstanceInfo>),
    RuntimeStatus(RuntimeStatus),
    ReplayOperation(ReplayOperation),
    ReplayOperations(Vec<ReplayOperation>),
    DlqList(DlqListResponse),
    DlqPeek(DlqPeekResponse),
    DlqRequeue(DlqRequeueResponse),
    DlqPurge(DlqPurgeResponse),
    QueryResult(EventQueryResult),
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
            ComponentHealthReport, ComponentsHealth, ReplayControlHealth,
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
            .unwrap_or(SystemHealthResponse {
                status: HealthStatus::Healthy,
                healthy: true,
                serving: true,
                degradation_reasons: Vec::new(),
                components: ComponentsHealth {
                    database: ComponentHealthReport {
                        status: HealthStatus::Healthy,
                        connected: true,
                        latency_ms: None,
                        detail: None,
                        attributes: Default::default(),
                    },
                    nats: ComponentHealthReport {
                        status: HealthStatus::Healthy,
                        connected: true,
                        latency_ms: None,
                        detail: None,
                        attributes: Default::default(),
                    },
                    raw_ingest_dlq: ComponentHealthReport {
                        status: HealthStatus::Healthy,
                        connected: true,
                        latency_ms: None,
                        detail: Some("raw-ingest DLQ empty".to_string()),
                        attributes: Default::default(),
                    },
                    replay_control: ReplayControlHealth {
                        status: HealthStatus::Healthy,
                        enabled: true,
                        connected: true,
                        last_error: None,
                    },
                    sse_confirmation: ComponentHealthReport {
                        status: HealthStatus::Healthy,
                        connected: true,
                        latency_ms: None,
                        detail: None,
                        attributes: Default::default(),
                    },
                },
            }))
    }

    pub(crate) async fn list_runtime(&self) -> Result<Vec<InstanceInfo>> {
        self.record_call("list_runtime", vec![]);
        Ok(self
            .get_response("list_runtime")
            .and_then(|r| {
                if let MockResponse::RuntimeModules(modules) = r {
                    Some(modules)
                } else {
                    None
                }
            })
            .unwrap_or_default())
    }

    pub(crate) async fn runtime_status(&self, module_name: &str) -> Result<RuntimeStatus> {
        use sinex_primitives::domain::{ModuleName, ModuleState};

        self.record_call("runtime_status", vec![module_name.to_string()]);
        Ok(self
            .get_response("runtime_status")
            .and_then(|r| {
                if let MockResponse::RuntimeStatus(status) = r {
                    Some(status)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| RuntimeStatus {
                module_name: ModuleName::new(module_name),
                state: ModuleState::Running,
                last_heartbeat: None,
                processing_horizon: None,
            }))
    }

    pub(crate) async fn drain_runtime(
        &self,
        module_name: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        self.record_call(
            "drain_runtime",
            vec![module_name.to_string(), reason.unwrap_or("").to_string()],
        );
        Ok(())
    }

    pub(crate) async fn resume_runtime(&self, module_name: &str) -> Result<()> {
        self.record_call("resume_runtime", vec![module_name.to_string()]);
        Ok(())
    }

    pub(crate) async fn set_runtime_horizon(&self, module_name: &str, horizon: &str) -> Result<()> {
        self.record_call(
            "set_runtime_horizon",
            vec![module_name.to_string(), horizon.to_string()],
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
                    source_name: "test-source".to_string(),
                    time_window: None,
                    material_filter: None,
                    filters: HashMap::new(),
                    source_id: None,
                    source_material_id: None,
                    parser_id: None,
                    parser_version: None,
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
                executor_module: None,
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
                pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
                resource_pressure: sinex_primitives::rpc::dlq::DlqPressureSignal {
                    pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
                    runtime_action: sinex_primitives::RuntimePressureAction::Admit,
                    pending_messages: 0,
                    pending_bytes: 0,
                    retry_batch_size: 100,
                    recommended_action: "none".to_string(),
                    reason: "raw-ingest DLQ is empty".to_string(),
                },
                pending_sequence_span: 0,
                recommended_action: "none".to_string(),
                action_reason: "raw-ingest DLQ is empty".to_string(),
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
            .unwrap_or_else(|| DlqPeekResponse::from_messages(Vec::new())))
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
                operation_id: "00000000-0000-0000-0000-000000000000".to_string(),
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
                operation_id: "00000000-0000-0000-0000-000000000000".to_string(),
            }))
    }

    pub(crate) async fn query_events(&self, query: EventQuery) -> Result<EventQueryResult> {
        self.record_call("query_events", vec![format!("{query:?}")]);
        Ok(self
            .get_response("query_events")
            .and_then(|r| {
                if let MockResponse::QueryResult(result) = r {
                    Some(result)
                } else {
                    None
                }
            })
            .unwrap_or(EventQueryResult::Events {
                events: vec![],
                next_cursor: None,
                total_estimate: None,
            }))
    }
}

impl Default for MockGatewayClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "mock_client_test.rs"]
mod tests;
