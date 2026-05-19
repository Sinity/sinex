//! Node operations types

use crate::Timestamp;
use crate::Uuid;
use crate::domain::{NodeId, NodeName, NodeState, NodeType, OperationStatus};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};

pub const NODES_LIST_METHOD: RpcMethod<NodesListRequest, NodesListResponse> = RpcMethod::new(
    methods::NODES_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Nodes,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const NODES_LIST_ACTIVE_METHOD: RpcMethod<NodesListActiveRequest, NodesListActiveResponse> =
    RpcMethod::new(
        methods::NODES_LIST_ACTIVE,
        RpcRole::ReadOnly,
        RpcDomain::Nodes,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const NODES_HEALTH_METHOD: RpcMethod<NodesHealthRequest, NodesHealthResponse> = RpcMethod::new(
    methods::NODES_HEALTH,
    RpcRole::ReadOnly,
    RpcDomain::Nodes,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

pub const NODES_DRAIN_METHOD: RpcMethod<NodeDrainRequest, NodeDrainResponse> = RpcMethod::new(
    methods::NODES_DRAIN,
    RpcRole::Write,
    RpcDomain::Nodes,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const NODES_RESUME_METHOD: RpcMethod<NodeResumeRequest, NodeResumeResponse> = RpcMethod::new(
    methods::NODES_RESUME,
    RpcRole::Write,
    RpcDomain::Nodes,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const NODES_SET_HORIZON_METHOD: RpcMethod<NodeSetHorizonRequest, NodeSetHorizonResponse> =
    RpcMethod::new(
        methods::NODES_SET_HORIZON,
        RpcRole::Write,
        RpcDomain::Nodes,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

/// Node status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub state: NodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_horizon: Option<Timestamp>,
}

// ─────────────────────────────────────────────────────────────
// nodes.list
// ─────────────────────────────────────────────────────────────

/// Request: nodes.list (no params required)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodesListRequest {}

/// Response: nodes.list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesListResponse {
    pub nodes: Vec<NodeStatus>,
}

fn default_stale_after_secs() -> u64 {
    300
}

// ─────────────────────────────────────────────────────────────
// nodes.list_active
// ─────────────────────────────────────────────────────────────

/// Request: nodes.list_active
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesListActiveRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

impl Default for NodesListActiveRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
        }
    }
}

/// Response: nodes.list_active
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesListActiveResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHeartbeatSource {
    Run,
    Manifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub version: String,
    pub description: Option<String>,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub source_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub heartbeat_source: NodeHeartbeatSource,
}

// ─────────────────────────────────────────────────────────────
// nodes.health
// ─────────────────────────────────────────────────────────────

/// Request: nodes.health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesHealthRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

impl Default for NodesHealthRequest {
    fn default() -> Self {
        Self {
            stale_after_secs: default_stale_after_secs(),
        }
    }
}

/// Response: nodes.health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesHealthResponse {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_nodes: i64,
    pub active_run_count: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

// ─────────────────────────────────────────────────────────────
// nodes.drain
// ─────────────────────────────────────────────────────────────

/// Request: nodes.drain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDrainRequest {
    pub node_id: NodeId,
    /// Optional reason for draining
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response: nodes.drain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDrainResponse {
    pub status: OperationStatus,
    pub node_id: NodeId,
}

// ─────────────────────────────────────────────────────────────
// nodes.resume
// ─────────────────────────────────────────────────────────────

/// Request: nodes.resume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResumeRequest {
    pub node_id: NodeId,
}

/// Response: nodes.resume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResumeResponse {
    pub status: OperationStatus,
    pub node_id: NodeId,
}

// ─────────────────────────────────────────────────────────────
// nodes.set_horizon
// ─────────────────────────────────────────────────────────────

/// Request: `nodes.set_horizon`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSetHorizonRequest {
    pub node_id: NodeId,
    /// Horizon timestamp
    pub horizon: Timestamp,
}

/// Response: `nodes.set_horizon`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSetHorizonResponse {
    pub status: OperationStatus,
    pub node_id: NodeId,
    pub horizon: Timestamp,
}
