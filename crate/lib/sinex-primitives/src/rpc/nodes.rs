//! Node operations types

use crate::Timestamp;
use crate::domain::{NodeId, NodeState, OperationStatus};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use serde::{Deserialize, Serialize};

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
