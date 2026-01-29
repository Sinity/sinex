//! Coordination types

use crate::domain::{HostName, InstanceId, NodeType};
use crate::temporal::Timestamp;
use serde::{Deserialize, Serialize};

/// Instance info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub instance_id: InstanceId,
    pub node_type: NodeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<HostName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<Timestamp>,
    pub is_leader: bool,
}

// ─────────────────────────────────────────────────────────────
// coordination.list_instances
// ─────────────────────────────────────────────────────────────

/// Request: coordination.list_instances
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListInstancesRequest {
    /// Filter by node type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_type: Option<NodeType>,
}

/// Response: coordination.list_instances
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListInstancesResponse {
    pub instances: Vec<InstanceInfo>,
}

// ─────────────────────────────────────────────────────────────
// coordination.get_leader
// ─────────────────────────────────────────────────────────────

/// Request: coordination.get_leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLeaderRequest {
    pub node_type: NodeType,
}

/// Response: coordination.get_leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLeaderResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<InstanceInfo>,
}

// ─────────────────────────────────────────────────────────────
// coordination.instance_health
// ─────────────────────────────────────────────────────────────

/// Request: coordination.instance_health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceHealthRequest {
    pub instance_id: InstanceId,
}

/// Structured error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,
}

impl std::fmt::Display for ErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref code) = self.code {
            write!(f, "[{}] {}", code, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

/// Response: coordination.instance_health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceHealthResponse {
    pub instance: InstanceInfo,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ErrorInfo>,
}
