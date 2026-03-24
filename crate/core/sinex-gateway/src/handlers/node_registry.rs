//! Node status and lifecycle handlers
//!
//! Provides RPC methods for querying node status, health, and managing
//! node lifecycle events (heartbeats, status changes).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::domain::{NodeName, NodeType};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;
use tracing::info;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Request/Response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct NodesListActiveRequest {}

#[derive(Debug, Serialize)]
pub struct NodesListActiveResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub version: String,
    pub description: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
pub struct NodesHealthRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

fn default_stale_after_secs() -> u64 {
    300 // 5 minutes
}

#[derive(Debug, Serialize)]
pub struct NodesHealthResponse {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_nodes: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
pub struct NodesHeartbeatRequest {
    pub node_name: NodeName,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct NodesHeartbeatResponse {
    pub updated: bool,
}

#[derive(Debug, Deserialize)]
pub struct NodesMarkInactiveRequest {
    pub node_name: NodeName,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct NodesMarkInactiveResponse {
    pub marked: bool,
}

// ─── Handlers ───────────────────────────────────────────────────────────

/// List all active nodes.
///
/// Returns nodes where status = 'active' and `last_heartbeat_at` is recent
/// (within the default 5-minute stale threshold).
pub async fn handle_nodes_list_active(pool: &PgPool, params: Value) -> Result<Value> {
    let _request: NodesListActiveRequest =
        serde_json::from_value(params).unwrap_or(NodesListActiveRequest {});

    let manifests = pool
        .state()
        .get_active_nodes()
        .await
        .map_err(|e| SinexError::database("Failed to list active nodes").with_std_error(&e))?;

    let nodes = manifests
        .into_iter()
        .map(|m| NodeInfo {
            node_name: m.node_name,
            node_type: m.node_type,
            version: m.version,
            description: m.description,
            status: m.status,
            last_heartbeat_at: m.last_heartbeat_at,
        })
        .collect();

    let response = NodesListActiveResponse { nodes };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize nodes list response").with_std_error(&e)
    })
}

/// Get node health summary.
///
/// Returns counts of active/inactive nodes and the oldest heartbeat timestamp.
/// The `stale_after_secs` parameter determines what is considered "active" (default: 300 seconds = 5 minutes).
pub async fn handle_nodes_health(pool: &PgPool, params: Value) -> Result<Value> {
    let request: NodesHealthRequest =
        serde_json::from_value(params).unwrap_or(NodesHealthRequest {
            stale_after_secs: 300,
        });

    let stale_after = Duration::from_secs(request.stale_after_secs);
    let health = pool
        .state()
        .get_node_health(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to get node health").with_std_error(&e))?;

    let response = NodesHealthResponse {
        active_count: health.active_count,
        inactive_count: health.inactive_count,
        unique_nodes: health.unique_nodes,
        oldest_heartbeat: health.oldest_heartbeat,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize node health response").with_std_error(&e)
    })
}

/// Update a node's heartbeat timestamp.
///
/// Called by a running node to indicate it is alive and actively executing.
/// Sets the status to 'active' and updates `last_heartbeat_at` to `NOW()`.
pub async fn handle_nodes_heartbeat(pool: &PgPool, params: Value) -> Result<Value> {
    let request: NodesHeartbeatRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid nodes heartbeat request").with_std_error(&e)
    })?;

    let updated = pool
        .state()
        .update_node_heartbeat_for_version(&request.node_name, &request.version)
        .await
        .map_err(|e| SinexError::database("Failed to update node heartbeat").with_std_error(&e))?;

    info!(
        node_name = %request.node_name,
        version = %request.version,
        "Updated node heartbeat"
    );

    let response = NodesHeartbeatResponse { updated };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize heartbeat response").with_std_error(&e)
    })
}

/// Mark a node as inactive.
///
/// Called when a node is known to have stopped (graceful shutdown, detected stale, etc).
/// Sets the status to 'inactive'.
pub async fn handle_nodes_mark_inactive(pool: &PgPool, params: Value) -> Result<Value> {
    let request: NodesMarkInactiveRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid nodes mark inactive request").with_std_error(&e)
    })?;

    let marked = pool
        .state()
        .mark_node_inactive_for_version(&request.node_name, &request.version)
        .await
        .map_err(|e| SinexError::database("Failed to mark node inactive").with_std_error(&e))?;

    info!(
        node_name = %request.node_name,
        version = %request.version,
        "Marked node as inactive"
    );

    let response = NodesMarkInactiveResponse { marked };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize mark inactive response").with_std_error(&e)
    })
}
