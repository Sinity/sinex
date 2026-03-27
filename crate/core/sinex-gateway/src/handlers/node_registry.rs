//! Node runtime status handlers.
//!
//! These surfaces expose live node presence and aggregate health for operators.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{NodeName, NodeType};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Request/Response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Serialize)]
pub struct NodesListActiveResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHeartbeatSource {
    Run,
    Manifest,
}

#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub version: String,
    pub description: Option<String>,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub node_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub heartbeat_source: NodeHeartbeatSource,
}

#[derive(Debug, Deserialize)]
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

fn default_stale_after_secs() -> u64 {
    300 // 5 minutes
}

#[derive(Debug, Serialize)]
pub struct NodesHealthResponse {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_nodes: i64,
    pub active_run_count: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

// ─── Handlers ───────────────────────────────────────────────────────────

impl NodeHeartbeatSource {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "run" => Ok(Self::Run),
            "manifest" => Ok(Self::Manifest),
            other => Err(SinexError::processing(format!(
                "Unknown node heartbeat source '{other}'"
            ))),
        }
    }
}

/// List live node presence.
///
/// Returns active run rows when available and falls back to manifest-only
/// heartbeats for services that do not yet register runs.
pub async fn handle_nodes_list_active(pool: &PgPool, params: Value) -> Result<Value> {
    let request: NodesListActiveRequest = super::parse_default_on_null(params).map_err(|e| {
        SinexError::serialization("Invalid nodes list active request").with_std_error(&e)
    })?;
    let stale_after = Duration::from_secs(request.stale_after_secs);

    let live_nodes = pool
        .state()
        .list_live_node_presence(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to list active nodes").with_std_error(&e))?;

    let nodes = live_nodes
        .into_iter()
        .map(|node| {
            Ok(NodeInfo {
                node_name: node.node_name,
                node_type: node.node_type,
                version: node.version,
                description: node.description,
                service_name: node.service_name,
                instance_id: node.instance_id,
                node_run_id: node.node_run_id,
                host: node.host,
                status: node.status,
                last_heartbeat_at: node.last_heartbeat_at,
                started_at: node.started_at,
                heartbeat_source: NodeHeartbeatSource::parse(&node.heartbeat_source)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let response = NodesListActiveResponse { nodes };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize nodes list response").with_std_error(&e)
    })
}

/// Get node health summary.
///
/// Returns unique-node counts plus the number of concrete active runs.
pub async fn handle_nodes_health(pool: &PgPool, params: Value) -> Result<Value> {
    let request: NodesHealthRequest = super::parse_default_on_null(params).map_err(|e| {
        SinexError::serialization("Invalid nodes health request").with_std_error(&e)
    })?;

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
        active_run_count: health.active_run_count,
        oldest_heartbeat: health.oldest_heartbeat,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize node health response").with_std_error(&e)
    })
}
