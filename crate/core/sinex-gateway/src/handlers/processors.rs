//! Processor status and lifecycle handlers
//!
//! Provides RPC methods for querying processor status, health, and managing
//! processor lifecycle events (heartbeats, status changes).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::domain::NodeName;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::SinexError;
use sqlx::PgPool;
use std::time::Duration;
use tracing::info;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Request/Response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ProcessorsListActiveRequest {}

#[derive(Debug, Serialize)]
pub struct ProcessorsListActiveResponse {
    pub processors: Vec<ProcessorInfo>,
}

#[derive(Debug, Serialize)]
pub struct ProcessorInfo {
    pub node_name: String,
    pub node_type: String,
    pub version: String,
    pub description: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
pub struct ProcessorsHealthRequest {
    #[serde(default = "default_stale_after_secs")]
    pub stale_after_secs: u64,
}

fn default_stale_after_secs() -> u64 {
    300 // 5 minutes
}

#[derive(Debug, Serialize)]
pub struct ProcessorsHealthResponse {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_processors: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
pub struct ProcessorsHeartbeatRequest {
    pub node_name: NodeName,
}

#[derive(Debug, Serialize)]
pub struct ProcessorsHeartbeatResponse {
    pub updated: bool,
}

#[derive(Debug, Deserialize)]
pub struct ProcessorsMarkInactiveRequest {
    pub node_name: NodeName,
}

#[derive(Debug, Serialize)]
pub struct ProcessorsMarkInactiveResponse {
    pub marked: bool,
}

// ─── Handlers ───────────────────────────────────────────────────────────

/// List all active processors.
///
/// Returns processors where status = 'active' and `last_heartbeat_at` is recent
/// (within the default 5-minute stale threshold).
pub async fn handle_processors_list_active(pool: &PgPool, params: Value) -> Result<Value> {
    let _request: ProcessorsListActiveRequest =
        serde_json::from_value(params).unwrap_or(ProcessorsListActiveRequest {});

    let manifests = pool
        .state()
        .get_active_nodes()
        .await
        .map_err(|e| SinexError::database("Failed to list active nodes").with_std_error(&e))?;

    let processors = manifests
        .into_iter()
        .map(|m| ProcessorInfo {
            node_name: m.node_name,
            node_type: m.node_type,
            version: m.version,
            description: m.description,
            status: m.status,
            last_heartbeat_at: m.last_heartbeat_at,
        })
        .collect();

    let response = ProcessorsListActiveResponse { processors };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize processors list response").with_std_error(&e)
    })
}

/// Get processor health summary.
///
/// Returns counts of active/inactive processors and the oldest heartbeat timestamp.
/// The `stale_after_secs` parameter determines what is considered "active" (default: 300 seconds = 5 minutes).
pub async fn handle_processors_health(pool: &PgPool, params: Value) -> Result<Value> {
    let request: ProcessorsHealthRequest =
        serde_json::from_value(params).unwrap_or(ProcessorsHealthRequest {
            stale_after_secs: 300,
        });

    let stale_after = Duration::from_secs(request.stale_after_secs);
    let health = pool
        .state()
        .get_processor_health(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to get processor health").with_std_error(&e))?;

    let response = ProcessorsHealthResponse {
        active_count: health.active_count,
        inactive_count: health.inactive_count,
        unique_processors: health.unique_processors,
        oldest_heartbeat: health.oldest_heartbeat,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize processor health response")
            .with_std_error(&e)
    })
}

/// Update a processor's heartbeat timestamp.
///
/// Called by a running processor to indicate it is alive and actively executing.
/// Sets the status to 'active' and updates `last_heartbeat_at` to `NOW()`.
pub async fn handle_processors_heartbeat(pool: &PgPool, params: Value) -> Result<Value> {
    let request: ProcessorsHeartbeatRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid processors heartbeat request").with_std_error(&e)
    })?;

    pool.state()
        .update_node_heartbeat(&request.node_name)
        .await
        .map_err(|e| SinexError::database("Failed to update node heartbeat").with_std_error(&e))?;

    info!(
        node_name = %request.node_name,
        "Updated node heartbeat"
    );

    let response = ProcessorsHeartbeatResponse { updated: true };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize heartbeat response").with_std_error(&e)
    })
}

/// Mark a processor as inactive.
///
/// Called when a processor is known to have stopped (graceful shutdown, detected stale, etc).
/// Sets the status to 'inactive'.
pub async fn handle_processors_mark_inactive(pool: &PgPool, params: Value) -> Result<Value> {
    let request: ProcessorsMarkInactiveRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid processors mark inactive request").with_std_error(&e)
    })?;

    pool.state()
        .mark_node_inactive(&request.node_name)
        .await
        .map_err(|e| SinexError::database("Failed to mark node inactive").with_std_error(&e))?;

    info!(
        node_name = %request.node_name,
        "Marked node as inactive"
    );

    let response = ProcessorsMarkInactiveResponse { marked: true };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize mark inactive response").with_std_error(&e)
    })
}
