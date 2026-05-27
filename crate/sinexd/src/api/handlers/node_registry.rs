//! Node runtime status handlers.
//!
//! These surfaces expose live node presence and aggregate health for operators.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::nodes::{
    NodeHeartbeatSource, NodeInfo, NodesHealthRequest, NodesHealthResponse, NodesListActiveRequest,
    NodesListActiveResponse,
};
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Handlers ───────────────────────────────────────────────────────────

fn parse_node_heartbeat_source(value: &str) -> Result<NodeHeartbeatSource> {
    match value {
        "run" => Ok(NodeHeartbeatSource::Run),
        "manifest" => Ok(NodeHeartbeatSource::Manifest),
        other => Err(SinexError::processing(format!(
            "Unknown node heartbeat source '{other}'"
        ))),
    }
}

/// List live node presence.
///
/// Returns active run rows when available and falls back to manifest-only
/// heartbeats for services that do not yet register runs.
pub async fn handle_nodes_list_active(
    pool: &PgPool,
    request: NodesListActiveRequest,
) -> Result<NodesListActiveResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);

    let live_nodes = pool
        .state()
        .list_live_node_presence(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to list active nodes").with_std_error(&e))?;

    let nodes = live_nodes
        .into_iter()
        .filter_map(
            |node| match parse_node_heartbeat_source(&node.heartbeat_source) {
                Ok(heartbeat_source) => Some(NodeInfo {
                    node_name: node.node_name,
                    node_type: node.node_type,
                    version: node.version,
                    description: node.description,
                    service_name: node.service_name,
                    instance_id: node.instance_id,
                    source_run_id: node.source_run_id,
                    host: node.host,
                    status: node.status,
                    last_heartbeat_at: node.last_heartbeat_at,
                    started_at: node.started_at,
                    heartbeat_source,
                }),
                Err(error) => {
                    tracing::warn!(
                        service_name = ?node.service_name,
                        heartbeat_source = ?node.heartbeat_source,
                        error = %error,
                        "Skipping node with unrecognised heartbeat_source in listing"
                    );
                    None
                }
            },
        )
        .collect::<Vec<_>>();

    Ok(NodesListActiveResponse { nodes })
}

/// Get node health summary.
///
/// Returns unique-node counts plus the number of concrete active runs.
pub async fn handle_nodes_health(
    pool: &PgPool,
    request: NodesHealthRequest,
) -> Result<NodesHealthResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let health = pool
        .state()
        .get_node_health(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to get node health").with_std_error(&e))?;

    Ok(NodesHealthResponse {
        active_count: health.active_count,
        inactive_count: health.inactive_count,
        unique_nodes: health.unique_nodes,
        active_run_count: health.active_run_count,
        oldest_heartbeat: health.oldest_heartbeat,
    })
}
