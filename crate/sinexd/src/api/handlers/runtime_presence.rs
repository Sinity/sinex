//! runtime module runtime status handlers.
//!
//! These surfaces expose live runtime module presence and aggregate health for operators.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::runtime::{
    RuntimeHealthRequest, RuntimeHealthResponse, RuntimeHeartbeatSource, RuntimeInfo,
    RuntimeListActiveRequest, RuntimeListActiveResponse,
};
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Handlers ───────────────────────────────────────────────────────────

fn parse_runtime_heartbeat_source(value: &str) -> Result<RuntimeHeartbeatSource> {
    match value {
        "run" => Ok(RuntimeHeartbeatSource::Run),
        "manifest" => Ok(RuntimeHeartbeatSource::Manifest),
        other => Err(SinexError::processing(format!(
            "Unknown runtime heartbeat source '{other}'"
        ))),
    }
}

/// List live runtime module presence.
///
/// Returns concrete active run rows. Manifest-only heartbeat rows are not
/// treated as runtime liveness evidence.
pub async fn handle_runtime_list_active(
    pool: &PgPool,
    request: RuntimeListActiveRequest,
) -> Result<RuntimeListActiveResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);

    let live_modules = pool
        .state()
        .list_live_runtime_presence(stale_after)
        .await
        .map_err(|e| {
            SinexError::database("Failed to list active runtime modules").with_std_error(&e)
        })?;

    let modules = live_modules
        .into_iter()
        .filter_map(
            |module| match parse_runtime_heartbeat_source(&module.heartbeat_source) {
                Ok(heartbeat_source) => Some(RuntimeInfo {
                    module_name: module.module_name,
                    module_kind: module.module_kind,
                    version: module.version,
                    description: module.description,
                    service_name: module.service_name,
                    instance_id: module.instance_id,
                    module_run_id: module.module_run_id,
                    host: module.host,
                    status: module.status,
                    last_heartbeat_at: module.last_heartbeat_at,
                    started_at: module.started_at,
                    heartbeat_source,
                }),
                Err(error) => {
                    tracing::warn!(
                        service_name = ?module.service_name,
                        heartbeat_source = ?module.heartbeat_source,
                        error = %error,
                        "Skipping runtime module with unrecognised heartbeat_source in listing"
                    );
                    None
                }
            },
        )
        .collect::<Vec<_>>();

    Ok(RuntimeListActiveResponse { modules })
}

/// Get runtime health summary.
///
/// Returns unique-module counts plus the number of concrete active runs.
pub async fn handle_runtime_health(
    pool: &PgPool,
    request: RuntimeHealthRequest,
) -> Result<RuntimeHealthResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let health = pool
        .state()
        .get_runtime_health(stale_after)
        .await
        .map_err(|e| SinexError::database("Failed to get runtime health").with_std_error(&e))?;

    Ok(RuntimeHealthResponse {
        active_count: health.active_count,
        inactive_count: health.inactive_count,
        unique_modules: health.unique_modules,
        active_run_count: health.active_run_count,
        oldest_heartbeat: health.oldest_heartbeat,
    })
}
