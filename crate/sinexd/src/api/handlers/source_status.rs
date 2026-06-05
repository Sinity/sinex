//! Operator-facing source status handler.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::source_status::{
    SourceStatus, SourcesStatusRequest, SourcesStatusResponse,
};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

/// List registered sources with run, health, and recent-emission stats.
///
/// Mirrors `handle_automata_status` (`automata.status`) for the source-side
/// surface; filtered to `manifest_type = 'source'`.
pub async fn handle_sources_status(
    pool: &PgPool,
    request: SourcesStatusRequest,
) -> Result<SourcesStatusResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let sources = pool
        .state()
        .list_sources_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list sources status").with_std_error(&e))?
        .into_iter()
        .map(|row| SourceStatus {
            module_name: row.module_name,
            version: row.version,
            description: row.description,
            manifest_status: row.manifest_status.unwrap_or_default(),
            live: row.live,
            service_name: row.service_name,
            instance_id: row.instance_id,
            module_run_id: row.module_run_id,
            host: row.host,
            run_status: row.run_status,
            started_at: row.started_at,
            last_heartbeat_at: row.last_heartbeat_at,
            current_health: row.current_health,
            health_changed_at: row.health_changed_at,
            health_reason: row.health_reason,
            recent_output_count: row.recent_output_count,
            last_output_at: row.last_output_at,
        })
        .collect();

    let response = SourcesStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: request.stale_after_secs,
        recent_window_secs: request.recent_window_secs,
        sources,
    };

    Ok(response)
}
