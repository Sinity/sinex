//! Operator-facing ingestor status handler.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::ingestors::{
    IngestorStatus, IngestorsStatusRequest, IngestorsStatusResponse,
};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

/// List registered ingestors with run, health, and recent-emission stats.
///
/// Mirrors `handle_automata_status` (`automata.status`) for the source-side
/// surface; filtered to `manifest_type = 'ingestor'`.
pub async fn handle_ingestors_status(
    pool: &PgPool,
    request: IngestorsStatusRequest,
) -> Result<IngestorsStatusResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let ingestors = pool
        .state()
        .list_ingestors_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list ingestors status").with_std_error(&e))?
        .into_iter()
        .map(|row| IngestorStatus {
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

    let response = IngestorsStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: request.stale_after_secs,
        recent_window_secs: request.recent_window_secs,
        ingestors,
    };

    Ok(response)
}
