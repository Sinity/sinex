//! Operator-facing automata status handlers.

use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::automata::{
    AutomataStatusRequest, AutomataStatusResponse, AutomatonStatus,
};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

/// List registered automata with run, checkpoint, and automaton telemetry.
pub async fn handle_automata_status(
    pool: &PgPool,
    request: AutomataStatusRequest,
) -> Result<AutomataStatusResponse> {
    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let automata = pool
        .state()
        .list_automata_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list automata status").with_std_error(&e))?
        .into_iter()
        .map(|row| AutomatonStatus {
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
            events_processed_current_run: row.events_processed_current_run,
            checkpoint_kind: row.checkpoint_kind,
            checkpoint_position: row.checkpoint_position,
            checkpoint_revision: row.checkpoint_revision,
            checkpoint_recorded_at: row.checkpoint_recorded_at,
            pending_invalidation_count: row.pending_invalidation_count,
            error_rate_5m: row.error_rate_5m,
            event_lag_p50_ms: row.event_lag_p50_ms,
            event_lag_p99_ms: row.event_lag_p99_ms,
            tick_runtime_p99_ms: row.tick_runtime_p99_ms,
            throughput_eps: row.throughput_eps,
            recent_output_count: row.recent_output_count,
            last_output_at: row.last_output_at,
            last_replay_at: row.last_replay_at,
        })
        .collect();

    let response = AutomataStatusResponse {
        generated_at: Timestamp::now(),
        stale_after_secs: request.stale_after_secs,
        recent_window_secs: request.recent_window_secs,
        automata,
    };

    Ok(response)
}
