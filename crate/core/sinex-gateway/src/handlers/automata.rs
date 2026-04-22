//! Operator-facing automata status handlers.

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::SinexError;
use sinex_primitives::rpc::automata::{
    AutomataStatusRequest, AutomataStatusResponse, AutomatonStatus,
};
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use std::time::Duration;

type Result<T> = std::result::Result<T, SinexError>;

/// List registered automata with run, checkpoint, and derived-node telemetry.
pub async fn handle_automata_status(pool: &PgPool, params: Value) -> Result<Value> {
    let request: AutomataStatusRequest = super::parse_default_on_null(params).map_err(|e| {
        SinexError::serialization("Invalid automata status request").with_std_error(&e)
    })?;

    let stale_after = Duration::from_secs(request.stale_after_secs);
    let recent_window = Duration::from_secs(request.recent_window_secs);
    let automata = pool
        .state()
        .list_automata_status(stale_after, recent_window)
        .await
        .map_err(|e| SinexError::database("Failed to list automata status").with_std_error(&e))?
        .into_iter()
        .map(|row| AutomatonStatus {
            node_name: row.node_name,
            version: row.version,
            description: row.description,
            manifest_status: row.manifest_status,
            live: row.live,
            service_name: row.service_name,
            instance_id: row.instance_id,
            node_run_id: row.node_run_id,
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

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize automata status response").with_std_error(&e)
    })
}
