use crate::{Id, JsonValue};
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{HealthStatus, ModuleKind, ModuleName, OperationStatus};
use sqlx::FromRow;
use uuid::Uuid;

/// Database record for `operations_log` table
/// NOTE: The actual table only has: id, `operation_type`, operator, scope,
/// `result_status`, `result_message`, `preview_summary`, `duration_ms`
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: Id<Operation>,
    pub operation_type: String,
    pub operator: String,
    pub scope: Option<JsonValue>,
    pub result_status: OperationStatus,
    pub result_message: Option<String>,
    pub preview_summary: Option<JsonValue>,
    pub duration_ms: Option<i32>,
}

/// Operation log entry for creating operations
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct Operation {
    /// Operation ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(skip)]
    pub id: Option<Id<Operation>>,

    pub operation_type: String,
    pub operator: String,
    pub scope: Option<JsonValue>,
    pub result_status: OperationStatus,
    pub result_message: Option<String>,
    pub preview_summary: Option<JsonValue>,
    pub duration_ms: Option<i32>,
}

/// Manifest row returned by `register_module` — lightweight projection of `core.manifests`.
#[derive(Debug, sqlx::FromRow)]
pub struct ManifestRow {
    pub id: i32,
    pub name: String,
    pub manifest_type: String,
    pub version: String,
    pub description: Option<String>,
    pub created_at: sinex_primitives::temporal::Timestamp,
}

/// runtime module manifest record
#[derive(Debug, sqlx::FromRow)]
pub struct ModuleManifest {
    pub id: i32,
    pub module_name: ModuleName,
    pub module_kind: ModuleKind,
    pub version: String,
    pub description: Option<String>,
    pub anchor_rule_version: Option<i32>,
    pub config_schema: Option<JsonValue>,
    pub created_at: Timestamp,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
}

/// runtime module run record
#[derive(Debug, sqlx::FromRow)]
pub struct ModuleRun {
    pub id: Id<ModuleRun>,
    pub manifest_id: Option<i32>,
    pub service_name: String,
    pub instance_id: String,
    pub host: String,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub effective_config_hash: Option<String>,
    pub effective_config: Option<JsonValue>,
}

/// Live module presence for operator-facing status surfaces.
#[derive(Debug, sqlx::FromRow)]
pub struct LiveModulePresence {
    pub module_name: ModuleName,
    pub module_kind: ModuleKind,
    pub version: String,
    pub description: Option<String>,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub module_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub status: String,
    pub last_heartbeat_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub heartbeat_source: String,
}

/// Runtime module health summary
#[derive(Debug, Serialize, Deserialize)]
pub struct ModuleHealthSummary {
    pub active_count: i64,
    pub inactive_count: i64,
    pub unique_modules: i64,
    pub active_run_count: i64,
    pub oldest_heartbeat: Option<Timestamp>,
}

/// Operator-facing automaton status row.
#[derive(Debug, sqlx::FromRow)]
pub struct AutomataStatusRow {
    pub module_name: ModuleName,
    pub version: String,
    pub description: Option<String>,
    /// Status of the latest run for this manifest. `None` when no run row
    /// exists yet (manifest registered but never started).
    pub manifest_status: Option<String>,
    pub live: bool,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub module_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub run_status: Option<String>,
    pub started_at: Option<Timestamp>,
    pub last_heartbeat_at: Option<Timestamp>,
    pub events_processed_current_run: Option<i64>,
    pub checkpoint_kind: Option<String>,
    pub checkpoint_position: Option<String>,
    pub checkpoint_revision: Option<i64>,
    pub checkpoint_recorded_at: Option<Timestamp>,
    pub pending_invalidation_count: Option<i64>,
    pub error_rate_5m: Option<f64>,
    pub event_lag_p50_ms: Option<f64>,
    pub event_lag_p99_ms: Option<f64>,
    pub tick_runtime_p99_ms: Option<f64>,
    pub throughput_eps: Option<f64>,
    pub recent_output_count: i64,
    pub last_output_at: Option<Timestamp>,
    pub last_replay_at: Option<Timestamp>,
}

/// Operator-facing source status row.
#[derive(Debug, sqlx::FromRow)]
pub struct SourcesStatusRow {
    pub module_name: ModuleName,
    pub version: String,
    pub description: Option<String>,
    /// Status of the latest run for this manifest. `None` when no run row
    /// exists yet (manifest registered but never started).
    pub manifest_status: Option<String>,
    pub live: bool,
    pub service_name: Option<String>,
    pub instance_id: Option<String>,
    pub module_run_id: Option<Uuid>,
    pub host: Option<String>,
    pub run_status: Option<String>,
    pub started_at: Option<Timestamp>,
    pub last_heartbeat_at: Option<Timestamp>,
    pub current_health: Option<HealthStatus>,
    pub health_changed_at: Option<Timestamp>,
    pub health_reason: Option<String>,
    pub recent_output_count: i64,
    pub last_output_at: Option<Timestamp>,
}

/// Operation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStatistics {
    pub total: i64,
    pub successful: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub avg_duration_ms: Option<i64>,
}

/// System health report
#[derive(Debug, Serialize, Deserialize)]
pub struct SystemHealthReport {
    pub db_connected: bool,
    pub db_connect_error: Option<String>,
    pub timescaledb_version: Option<String>,
    pub timescaledb_error: Option<String>,
    pub uuid_v7_generation_works: bool,
    pub uuid_v7_error: Option<String>,
    pub json_schema_extension_works: bool,
    pub json_schema_error: Option<String>,
    pub events_table_exists: bool,
    pub events_table_error: Option<String>,

    pub module_health: Option<ModuleHealthSummary>,
    pub module_health_error: Option<String>,
}
