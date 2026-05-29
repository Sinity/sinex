//! Model-effect cache model types.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Row from `core.model_effects`.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ModelEffectRecord {
    pub id: sqlx::types::Uuid,
    pub provider: String,
    pub model: String,
    pub prompt_hash: String,
    pub schema_hash: Option<String>,
    pub input_hash: String,
    pub composite_key: String,
    pub output: String,
    pub output_hash: String,
    pub replay_policy: String,
    pub recorded_at: time::OffsetDateTime,
    pub recorded_by: String,
    pub source_node_id: Option<String>,
    pub source_event_id: Option<sqlx::types::Uuid>,
}
