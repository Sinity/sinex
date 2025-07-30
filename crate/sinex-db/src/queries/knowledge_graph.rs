//! Knowledge graph types for legacy compatibility

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type for entity results
#[derive(Debug, FromRow)]
pub struct EntityRecord {
    pub id: sqlx::types::Uuid,
    pub entity_type: String,
    pub name: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub description: Option<String>,
    pub metadata: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub merged_into_id: Option<sqlx::types::Uuid>,
}

/// Record type for entity relation results
#[derive(Debug, FromRow)]
pub struct EntityRelationRecord {
    pub id: sqlx::types::Uuid,
    pub from_entity_id: sqlx::types::Uuid,
    pub to_entity_id: sqlx::types::Uuid,
    pub relation_type: String,
    pub strength: Option<f64>,
    pub metadata: JsonValue,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub created_from_event_id: Option<sqlx::types::Uuid>,
}
