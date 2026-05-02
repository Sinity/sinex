//! Composable event query and provenance lineage handlers.

use serde_json::Value;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::query::{EventQuery, LineageQuery};
use sinex_primitives::{Result, SinexError};
use sqlx::PgPool;

pub async fn handle_events_query(pool: &PgPool, params: Value) -> Result<Value> {
    let query: EventQuery = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid event query parameters").with_std_error(&error)
    })?;
    let result = pool.events().query(query).await?;
    serde_json::to_value(&result).map_err(|error| {
        SinexError::serialization("failed to serialize events.query response")
            .with_std_error(&error)
    })
}

pub async fn handle_events_lineage(pool: &PgPool, params: Value) -> Result<Value> {
    let query: LineageQuery = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("Invalid lineage query parameters").with_std_error(&error)
    })?;
    let result = pool.events().lineage(query).await?;
    serde_json::to_value(&result).map_err(|error| {
        SinexError::serialization("failed to serialize events.lineage response")
            .with_std_error(&error)
    })
}
