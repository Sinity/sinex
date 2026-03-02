//! Composable event query and provenance lineage handlers.

use color_eyre::eyre::{Result, WrapErr};
use serde_json::Value;
use sinex_db::repositories::DbPoolExt;
use sinex_primitives::query::{EventQuery, LineageQuery};
use sqlx::PgPool;

pub async fn handle_events_query(pool: &PgPool, params: Value) -> Result<Value> {
    let query: EventQuery =
        serde_json::from_value(params).wrap_err("Invalid event query parameters")?;
    let result = pool.events().query(query).await?;
    Ok(serde_json::to_value(&result)?)
}

pub async fn handle_events_lineage(pool: &PgPool, params: Value) -> Result<Value> {
    let query: LineageQuery =
        serde_json::from_value(params).wrap_err("Invalid lineage query parameters")?;
    let result = pool.events().lineage(query).await?;
    Ok(serde_json::to_value(&result)?)
}
