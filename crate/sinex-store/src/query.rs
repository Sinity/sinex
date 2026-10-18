//! The query language compiled to SQL; search; recall; joins by time
//! (architecture §7.1). Filled by the query and surfaces lane.
use crate::Result;
use sqlx::PgConnection;

/// One row of a query result.
pub type Row = serde_json::Value;

/// Run a parsed query against the live events.
pub async fn run(_conn: &mut PgConnection, _query: &str) -> Result<Vec<Row>> {
    todo!("architecture §7.1: compile the query syntax tree to SQL")
}

/// Live events of one type in a `ts_orig` window, newest first.
pub async fn live_by_type(
    conn: &mut PgConnection,
    event_type: &str,
    limit: i64,
) -> Result<Vec<(uuid::Uuid, serde_json::Value)>> {
    Ok(sqlx::query!(
        r#"SELECT e.id, e.payload FROM core.events e
           WHERE e.event_type = $1 AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY e.ts_orig DESC NULLS LAST LIMIT $2"#,
        event_type,
        limit
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| (r.id, r.payload))
    .collect())
}
