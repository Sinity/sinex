//! Read-only SQL under the caller's grants: the statement runs in a read-only
//! transaction as the reader role, so it can read the stable views and tables
//! and change nothing. Rows come back as JSON objects with the statement's own
//! column names, in order.
use crate::{Error, Result};
use serde_json::Value as Json;
use sqlx::{Connection, PgConnection};

/// The rows of a read-only statement.
pub async fn run(conn: &mut PgConnection, statement: &str) -> Result<Vec<Json>> {
    sinex_schema::require_read_boundary(conn).await?;
    let statement = statement.trim().trim_end_matches(';').trim();
    if statement.is_empty() {
        return Err(Error::Invalid("empty SQL statement".into()));
    }
    // A second statement cannot hide inside the wrapping subquery: it is a
    // syntax error there, and a prepared statement is one statement.
    let mut tx = conn.begin().await?;
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query!("SET LOCAL ROLE sinex_reader")
        .execute(&mut *tx)
        .await?;
    sqlx::query!("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await?;
    // An empty or planner-pruned view must not turn an incomplete projection
    // into an apparently complete empty answer.
    sqlx::query_scalar::<_, i64>("SELECT privacy.require_active_epoch()")
        .fetch_one(&mut *tx)
        .await?;
    // Wrapping keeps column order (json, not jsonb) and refuses anything that
    // is not a query.
    let wrapped = format!("SELECT row_to_json(q) FROM ({statement}) q");
    let rows: Vec<Json> = sqlx::query_scalar(&wrapped).fetch_all(&mut *tx).await?;
    tx.rollback().await?;
    Ok(rows)
}
