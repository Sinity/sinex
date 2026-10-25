//! The session advisory lock held by the database's one writer.
use crate::Result;
use sqlx::PgConnection;

/// Try to take the writer lock on this connection's session. A killed writer
/// frees it with its connection.
pub async fn try_claim(conn: &mut PgConnection) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT pg_try_advisory_lock($1) AS "won!""#,
        sinex_schema::WRITER_LOCK
    )
    .fetch_one(&mut *conn)
    .await?)
}
