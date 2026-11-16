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

/// Wait up to `wait` for the writer lock on this connection's session,
/// blocking on the lock itself. A writer reconnecting after a lost connection
/// waits here until the server ends the old session; `false` means another
/// writer still holds the lock when the wait ends.
pub async fn claim_waiting(conn: &mut PgConnection, wait: std::time::Duration) -> Result<bool> {
    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query(&format!(
        "SET LOCAL lock_timeout = '{}ms'",
        wait.as_millis()
    ))
    .execute(&mut *tx)
    .await?;
    // A session lock taken inside a transaction outlives it.
    let taken = sqlx::query!("SELECT pg_advisory_lock($1)", sinex_schema::WRITER_LOCK)
        .execute(&mut *tx)
        .await;
    match taken {
        Ok(_) => {
            tx.commit().await?;
            Ok(true)
        }
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => {
            tx.rollback().await?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}
