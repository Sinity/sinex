//! The commit sequence in `core.commit_counter`, which orders all admitted changes.
use crate::Result;
use sqlx::PgConnection;

const NEXT: &str = "UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq";
const CURRENT: &str = "SELECT seq FROM core.commit_counter WHERE one";

/// Takes the next sequence number. The UPDATE locks the counter row until the
/// transaction ends, so transactions become visible in sequence order.
pub(crate) async fn next_commit_seq(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(NEXT).fetch_one(connection).await?)
}

/// The latest sequence number visible to this connection.
pub(crate) async fn current_commit_seq(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(CURRENT).fetch_one(connection).await?)
}

/// Statements for `prepare_admission_statements`.
pub(crate) const STATEMENTS: [&str; 2] = [NEXT, CURRENT];
