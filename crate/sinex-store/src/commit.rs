//! The commit sequence in `core.commit_counter`, which orders all admitted changes.
use crate::Result;
use sqlx::PgConnection;

const NEXT: &str = "UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq";
const CURRENT: &str = "SELECT seq FROM core.commit_counter WHERE one";
const LOG: &str = "INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) \
                   VALUES ($1,$2,$3,0,$4)";

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
pub(crate) const STATEMENTS: [&str; 3] = [NEXT, CURRENT, LOG];

/// Value of `core.commit_log.change`.
#[derive(Clone, Copy)]
pub(crate) enum Change {
    Added = 0,
    Removed = 1,
}

/// Ids in `core.event_types`, as inserted by the schema.
pub(crate) mod event_type {
    pub(crate) const COMMAND_EXECUTED: i32 = 2;
    pub(crate) const COMMAND_CANONICAL: i32 = 3;
    pub(crate) const COMMAND_IMPORTED: i32 = 4;
    pub(crate) const SESSION_BOUNDARY: i32 = 5;
}

/// Records in `core.commit_log` that the event was added to or removed from the
/// live store at sequence `seq`.
pub(crate) async fn log_commit(
    connection: &mut PgConnection,
    seq: i64,
    event: uuid::Uuid,
    change: Change,
    event_type: i32,
) -> Result<()> {
    sqlx::query(LOG)
        .bind(seq)
        .bind(event)
        .bind(change as i16)
        .bind(event_type)
        .execute(connection)
        .await?;
    Ok(())
}
