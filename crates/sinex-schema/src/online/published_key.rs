//! The publication key over (event, stream position, stream instance), built
//! once after start. The upgrade adds the instance column, whose constant
//! default costs no write, and keeps the single-column key it found. Here the
//! new unique index is built concurrently and then swapped in as the primary
//! key in one short transaction.
//!
//! While this task remains the publisher holds: the old key would refuse a
//! second copy of an event and its `ON CONFLICT` target needs the new index.
//! Its saved position reports the delay. No other writer touches the table.
use super::Task;
use crate::Error;
use sqlx::{Connection, PgConnection};

const BUILD: &str = "CREATE UNIQUE INDEX CONCURRENTLY published_pairs_pkey ON core.published (event_id, stream_seq, stream_instance)";

/// Build the index unless a valid build exists (an invalid one, left by an
/// interrupted build, is dropped first), then swap it in. The swap and the
/// end of the task are one transaction, so the old key is always in place
/// while the task remains.
pub(super) async fn step(conn: &mut PgConnection) -> Result<(), Error> {
    let built: Option<bool> = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('core.published_pairs_pkey') AND indrelid = 'core.published'::regclass",
    )
    .fetch_optional(&mut *conn)
    .await?;
    if built == Some(false) {
        sqlx::query("DROP INDEX CONCURRENTLY core.published_pairs_pkey")
            .persistent(false)
            .execute(&mut *conn)
            .await?;
    }
    if built != Some(true) {
        sqlx::query(BUILD)
            .persistent(false)
            .execute(&mut *conn)
            .await?;
    }
    let mut tx = conn.begin().await?;
    sqlx::query(
        "ALTER TABLE core.published DROP CONSTRAINT published_pkey,
           ADD CONSTRAINT published_pairs_pkey PRIMARY KEY USING INDEX published_pairs_pkey",
    )
    .persistent(false)
    .execute(&mut *tx)
    .await?;
    sqlx::query(&super::end(Task::PublishedKey))
        .persistent(false)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
