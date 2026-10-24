//! Producer transport custody adds no ready material or historical observations.
use crate::{Error, tables};
use sqlx::{Executor, PgConnection};

pub(super) async fn apply(conn: &mut PgConnection) -> Result<(), Error> {
    let collision: bool = sqlx::query_scalar("SELECT to_regclass('raw.pending_producer_bytes') IS NOT NULL OR to_regclass('raw.pending_producer_blobs') IS NOT NULL")
        .fetch_one(&mut *conn).await?;
    if collision {
        return Err(Error::Shape(4));
    }
    for statement in [
        tables::pending_producer_table(),
        tables::pending_producer_blobs_table(),
        "CREATE INDEX pending_producer_blob ON raw.pending_producer_blobs(blob)",
        "GRANT SELECT ON raw.pending_producer_bytes,raw.pending_producer_blobs TO sinex_reader",
        "GRANT SELECT,INSERT,UPDATE,DELETE ON raw.pending_producer_bytes,raw.pending_producer_blobs TO sinex_writer",
    ] {
        conn.execute(statement).await?;
    }
    Ok(())
}
