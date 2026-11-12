//! Retained parent arrays are streamed into the edge table once. The saved
//! prefix remains authoritative across restarts; admission and tombstones
//! write their own edges while the projection is incomplete.
//!
//! Walks use the edge table plus parent arrays beyond the committed prefix.
//! A completed pass commits its edges and upper UUID cut together. Replacing
//! the walk definitions and then removing the mark retain the existing
//! two-transaction guarantee for readers planned against the fallback.
use super::Task;
use crate::Error;
use sqlx::{Connection, PgConnection};

/// The edges are complete for every event up to and including
/// `filled_through`.
const MARK: &str = "CREATE TABLE sinex_schema.event_children_fill (
  id boolean PRIMARY KEY CHECK (id),
  filled_through uuid NOT NULL
)";

const CHILDREN_WHILE_FILLING: &str = "CREATE OR REPLACE FUNCTION core.event_children_of(event uuid)
RETURNS TABLE (child uuid) LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT c.child FROM core.event_children c WHERE c.parent = $1
  UNION
  SELECT e.id FROM core.events e
  WHERE e.parents IS NOT NULL AND e.parents @> ARRAY[$1]
    AND e.id > (SELECT f.filled_through FROM sinex_schema.event_children_fill f)
$$";

const PARENTS_WHILE_FILLING: &str = "CREATE OR REPLACE FUNCTION core.event_parents_of(event uuid)
RETURNS TABLE (parent uuid) LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT c.parent FROM core.event_children c WHERE c.child = $1
  UNION
  SELECT p.parent FROM core.events e CROSS JOIN LATERAL unnest(e.parents) p(parent)
  WHERE e.id = $1 AND e.id > (SELECT f.filled_through FROM sinex_schema.event_children_fill f)
$$";

/// What the upgrade runs instead of filling the edges: the mark, starting
/// before every event (the smallest version-7 UUID, the only kind the event
/// partitioning compares), and the walk definitions that read past it.
pub(super) fn begin() -> Vec<String> {
    vec![
        MARK.into(),
        "INSERT INTO sinex_schema.event_children_fill (id, filled_through) VALUES (true, '00000000-0000-7000-8000-000000000000')".into(),
        CHILDREN_WHILE_FILLING.into(),
        PARENTS_WHILE_FILLING.into(),
    ]
}

/// Complete the remaining parent projection, then retire its fallback.
pub(super) async fn step(conn: &mut PgConnection) -> Result<(), Error> {
    fill(conn).await?;
    finish(conn).await
}

/// A pass and its upper UUID cut commit together. A crash rolls back the
/// pass; a crash after commit resumes only beyond that cut. New admissions
/// already own their edges, including rows whose IDs precede the cut.
async fn fill(conn: &mut PgConnection) -> Result<u64, Error> {
    let mut tx = conn.begin().await?;
    // A tombstone writes its edges before removing the event. Order it
    // wholly before or after this pass so the two cannot deadlock on edges.
    sqlx::query("LOCK TABLE core.event_tombstones IN SHARE MODE")
        .execute(&mut *tx)
        .await?;
    let mark: sqlx::types::Uuid = sqlx::query_scalar(
        "SELECT filled_through FROM sinex_schema.event_children_fill FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    // Read the newest key without a strict partition predicate. UUID time
    // pruning must not collapse distinct IDs in the prefix's timestamp.
    let cut: Option<sqlx::types::Uuid> =
        sqlx::query_scalar("SELECT e.id FROM core.events e ORDER BY e.id DESC LIMIT 1")
            .persistent(false)
            .fetch_optional(&mut *tx)
            .await?;
    let mut written = 0;
    if let Some(cut) = cut.filter(|cut| *cut > mark) {
        // Inclusive bounds prune chunks without excluding a shared UUID
        // timestamp. uuid_cmp retains the exact residual over every bit.
        // A nil/older prefix has no timestamp that native pruning can use.
        let lower = if mark.get_version_num() == 7 {
            format!("AND e.id >= '{mark}'::uuid")
        } else {
            String::new()
        };
        let upper = if cut.get_version_num() == 7 {
            format!("AND e.id <= '{cut}'::uuid")
        } else {
            String::new()
        };
        written = sqlx::query(&format!(
            "INSERT INTO core.event_children (parent, child)
             SELECT p.parent, e.id FROM core.events e
             CROSS JOIN LATERAL unnest(e.parents) p(parent)
             WHERE pg_catalog.uuid_cmp(e.id,'{mark}'::uuid)>0
               AND pg_catalog.uuid_cmp(e.id,'{cut}'::uuid)<=0
               {lower} {upper} AND e.parents IS NOT NULL
             ON CONFLICT DO NOTHING"
        ))
        .persistent(false)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        sqlx::query("UPDATE sinex_schema.event_children_fill SET filled_through = $1::uuid")
            .bind(&cut)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(written)
}

/// Every edge is stored: give the walks their fresh definitions, then end the
/// task. Two transactions, so a walk planned with the definition that reads
/// the mark never finds the mark gone; an interruption between them leaves a
/// filled store whose next pass reads only a newly admitted tail.
async fn finish(conn: &mut PgConnection) -> Result<(), Error> {
    install_walks(conn).await?;
    sqlx::query(&super::end(Task::EventChildren))
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

async fn install_walks(conn: &mut PgConnection) -> Result<(), Error> {
    let mut tx = conn.begin().await?;
    for definition in [
        crate::tables::EVENT_CHILDREN_OF,
        crate::tables::EVENT_PARENTS_OF,
    ] {
        sqlx::query(&definition.replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
            .persistent(false)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
