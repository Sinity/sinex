//! The parent edges of events stored before the upgrade, filled after start
//! in ascending event order, so each step reads only the chunks after its
//! mark. Events admitted after start already have their edges; inserting one
//! again changes nothing.
//!
//! Every walk reads edges through `core.event_children_of` and
//! `core.event_parents_of`. While the fill runs, the upgrade gives them a
//! definition that also reads the `parents` arrays of the events after the
//! mark, through the `parents` index, so a walk answers exactly as on a
//! filled store. The step that reaches the end puts the fresh definitions
//! back and ends the task in one transaction. The oldest chunks are filled
//! first, and those are the compressed ones, so the fallback reads only
//! uncompressed chunks once the fill is under way.
//!
//! A tombstone keeps its event's edges (it writes them before removing the
//! event), so an event tombstoned before the fill reaches it keeps them too.
//! Each batch locks the tombstone table, so a tombstone runs wholly before or
//! after a batch and the two never wait on each other's edges.
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

/// Fill the edges of the next `batch` events with parents after the mark and
/// move the mark, in one transaction. `Some(edges written)` while events
/// remained; `None` once the fill reached the end and the task finished.
pub(super) async fn step(conn: &mut PgConnection, batch: i64) -> Result<Option<u64>, Error> {
    let mut tx = conn.begin().await?;
    // A tombstone writes its event's edges and then removes the event; this
    // lock orders it before or after the batch, never interleaved with it.
    sqlx::query("LOCK TABLE core.event_tombstones IN SHARE MODE")
        .execute(&mut *tx)
        .await?;
    let mark: String = sqlx::query_scalar(
        "SELECT filled_through::text FROM sinex_schema.event_children_fill FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    // The mark is spelled out as a constant so chunks before it are excluded
    // when the statement is planned.
    let (last, written): (Option<String>, i64) = sqlx::query_as(&format!(
        "WITH batch AS (
           SELECT e.id, e.parents FROM core.events e
           WHERE e.id > '{mark}'::uuid AND e.parents IS NOT NULL
           ORDER BY e.id LIMIT $1),
         written AS (
           INSERT INTO core.event_children (parent, child)
           SELECT DISTINCT p.parent, b.id FROM batch b CROSS JOIN LATERAL unnest(b.parents) p(parent)
           ON CONFLICT DO NOTHING RETURNING 1)
         SELECT (SELECT id::text FROM batch ORDER BY id DESC LIMIT 1), (SELECT count(*) FROM written)"
    ))
    .persistent(false)
    .bind(batch.max(1))
    .fetch_one(&mut *tx)
    .await?;
    let Some(last) = last else {
        tx.commit().await?;
        finish(conn).await?;
        return Ok(None);
    };
    sqlx::query("UPDATE sinex_schema.event_children_fill SET filled_through = $1::uuid")
        .bind(&last)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(
        u64::try_from(written).expect("a count is not negative"),
    ))
}

/// Every edge is stored: give the walks their fresh definitions, then end the
/// task. Two transactions, so a walk planned with the definition that reads
/// the mark never finds the mark gone; an interruption between them leaves a
/// filled store whose next step finds no event and ends here again.
async fn finish(conn: &mut PgConnection) -> Result<(), Error> {
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
    sqlx::query(&super::end(Task::EventChildren))
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
