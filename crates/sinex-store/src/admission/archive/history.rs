//! Archived state, its history and the live count of each source are
//! written together.
use crate::Result;
use sinex_model::{ArchiveReason, DbCheck};
use sqlx::PgConnection;
use uuid::Uuid;

/// A non-event change receives an ordinary writer position, not an Event.
pub(in crate::admission) async fn position(conn: &mut PgConnection) -> Result<i64> {
    crate::commit_log::allocate_position(conn).await
}

pub(in crate::admission) async fn archive_at(
    conn: &mut PgConnection,
    ids: &[Uuid],
    position: i64,
    reason: ArchiveReason,
    replacements: Option<&[Uuid]>,
    operation: Option<Uuid>,
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    if replacements.is_some_and(|replacements| replacements.len() != ids.len()) {
        return Err(crate::Error::Invalid(
            "replacement IDs do not cover archived events".into(),
        ));
    }
    sqlx::query!(r#"WITH added AS (
        INSERT INTO audit.archived(event_id,archived_seq,reason,replaced_by,operation_id)
        SELECT event_id,$3,$4,replaced_by,$5 FROM unnest($1::uuid[],$2::uuid[]) AS r(event_id,replaced_by)
        ON CONFLICT(event_id) DO NOTHING
        RETURNING change_seq,event_id,archived_seq,reason,replaced_by,operation_id,archived_at),
      changes AS (
        INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason,replaced_by,operation_id,recorded_at)
        SELECT change_seq,event_id,archived_seq,true,reason,replaced_by,operation_id,archived_at FROM added)
      UPDATE core.source_counts c SET events = c.events - left_live.n
      FROM (SELECT e.source, count(*) AS n FROM core.events e
            WHERE e.id = ANY($1) AND e.id IN (SELECT event_id FROM added)
            GROUP BY e.source) left_live
      WHERE c.source = left_live.source"#,
      ids,replacements,position,reason.as_db(),operation)
      .execute(conn).await?;
    Ok(())
}

pub(super) async fn unhide_at(
    conn: &mut PgConnection,
    event: Uuid,
    position: i64,
    operation: Uuid,
) -> Result<bool> {
    let changed=sqlx::query_scalar!(r#"WITH unhidden AS (
        DELETE FROM audit.archived WHERE event_id=$1 AND reason='hide' RETURNING event_id),
      changes AS (
        INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason,operation_id)
        SELECT nextval(pg_get_serial_sequence('audit.archived','change_seq')),event_id,$2,false,'unhide',$3 FROM unhidden),
      counted AS (
        UPDATE core.source_counts c SET events = c.events + 1
        FROM core.events e
        WHERE e.id = $1 AND e.id IN (SELECT event_id FROM unhidden) AND c.source = e.source)
      SELECT count(*) AS "n!" FROM unhidden"#,event,position,operation)
      .fetch_one(conn).await?;
    Ok(changed != 0)
}
