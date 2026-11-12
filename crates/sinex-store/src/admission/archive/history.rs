//! Archived state, its history and the live count of each source are
//! written together.
use crate::Result;
use sinex_model::{ArchiveReason, DbCheck};
use sqlx::PgConnection;
use uuid::Uuid;

/// A non-event change receives an ordinary writer position, not an Event.
pub(crate) async fn position(conn: &mut PgConnection) -> Result<i64> {
    crate::commit_log::allocate_position(conn).await
}

pub(crate) async fn archive_at(
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
      FROM (SELECT coalesce(live.source, stored.source) AS source, count(*) AS n
            FROM added
            LEFT JOIN LATERAL (
              SELECT source FROM core.live_equivalence WHERE event_id = added.event_id OFFSET 0
            ) live ON true
            LEFT JOIN LATERAL (
              SELECT source FROM core.events
              WHERE id = added.event_id AND live.source IS NULL OFFSET 0
            ) stored ON true
            GROUP BY coalesce(live.source, stored.source)) left_live
      WHERE c.source = left_live.source"#,
      ids,replacements,position,reason.as_db(),operation)
      .execute(conn).await?;
    Ok(())
}

/// Take `events` out of the archive at `position`, under `operation`, and
/// count them live again. Returns the events that were archived.
pub(crate) async fn restore_at(
    conn: &mut PgConnection,
    events: &[Uuid],
    position: i64,
    operation: Uuid,
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar!(r#"WITH restored AS (
        DELETE FROM audit.archived WHERE event_id = ANY($1) RETURNING event_id),
      changes AS (
        INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason,operation_id)
        SELECT nextval(pg_get_serial_sequence('audit.archived','change_seq')),event_id,$2,false,'restore',$3 FROM restored),
      counted AS (
        UPDATE core.source_counts c SET events = c.events + back.n
        FROM (SELECT e.source, count(*) AS n FROM core.events e
              WHERE e.id IN (SELECT event_id FROM restored) GROUP BY e.source) back
        WHERE c.source = back.source)
      SELECT event_id AS "event_id!" FROM restored"#,events,position,operation)
      .fetch_all(conn).await?)
}
