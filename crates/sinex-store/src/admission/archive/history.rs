//! Current archival membership and its history have one transactional writer.
use crate::Result;
use sinex_model::{ArchiveReason, DbCheck};
use sqlx::PgConnection;
use uuid::Uuid;

/// A non-event change receives an ordinary writer position, not an Event.
pub(in crate::admission) async fn position(conn: &mut PgConnection) -> Result<i64> {
    let position = sqlx::query_scalar!(
        "UPDATE core.counters SET value=value+1 WHERE name='commit_seq' RETURNING value"
    )
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query!(
        "INSERT INTO core.commit_log(commit_seq,event_id,event_type) VALUES($1,NULL,NULL)",
        position
    )
    .execute(conn)
    .await?;
    Ok(position)
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
        RETURNING notice_seq,event_id,archived_seq,reason,replaced_by,operation_id,archived_at)
      INSERT INTO audit.archive_changes(notice_seq,event_id,commit_seq,archived,reason,replaced_by,operation_id,recorded_at)
      SELECT notice_seq,event_id,archived_seq,true,reason,replaced_by,operation_id,archived_at FROM added"#,
      ids,replacements,position,reason.as_db(),operation)
      .execute(conn).await?;
    Ok(())
}

pub(super) async fn restore_at(
    conn: &mut PgConnection,
    event: Uuid,
    position: i64,
    operation: Uuid,
) -> Result<bool> {
    let changed=sqlx::query!(r#"WITH restored AS (
        DELETE FROM audit.archived WHERE event_id=$1 AND reason='hide' RETURNING event_id)
      INSERT INTO audit.archive_changes(notice_seq,event_id,commit_seq,archived,reason,operation_id)
      SELECT nextval(pg_get_serial_sequence('audit.archived','notice_seq')),event_id,$2,false,'unhide',$3 FROM restored"#,event,position,operation)
      .execute(conn).await?;
    Ok(changed.rows_affected() != 0)
}

/// Excision removes the archive state and history of the excised events too.
pub async fn erase_archive_history(conn: &mut PgConnection, ids: &[Uuid]) -> Result<()> {
    sqlx::query!("DELETE FROM audit.archived WHERE event_id=ANY($1)", ids)
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        "DELETE FROM audit.archive_changes WHERE event_id=ANY($1)",
        ids
    )
    .execute(conn)
    .await?;
    Ok(())
}
