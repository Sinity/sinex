//! Archiving live readings inside admission: a replay's re-read scope, and
//! the archive history and live side-table rows every move writes.
use super::*;
use sinex_model::{ArchiveReason, OperationId, ReplayScope};

mod history;
pub(crate) use history::{archive_at, position, restore_at};

/// Archive the live readings a replay re-reads: those read from the scope's
/// materials (narrowed by type and time), and the live derived events that
/// descend from them. Their keys become dirty, so derivations recompute. The
/// materials' failure rows go too when whole materials are re-read: the
/// replay records its own.
pub(super) async fn replay_scope(
    conn: &mut PgConnection,
    scope: &ReplayScope,
    operation: OperationId,
    keys: &dyn DirtyKeys,
) -> Result<(usize, usize)> {
    let materials: Vec<Uuid> = scope.materials.iter().map(|m| m.0).collect();
    let types: Option<Vec<String>> = scope.event_types.clone();
    let live: Vec<Uuid> = sqlx::query_scalar!(
        r#"WITH RECURSIVE read_here AS (
             SELECT e.id FROM core.events e
             WHERE e.material_id = ANY($1)
               AND ($2::text[] IS NULL OR e.event_type = ANY($2))
               AND ($3::timestamptz IS NULL OR e.ts_orig >= $3)
               AND ($4::timestamptz IS NULL OR e.ts_orig < $4)
               AND ($3::timestamptz IS NULL AND $4::timestamptz IS NULL OR e.ts_orig IS NOT NULL)),
           descendants AS (
             SELECT id FROM read_here
             UNION
             SELECT edge.child FROM core.event_children edge JOIN descendants p ON edge.parent = p.id)
           SELECT x.id AS "id!" FROM descendants x
           WHERE EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = x.id)
              OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = x.id)"#,
        &materials,
        types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0),
    )
    .fetch_all(&mut *conn)
    .await?;
    let seq = position(conn).await?;
    let dirty = mark_archived_dirty(
        conn,
        &live,
        seq,
        keys,
        None,
        (ArchiveReason::Replay, Some(operation.0)),
    )
    .await?;
    archive_at(
        conn,
        &live,
        seq,
        ArchiveReason::Replay,
        None,
        Some(operation.0),
    )
    .await?;
    remove_live(conn, &live).await?;
    if scope.event_types.is_none() && scope.from.is_none() && scope.until.is_none() {
        sqlx::query!(
            "DELETE FROM raw.failures WHERE material_id = ANY($1)",
            &materials
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok((live.len(), dirty))
}

pub(crate) async fn remove_live(conn: &mut PgConnection, ids: &[Uuid]) -> Result<()> {
    sqlx::query!(
        "DELETE FROM core.live_equivalence WHERE event_id = ANY($1)",
        ids
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "DELETE FROM core.live_locations WHERE event_id = ANY($1)",
        ids
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}
