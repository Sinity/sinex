//! Archiving live readings: a replay's re-read scope, hide and unhide, and
//! removing live side-table rows.
use super::*;
use crate::Error;
use sinex_model::{ArchiveReason, OperationId, ReplayScope};

mod history;
pub use history::erase_archive_history;
pub(super) use history::{archive_at, position};

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
             SELECT d.id FROM core.events d JOIN descendants p ON d.parents @> ARRAY[p.id])
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
    let dirty = mark_archived_dirty(conn, &live, seq, keys).await?;
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

/// Hide and unhide work on equivalence keys, or record locations for keyless
/// types, and need nothing per source.
pub(super) async fn request(
    conn: &mut PgConnection,
    request: &ArchiveRequest,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    match request {
        ArchiveRequest::Replay { .. } | ArchiveRequest::RemovedAtSource { .. } => Ok(0),
        ArchiveRequest::Hide { target, operation } => {
            let event = sqlx::query!(
                "SELECT source, event_type, equivalence_key, material_id, anchor, ordinal FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            sqlx::query!(
                r#"INSERT INTO core.hidden_occurrences (source, event_type, equivalence_key, material_id, anchor, ordinal, operation_id)
                   VALUES ($1, $2, $3,
                     CASE WHEN $3::text IS NULL THEN $4::uuid END,
                     CASE WHEN $3::text IS NULL THEN $5::bigint END,
                     CASE WHEN $3::text IS NULL THEN $6::smallint END, $7)
                   ON CONFLICT DO NOTHING"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.ordinal,
                operation.0,
            )
            .execute(&mut *conn)
            .await?;
            // Whatever reading of this record is live goes, not only the one named.
            let live = live_reading(
                conn,
                &event.source,
                &event.event_type,
                event.equivalence_key.as_deref(),
                event.material_id,
                event.anchor,
                event.ordinal,
            )
            .await?;
            let seq = position(conn).await?;
            let dirty = mark_archived_dirty(conn, &live, seq, keys).await?;
            archive_at(
                conn,
                &live,
                seq,
                ArchiveReason::Hide,
                None,
                Some(operation.0),
            )
            .await?;
            remove_live(conn, &live).await?;
            Ok(dirty)
        }
        ArchiveRequest::Unhide { target, operation } => {
            let event = sqlx::query!(
                "SELECT source, event_type, equivalence_key, material_id, anchor, ordinal FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            let removed = sqlx::query_scalar!(
                r#"DELETE FROM core.hidden_occurrences WHERE source = $1 AND event_type = $2
                   AND ((equivalence_key = $3) OR ($3::text IS NULL AND material_id = $4 AND anchor = $5 AND ordinal = $6))
                   RETURNING id"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.ordinal,
            )
            .fetch_optional(&mut *conn)
            .await?;
            if removed.is_none() {
                return Ok(0);
            }
            let seq = position(conn).await?;
            // The newest archived-by-hide reading becomes live again.
            let restored = sqlx::query!(
                r#"SELECT e.id, e.material_id, e.anchor, e.ordinal, e.reading_order, e.record_hash
                   FROM core.events e JOIN audit.archived a ON a.event_id = e.id AND a.reason = 'hide'
                   WHERE e.source = $1 AND e.event_type = $2
                   AND ((e.equivalence_key = $3) OR ($3::text IS NULL AND e.material_id = $4 AND e.anchor = $5 AND e.ordinal = $6))
                   ORDER BY e.reading_order DESC, e.commit_seq DESC LIMIT 1"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.ordinal,
            )
            .fetch_optional(&mut *conn)
            .await?;
            let Some(r) = restored else { return Ok(0) };
            if !history::restore_at(conn, r.id, seq, operation.0).await? {
                return Err(Error::Invalid(
                    "hidden reading changed before restoration".into(),
                ));
            }
            match &event.equivalence_key {
                Some(key) => {
                    sqlx::query!(
                        "INSERT INTO core.live_equivalence (source, event_type, equivalence_key, event_id, reading_order, record_hash) VALUES ($1,$2,$3,$4,$5,$6)",
                        event.source, event.event_type, key, r.id, r.reading_order, r.record_hash,
                    )
                    .execute(&mut *conn)
                    .await?;
                }
                None => {
                    sqlx::query!(
                        "INSERT INTO core.live_locations (material_id, anchor, event_type, ordinal, event_id, reading_order, record_hash) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                        r.material_id, r.anchor, event.event_type, r.ordinal, r.id, r.reading_order, r.record_hash,
                    )
                    .execute(&mut *conn)
                    .await?;
                }
            }
            mark_archived_dirty(conn, &[r.id], seq, keys).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn live_reading(
    conn: &mut PgConnection,
    source: &str,
    event_type: &str,
    key: Option<&str>,
    material: Option<Uuid>,
    anchor: Option<i64>,
    ordinal: i16,
) -> Result<Vec<Uuid>> {
    Ok(match key {
        Some(key) => sqlx::query_scalar!(
            "SELECT event_id FROM core.live_equivalence WHERE source = $1 AND event_type = $2 AND equivalence_key = $3",
            source, event_type, key
        )
        .fetch_all(&mut *conn)
        .await?,
        None => sqlx::query_scalar!(
            "SELECT event_id FROM core.live_locations WHERE material_id = $1 AND anchor = $2 AND event_type = $3 AND ordinal = $4",
            material, anchor, event_type, ordinal
        )
        .fetch_all(&mut *conn)
        .await?,
    })
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
