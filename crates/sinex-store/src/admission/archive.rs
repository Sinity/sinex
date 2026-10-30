//! Archiving live readings: a replay's re-read scope, hide and unhide, and
//! removing live side-table rows.
use super::*;
use crate::Error;
use sinex_model::{ArchiveReason, OperationId, ReplayScope};

mod history;
mod restore;
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
    let dirty = mark_archived_dirty(conn, &live, seq, keys, None, None).await?;
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
                "SELECT source, event_type, equivalence_key, material_id, anchor, output_index FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            sqlx::query!(
                r#"INSERT INTO core.hidden_records (source, event_type, equivalence_key, material_id, anchor, output_index, operation_id)
                   VALUES ($1, $2, $3,
                     CASE WHEN $3::text IS NULL THEN $4::uuid END,
                     CASE WHEN $3::text IS NULL THEN $5::bigint END,
                     CASE WHEN $3::text IS NULL THEN $6::bigint END, $7)
                   ON CONFLICT DO NOTHING"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.output_index,
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
                event.output_index,
            )
            .await?;
            let seq = position(conn).await?;
            let dirty = mark_archived_dirty(conn, &live, seq, keys, None, Some(*operation)).await?;
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
                "SELECT source, event_type, equivalence_key, material_id, anchor, output_index FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            let removed = sqlx::query_scalar!(
                r#"DELETE FROM core.hidden_records WHERE source = $1 AND event_type = $2
                   AND ((equivalence_key = $3) OR ($3::text IS NULL AND material_id = $4 AND anchor = $5 AND output_index = $6))
                   RETURNING operation_id"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.output_index,
            )
            .fetch_optional(&mut *conn)
            .await?;
            let owners = sqlx::query!(
                "SELECT derivation, key FROM derivation.outputs WHERE event_id = $1",
                target.0,
            )
            .fetch_all(&mut *conn)
            .await?;
            if !owners.is_empty() {
                let seq = position(conn).await?;
                return super::derived::mark_dirty(
                    conn,
                    owners
                        .into_iter()
                        .map(|row| (row.derivation, row.key))
                        .collect(),
                    seq,
                )
                .await;
            }
            if removed.is_none() {
                return Ok(0);
            }
            let seq = position(conn).await?;
            // The newest archived-by-hide reading becomes live again.
            let unhidden = sqlx::query!(
                r#"SELECT e.id, e.material_id, e.anchor, e.output_index, e.reading_order, e.record_hash
                   FROM core.events e JOIN audit.archived a ON a.event_id = e.id AND a.reason = 'hide'
                   WHERE e.source = $1 AND e.event_type = $2
                   AND ((e.equivalence_key = $3) OR ($3::text IS NULL AND e.material_id = $4 AND e.anchor = $5 AND e.output_index = $6))
                   ORDER BY e.reading_order DESC, e.commit_seq DESC LIMIT 1"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.output_index,
            )
            .fetch_optional(&mut *conn)
            .await?;
            let Some(r) = unhidden else { return Ok(0) };
            restore::reading(conn, r.id, seq, operation.0).await?;
            let dirty = mark_archived_dirty(conn, &[r.id], seq, keys, None, None).await?;
            let restored = restore::descendants(
                conn,
                r.id,
                removed.expect("the hidden record was removed"),
                seq,
                operation.0,
                keys,
            )
            .await?;
            Ok(dirty + restored)
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
    output_index: i64,
) -> Result<Vec<Uuid>> {
    Ok(match key {
        Some(key) => sqlx::query_scalar!(
            "SELECT event_id FROM core.live_equivalence WHERE source = $1 AND event_type = $2 AND equivalence_key = core.encode_equivalence_key($3)",
            source, event_type, key
        )
        .fetch_all(&mut *conn)
        .await?,
        None => sqlx::query_scalar!(
            "SELECT event_id FROM core.live_locations WHERE material_id = $1 AND anchor = $2 AND event_type = $3 AND output_index = $4",
            material, anchor, event_type, output_index
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
