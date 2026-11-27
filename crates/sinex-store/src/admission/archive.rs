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
    failed_records: &[sinex_model::RecordLocation],
    operation: OperationId,
    keys: &dyn DirtyKeys,
) -> Result<(usize, usize)> {
    let materials: Vec<Uuid> = scope.materials.iter().map(|m| m.0).collect();
    // Preparation runs outside the writer. A lifecycle operation may have
    // archived or tombstoned a selected material since it was read.
    if let Some(material) = sqlx::query!(
        "SELECT id,status FROM raw.materials WHERE id=ANY($1) AND status <> 'live' LIMIT 1",
        &materials,
    )
    .fetch_optional(&mut *conn)
    .await?
    {
        return Err(crate::Error::Invalid(format!(
            "material {} is {}; {}",
            material.id,
            material.status,
            if material.status == "archived" {
                "restore it before replaying it"
            } else {
                "it is never read again"
            }
        )));
    }
    let failed_materials: Vec<Uuid> = failed_records
        .iter()
        .map(|record| record.material.0)
        .collect();
    let failed_offsets: Vec<i64> = failed_records
        .iter()
        .map(|record| {
            i64::try_from(record.record_offset)
                .map_err(|_| crate::Error::Invalid("record offset exceeds database integer".into()))
        })
        .collect::<Result<_>>()?;
    let types: Option<Vec<String>> = scope.event_types.clone();
    let chain = sqlx::query!(
        r#"WITH RECURSIVE read_here AS (
             SELECT e.id FROM core.events e
             WHERE e.material_id = ANY($1)
               AND ($2::text[] IS NULL OR e.event_type = ANY($2))
               AND ($3::timestamptz IS NULL OR e.ts_orig >= $3)
               AND ($4::timestamptz IS NULL OR e.ts_orig < $4)
               AND ($3::timestamptz IS NULL AND $4::timestamptz IS NULL OR e.ts_orig IS NOT NULL)
               AND NOT EXISTS (
                 SELECT 1 FROM unnest($5::uuid[], $6::bigint[]) failed(material_id, record_offset)
                 WHERE failed.material_id = e.material_id AND failed.record_offset = e.record_offset)),
           descendants AS (
             SELECT id FROM read_here
             UNION
             SELECT edge.child FROM descendants p CROSS JOIN LATERAL core.event_children_of(p.id) edge)
           SELECT live.event_id AS "id!", live.event_type AS "event_type!"
           FROM descendants x CROSS JOIN LATERAL (
             SELECT event_id, event_type FROM core.live_locations WHERE event_id = x.id
             UNION
             SELECT event_id, event_type FROM core.live_equivalence WHERE event_id = x.id
             OFFSET 0) live"#,
        &materials,
        types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0),
        &failed_materials,
        &failed_offsets,
    )
    .fetch_all(&mut *conn)
    .await?;
    let live: Vec<Uuid> = chain.iter().map(|member| member.id).collect();
    let selected: Vec<(Uuid, &str)> = chain
        .iter()
        .map(|member| (member.id, member.event_type.as_str()))
        .collect();
    let seq = position(conn).await?;
    // The selection is already whole chains, so nothing further descends.
    let dirty = mark_selected_dirty(conn, &selected, seq, keys, None).await?;
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

/// The live readings among `roots` and everything derived from them, each
/// with its event type. Recursive cardinality estimates grow independently of
/// a chain's real size, so every lookup is parameterized by a discovered ID;
/// the live tables carry the type, so no event row is read.
pub(crate) async fn live_chain(
    conn: &mut PgConnection,
    roots: &[Uuid],
) -> Result<Vec<(Uuid, String)>> {
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query!(
        r#"WITH RECURSIVE chain(id) AS (
             SELECT unnest($1::uuid[])
             UNION
             SELECT c.child FROM chain CROSS JOIN LATERAL (
               SELECT child FROM core.event_children_of(chain.id) OFFSET 0) c)
           SELECT live.event_id AS "id!", live.event_type AS "event_type!"
           FROM chain CROSS JOIN LATERAL (
             SELECT event_id, event_type FROM core.live_locations WHERE event_id = chain.id
             UNION
             SELECT event_id, event_type FROM core.live_equivalence WHERE event_id = chain.id
             OFFSET 0) live"#,
        roots
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|member| (member.id, member.event_type))
    .collect())
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
