//! Restore per-event readings from the same explicit archive operation.
use crate::admission::derived::{mark_dirty, mark_inputs_dirty};
use crate::admission::{DirtyKeys, KeyInput};
use crate::{Error, Result};
use sqlx::PgConnection;
use uuid::Uuid;

pub(super) async fn reading(
    conn: &mut PgConnection,
    event: Uuid,
    seq: i64,
    operation: Uuid,
) -> Result<()> {
    let allowed = sqlx::query_scalar!(
        r#"SELECT NOT EXISTS (
             SELECT 1 FROM unnest(e.parents) p(id)
             WHERE NOT EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id=p.id)
               AND NOT EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id=p.id))
           AND NOT EXISTS (
             SELECT 1 FROM core.hidden_records h WHERE h.source=e.source AND h.event_type=e.event_type
               AND (h.equivalence_key=e.equivalence_key OR (e.equivalence_key IS NULL
                    AND h.material_id=e.material_id AND h.anchor=e.anchor AND h.output_index=e.output_index))) AS "allowed!"
           FROM core.events e WHERE e.id=$1"#,
        event,
    ).fetch_optional(&mut *conn).await?.unwrap_or(false);
    if !allowed {
        return Err(Error::Invalid(format!("cannot restore {event}: a parent is not live or the record is hidden")));
    }
    if !super::history::unhide_at(conn, event, seq, operation).await? {
        return Err(Error::Invalid("hidden reading changed before it was restored".into()));
    }
    sqlx::query!(
        r#"INSERT INTO core.live_equivalence(source,event_type,equivalence_key,event_id,reading_order,record_hash)
           SELECT source,event_type,equivalence_key,id,reading_order,record_hash FROM core.events
           WHERE id=$1 AND equivalence_key IS NOT NULL"#, event,
    ).execute(&mut *conn).await?;
    sqlx::query!(
        r#"INSERT INTO core.live_locations(material_id,anchor,event_type,output_index,event_id,reading_order,record_hash)
           SELECT material_id,anchor,event_type,output_index,id,reading_order,record_hash FROM core.events
           WHERE id=$1 AND equivalence_key IS NULL"#, event,
    ).execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn descendants(
    conn: &mut PgConnection,
    root: Uuid,
    archived_by: Uuid,
    seq: i64,
    operation: Uuid,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let candidates = sqlx::query_scalar!(
        r#"WITH RECURSIVE descendants(id) AS (
             SELECT child FROM core.event_children WHERE parent=$1
             UNION SELECT edge.child FROM core.event_children edge JOIN descendants d ON edge.parent=d.id)
           SELECT d.id AS "id!" FROM descendants d JOIN audit.archived a ON a.event_id=d.id
           WHERE a.reason='hide' AND a.operation_id=$2"#, root, archived_by,
    ).fetch_all(&mut *conn).await?;
    let owners = sqlx::query!(
        "SELECT DISTINCT derivation,key FROM derivation.outputs WHERE event_id=ANY($1)", &candidates,
    ).fetch_all(&mut *conn).await?;
    let mut dirty = mark_dirty(conn, owners.into_iter().map(|row|
        (crate::derivations::static_type(&row.derivation), row.key)).collect(), seq).await?;
    // Keyed aggregates and their descendants get new readings from recomputation.
    let blocked = sqlx::query_scalar!(
        r#"WITH RECURSIVE blocked(id) AS (
             SELECT event_id FROM derivation.outputs WHERE event_id=ANY($1)
             UNION SELECT edge.child FROM core.event_children edge JOIN blocked b ON edge.parent=b.id)
           SELECT id AS "id!" FROM blocked"#, &candidates,
    ).fetch_all(&mut *conn).await?.into_iter().collect::<std::collections::HashSet<_>>();
    let mut pending: Vec<_> = candidates.into_iter().filter(|id| !blocked.contains(id)).collect();
    while !pending.is_empty() {
        let ready = sqlx::query!(
            r#"SELECT e.id,e.event_type,e.ts_orig,e.payload FROM core.events e
               WHERE e.id=ANY($1) AND NOT EXISTS (
                 SELECT 1 FROM unnest(e.parents) p(id)
                 WHERE NOT EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id=p.id)
                   AND NOT EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id=p.id))"#, &pending,
        ).fetch_all(&mut *conn).await?;
        if ready.is_empty() {
            return Err(Error::Invalid("cannot restore the derived chain: a parent is not live".into()));
        }
        for event in ready {
            reading(conn, event.id, seq, operation).await?;
            dirty += mark_inputs_dirty(conn, &[KeyInput { event_type:&event.event_type,
                ts_orig:event.ts_orig, payload:&event.payload }], seq, keys).await?;
            pending.retain(|id| *id != event.id);
        }
    }
    Ok(dirty)
}
