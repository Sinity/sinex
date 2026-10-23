//! Excision: the one true deletion (architecture §3.4; spec `storage.md` §9).
//! Operator only. It removes every reading of a record, live and archived,
//! with every derived event that descends from them, and the record's bytes:
//!
//! - an open or guarded material gets a new blob in which the excised records
//!   are blanked to spaces of the same length (the closing newline kept), so
//!   every other record keeps its anchor; the old blob is deleted once nothing
//!   refers to it;
//! - a sealed material is crypto-shredded: its wrapped data key is deleted and
//!   the bytes stay, unreadable. Only a whole sealed material can go this way,
//!   because a sealed blob cannot be rewritten without its key;
//! - a tombstone (identity and time, no payload) remains for each event, the
//!   record's key or location goes into `core.hidden_occurrences` as `excise`,
//!   which nothing undoes, and its record hash into `core.excised_digests`, so
//!   neither a replay nor re-staging the same bytes brings it back;
//! - stored bytes the excised events own beyond their materials (a library
//!   file the scan kept, `core.event_blobs`) are deleted once no other event
//!   or material refers to them.
//!
//! Planning reads; the writer applies the plan in one transaction, checking
//! that the readings it removes are still exactly the planned ones, and
//! deletes the old blob files after commit.
use crate::admission::{DirtyKeys, mark_archived_dirty, remove_live};
use crate::{Error, Result};
use sinex_model::{EventId, MaterialId, OperationId};
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The record this event was read from, with every reading of it.
    Event(EventId),
    /// Every record of a material, and its bytes.
    Material(MaterialId),
}

/// A material whose records an excision blanks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Material {
    pub id: MaterialId,
    pub blob: String,
    /// The parser and host that read it: how the caller finds its framing.
    pub parser: String,
    pub host: String,
    pub manifest: serde_json::Value,
    /// Anchors of the records to blank; empty when `whole`.
    pub anchors: BTreeSet<i64>,
    pub whole: bool,
    pub sealed: bool,
}

/// What an excision removes, as planned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub events: BTreeSet<Uuid>,
    pub materials: Vec<Material>,
}

/// A material's replacement bytes, written to the content store before the
/// plan is applied (bytes before rows).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rewrite {
    pub material: MaterialId,
    pub blob: String,
    pub length: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub events: usize,
    pub materials_rewritten: usize,
    pub keys_destroyed: usize,
    /// Stored bytes the excised events owned that nothing else refers to.
    pub blobs_deleted: usize,
    pub dirty_keys: usize,
    /// Blob files no row refers to any more; the writer deletes them after
    /// commit.
    pub delete_after_commit: Vec<String>,
}

/// Find every reading the excision of `target` removes. Read-only.
pub async fn plan(conn: &mut PgConnection, target: Target) -> Result<Plan> {
    let seeds: Vec<Uuid> = match target {
        Target::Event(id) => {
            let e = sqlx::query!(
                "SELECT source, event_type, equivalence_key, material_id, anchor, ordinal FROM core.events WHERE id = $1",
                id.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {id}")))?;
            // Every reading of the record: by key, or by location for keyless types.
            sqlx::query_scalar!(
                r#"SELECT id AS "id!" FROM core.events
                   WHERE source = $1 AND event_type = $2
                   AND ((equivalence_key = $3) OR ($3::text IS NULL AND material_id = $4 AND anchor = $5 AND ordinal = $6))"#,
                e.source,
                e.event_type,
                e.equivalence_key,
                e.material_id,
                e.anchor,
                e.ordinal,
            )
            .fetch_all(&mut *conn)
            .await?
        }
        Target::Material(m) => {
            sqlx::query_scalar!(
                r#"SELECT id AS "id!" FROM core.events WHERE material_id = $1"#,
                m.0
            )
            .fetch_all(&mut *conn)
            .await?
        }
    };
    // A record's bytes hold all its events, so every event read from the same
    // record goes too; then every derived event that descends from any of them.
    let events = sqlx::query_scalar!(
        r#"WITH RECURSIVE located AS (
             SELECT e.id FROM core.events e
             WHERE e.id = ANY($1)
                OR (e.material_id, e.anchor) IN (
                     SELECT s.material_id, s.anchor FROM core.events s
                     WHERE s.id = ANY($1) AND s.material_id IS NOT NULL)),
           descendants AS (
             SELECT id FROM located
             UNION
             SELECT d.id FROM core.events d JOIN descendants p ON d.parents @> ARRAY[p.id])
           SELECT id AS "id!" FROM descendants"#,
        &seeds,
    )
    .fetch_all(&mut *conn)
    .await?;
    let events: BTreeSet<Uuid> = events.into_iter().collect();
    let ids: Vec<Uuid> = events.iter().copied().collect();

    let rows = sqlx::query!(
        r#"SELECT DISTINCT e.material_id AS "material!", e.anchor AS "anchor!", m.blob, m.parser, m.manifest,
             i.host AS "host?", (k.material_id IS NOT NULL) AS "sealed!"
           FROM core.events e
           JOIN raw.materials m ON m.id = e.material_id
           LEFT JOIN raw.inputs i ON i.id = m.input_id
           LEFT JOIN privacy.material_keys k ON k.material_id = m.id
           WHERE e.id = ANY($1) AND e.material_id IS NOT NULL"#,
        &ids,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut materials: BTreeMap<Uuid, Material> = BTreeMap::new();
    for r in rows {
        let m = materials.entry(r.material).or_insert_with(|| Material {
            id: MaterialId(r.material),
            blob: r.blob.clone(),
            parser: r.parser.clone(),
            host: r.host.clone().unwrap_or_default(),
            manifest: r.manifest.clone(),
            anchors: BTreeSet::new(),
            whole: matches!(target, Target::Material(t) if t.0 == r.material),
            sealed: r.sealed,
        });
        if !m.whole {
            m.anchors.insert(r.anchor);
        }
    }
    if let Target::Material(m) = target
        && !materials.contains_key(&m.0)
    {
        // A material with no events left (all suppressed or failed) still has bytes.
        let r = sqlx::query!(
            r#"SELECT m.blob, m.parser, m.manifest, i.host AS "host?", (k.material_id IS NOT NULL) AS "sealed!"
               FROM raw.materials m LEFT JOIN raw.inputs i ON i.id = m.input_id
               LEFT JOIN privacy.material_keys k ON k.material_id = m.id WHERE m.id = $1"#,
            m.0
        )
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(|| Error::Invalid(format!("no material {m}")))?;
        materials.insert(
            m.0,
            Material {
                id: m,
                blob: r.blob,
                parser: r.parser,
                host: r.host.unwrap_or_default(),
                manifest: r.manifest,
                anchors: BTreeSet::new(),
                whole: true,
                sealed: r.sealed,
            },
        );
    }
    // Record bodies do not replace their original archive. Blanking a decoded
    // part alone would leave the same private bytes in its captured ancestor.
    let material_ids: Vec<Uuid> = materials.keys().copied().collect();
    crate::sqlite_images::guard_excision(conn, &material_ids).await?;
    let retained_archive = sqlx::query_scalar!(
        r#"WITH RECURSIVE ancestors AS (
             SELECT id, previous_part_id, manifest FROM raw.materials WHERE id = ANY($1)
             UNION
             SELECT m.id, m.previous_part_id, m.manifest FROM raw.materials m
               JOIN ancestors a ON m.id = a.previous_part_id)
           SELECT id AS "id!" FROM ancestors WHERE manifest->>'role' = 'original-archive' LIMIT 1"#,
        &material_ids,
    )
    .fetch_optional(&mut *conn)
    .await?;
    if let Some(id) = retained_archive {
        return Err(Error::Invalid(format!(
            "material {id} retains an original archive: excision needs a cross-binding archive rewrite, which is not supported"
        )));
    }
    for m in materials.values() {
        if m.sealed && !m.whole {
            return Err(Error::Invalid(format!(
                "material {} is sealed: only the whole material can be excised (by destroying its key); excise material:{}",
                m.id, m.id
            )));
        }
    }
    Ok(Plan {
        events,
        materials: materials.into_values().collect(),
    })
}

/// Blank records in `bytes`: every byte of each `(start, end)` span becomes a
/// space, except a closing newline, so framing and every other anchor stay as
/// they were. A record that is all spaces is no record (the drain skips it).
pub fn blank(bytes: &mut [u8], spans: &[(usize, usize)]) {
    for &(start, end) in spans {
        let end = end.min(bytes.len());
        let keep_newline = end > start && bytes[end - 1] == b'\n';
        let stop = if keep_newline { end - 1 } else { end };
        for b in &mut bytes[start..stop] {
            *b = b' ';
        }
    }
}

/// Apply `plan` inside the writer's transaction. `rewrites` holds the new
/// blob for every planned material that is not sealed.
pub async fn apply(
    conn: &mut PgConnection,
    target: Target,
    plan: &Plan,
    rewrites: &[Rewrite],
    operation: OperationId,
    actor: &str,
    keys: &dyn DirtyKeys,
) -> Result<Applied> {
    let now = self::plan(conn, target).await?;
    if now != *plan {
        return Err(Error::Invalid(
            "the readings to excise changed after planning; plan the excision again".into(),
        ));
    }
    let ids: Vec<Uuid> = plan.events.iter().copied().collect();
    let mut applied = Applied::default();

    sqlx::query!(
        "INSERT INTO core.operations (id, kind, actor, scope) VALUES ($1, 'excise', $2, $3)",
        operation.0,
        actor,
        match target {
            Target::Event(e) => serde_json::json!({"event": e.0}),
            Target::Material(m) => serde_json::json!({"material": m.0}),
        },
    )
    .execute(&mut *conn)
    .await?;

    // Remembered, so neither replay nor re-import brings any of it back.
    sqlx::query!(
        r#"INSERT INTO core.hidden_occurrences (source, event_type, equivalence_key, kind, operation_id)
           SELECT DISTINCT source, event_type, equivalence_key, 'excise', $2::uuid FROM core.events
           WHERE id = ANY($1) AND equivalence_key IS NOT NULL
           ON CONFLICT (source, event_type, equivalence_key) WHERE equivalence_key IS NOT NULL
           DO UPDATE SET kind = 'excise', operation_id = EXCLUDED.operation_id"#,
        &ids,
        operation.0,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO core.hidden_occurrences (source, event_type, material_id, anchor, ordinal, kind, operation_id)
           SELECT DISTINCT source, event_type, material_id, anchor, ordinal, 'excise', $2::uuid FROM core.events
           WHERE id = ANY($1) AND equivalence_key IS NULL AND material_id IS NOT NULL
           ON CONFLICT (material_id, anchor, event_type, ordinal) WHERE material_id IS NOT NULL
           DO UPDATE SET kind = 'excise', operation_id = EXCLUDED.operation_id"#,
        &ids,
        operation.0,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO core.excised_digests (source, event_type, record_hash, operation_id)
           SELECT DISTINCT source, event_type, record_hash, $2::uuid FROM core.events
           WHERE id = ANY($1) AND material_id IS NOT NULL
           ON CONFLICT DO NOTHING"#,
        &ids,
        operation.0,
    )
    .execute(&mut *conn)
    .await?;

    // Derivations stop counting what goes: the live readings dirty their keys.
    let live: Vec<Uuid> = sqlx::query_scalar!(
        r#"SELECT event_id AS "id!" FROM core.live_equivalence WHERE event_id = ANY($1)
           UNION SELECT event_id FROM core.live_locations WHERE event_id = ANY($1)"#,
        &ids
    )
    .fetch_all(&mut *conn)
    .await?;
    let seq = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
        .fetch_one(&mut *conn)
        .await?;
    applied.dirty_keys = mark_archived_dirty(conn, &live, seq, keys).await?;
    remove_live(conn, &ids).await?;

    sqlx::query!(
        r#"INSERT INTO core.event_tombstones (event_id, source, event_type, ts_orig, operation_id)
           SELECT id, source, event_type, ts_orig, $2 FROM core.events WHERE id = ANY($1)
           ON CONFLICT DO NOTHING"#,
        &ids,
        operation.0,
    )
    .execute(&mut *conn)
    .await?;
    // Read models hold payload text; it goes with the events.
    sqlx::query!(
        "DELETE FROM core.search_text WHERE event_id = ANY($1)",
        &ids
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!("DELETE FROM audit.archived WHERE event_id = ANY($1)", &ids)
        .execute(&mut *conn)
        .await?;
    sqlx::query!("DELETE FROM core.commit_log WHERE event_id = ANY($1)", &ids)
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        "SELECT set_config('sinex.excise_operation', $1, true)",
        operation.0.to_string()
    )
    .fetch_one(&mut *conn)
    .await?;
    let deleted = sqlx::query!("DELETE FROM core.events WHERE id = ANY($1)", &ids)
        .execute(&mut *conn)
        .await?;
    sqlx::query!("SELECT set_config('sinex.excise_operation', '', true)")
        .fetch_one(&mut *conn)
        .await?;
    applied.events = usize::try_from(deleted.rows_affected()).unwrap_or(usize::MAX);

    // Bytes the excised events owned: gone with them unless something else
    // (another event, a material, a revision) still refers to them.
    let cited: Vec<String> = sqlx::query_scalar!(
        "DELETE FROM core.event_blobs WHERE event_id = ANY($1) RETURNING blob",
        &ids
    )
    .fetch_all(&mut *conn)
    .await?;
    let cited: BTreeSet<String> = cited.into_iter().collect();
    for blob in cited {
        if unreferenced(conn, &blob).await? {
            sqlx::query!(
                "UPDATE core.blobs SET status = 'deleting' WHERE hash = $1",
                blob
            )
            .execute(&mut *conn)
            .await?;
            applied.blobs_deleted += 1;
            applied.delete_after_commit.push(blob);
        }
    }

    for m in &plan.materials {
        if m.sealed {
            let gone = sqlx::query!(
                "DELETE FROM privacy.material_keys WHERE material_id = $1",
                m.id.0
            )
            .execute(&mut *conn)
            .await?;
            applied.keys_destroyed += usize::try_from(gone.rows_affected()).unwrap_or(0);
            continue;
        }
        let rewrite = rewrites
            .iter()
            .find(|r| r.material == m.id)
            .ok_or_else(|| Error::Invalid(format!("no replacement bytes for material {}", m.id)))?;
        let length = i64::try_from(rewrite.length)
            .map_err(|_| Error::Invalid("material length beyond i64".into()))?;
        sqlx::query!(
            r#"INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present')
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status = 'deleting'"#,
            rewrite.blob,
            length,
        )
        .execute(&mut *conn)
        .await?;
        sqlx::query!(
            r#"UPDATE raw.revision_extents x SET blob = $2
               FROM raw.materials m WHERE m.id = $1 AND x.revision_id = m.revision_id AND x.blob = m.blob"#,
            m.id.0,
            rewrite.blob,
        )
        .execute(&mut *conn)
        .await?;
        sqlx::query!(
            "UPDATE raw.materials SET blob = $2 WHERE id = $1",
            m.id.0,
            rewrite.blob
        )
        .execute(&mut *conn)
        .await?;
        applied.materials_rewritten += 1;
        // The old bytes go once nothing refers to them; deduplication may
        // share a blob with a material outside the excision.
        if m.blob != rewrite.blob && unreferenced(conn, &m.blob).await? {
            sqlx::query!(
                "UPDATE core.blobs SET status = 'deleting' WHERE hash = $1",
                m.blob
            )
            .execute(&mut *conn)
            .await?;
            applied.delete_after_commit.push(m.blob.clone());
        }
    }
    Ok(applied)
}

/// Whether no material, revision extent or event refers to `blob` any more.
async fn unreferenced(conn: &mut PgConnection, blob: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT NOT EXISTS (SELECT 1 FROM raw.materials WHERE blob = $1)
             AND NOT EXISTS (SELECT 1 FROM raw.revision_extents WHERE blob = $1)
             AND NOT EXISTS (SELECT 1 FROM core.event_blobs WHERE blob = $1) AS "gone!""#,
        blob
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// Delete the files of blobs left `deleting`, then their rows: after an
/// excision commits, and at daemon start for any a crash left behind. A blob
/// registered again meanwhile is `present` and is kept, as is one a row
/// refers to again. Returns the blobs deleted and those whose file could not
/// be deleted (their rows stay for the next attempt).
pub async fn finish_deletions(
    conn: &mut PgConnection,
    blobs: &crate::Blobs,
) -> Result<(Vec<String>, Vec<String>)> {
    let pending = sqlx::query_scalar!(
        r#"SELECT hash FROM core.blobs b WHERE status = 'deleting'
             AND NOT EXISTS (SELECT 1 FROM raw.materials WHERE blob = b.hash)
             AND NOT EXISTS (SELECT 1 FROM raw.revision_extents WHERE blob = b.hash)
             AND NOT EXISTS (SELECT 1 FROM core.event_blobs WHERE blob = b.hash)"#
    )
    .fetch_all(&mut *conn)
    .await?;
    let (mut deleted, mut left) = (Vec::new(), Vec::new());
    for name in pending {
        if blobs.delete(&name).is_ok() {
            sqlx::query!(
                "DELETE FROM core.blobs WHERE hash = $1 AND status = 'deleting'",
                name
            )
            .execute(&mut *conn)
            .await?;
            deleted.push(name);
        } else {
            left.push(name);
        }
    }
    Ok((deleted, left))
}

#[cfg(test)]
mod tests;
