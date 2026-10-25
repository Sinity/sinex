//! Excision: the one true deletion. Operator only. It removes every reading of
//! one record, live and archived, every derived event that descends from them,
//! and the record's bytes, and nothing else:
//!
//! - each material a reading was read from gets a new blob of spaces of the
//!   same length (the caller writes it, and refuses a material that holds
//!   other records too); the old blob is deleted once nothing refers to it,
//!   and kept while another material shares it;
//! - a tombstone (identity and time, no payload) remains for each event;
//! - stored bytes the excised events cite beyond their materials
//!   (`core.event_blobs`) are deleted once nothing else refers to them.
//!
//! [`plan`] refuses, before anything changes, what it cannot remove cleanly:
//! an event that was not read from a stored record, sealed material, and a
//! material whose bytes another part of the same revision shares. Whether the
//! source or producer may excise the planned materials is decided by the
//! caller, which knows the sources.
//!
//! Each excision is recorded once in `core.operations`. The writer applies the
//! plan in one transaction, checking that the readings it removes are still
//! exactly the planned ones, and deletes the old blob files after commit.
//! Running the same request again after a crash finds what is left and
//! finishes it.
use crate::admission::{DirtyKeys, mark_archived_dirty, remove_live};
use crate::{Error, Result};
use sinex_model::{EventId, MaterialId, OperationId};
use sqlx::{Connection, PgConnection};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The record this event was read from, with every reading of it.
    Event(EventId),
    /// Every record of a material, and its bytes.
    Material(MaterialId),
}

/// A material an excision blanks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Material {
    pub id: MaterialId,
    pub blob: String,
    pub length: u64,
    /// Who registered it: the binding, or `ingress:<producer>` for an outside
    /// producer's material.
    pub binding: Option<String>,
    /// The source of its binding; `None` for a producer's material.
    pub source: Option<String>,
    /// The parser that read it: with the source, how the caller finds its
    /// framing.
    pub parser: String,
    pub manifest: serde_json::Value,
    /// Anchors of the excised records; empty when `whole`.
    pub anchors: BTreeSet<i64>,
    /// The material itself is the target.
    pub whole: bool,
}

/// What an excision removes, as planned.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub failures_removed: usize,
    pub materials_rewritten: usize,
    /// Stored bytes the excised events cited that nothing else refers to.
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
                "SELECT source, event_type, equivalence_key, material_id, anchor, output_index FROM core.events WHERE id = $1",
                id.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {id}")))?;
            if e.material_id.is_none() {
                return Err(Error::Invalid(format!(
                    "event {id} was not read from a stored record; excise the records it was derived from"
                )));
            }
            // Every reading of the record: by key, or by location for keyless types.
            sqlx::query_scalar!(
                r#"SELECT id AS "id!" FROM core.events
                   WHERE source = $1 AND event_type = $2
                   AND ((equivalence_key = $3) OR ($3::text IS NULL AND material_id = $4 AND anchor = $5 AND output_index = $6))"#,
                e.source,
                e.event_type,
                e.equivalence_key,
                e.material_id,
                e.anchor,
                e.output_index,
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

    // The records to blank, by material; a whole material when it is the target.
    let mut anchors: BTreeMap<Uuid, BTreeSet<i64>> = BTreeMap::new();
    for r in sqlx::query!(
        r#"SELECT DISTINCT material_id AS "material!", anchor AS "anchor!" FROM core.events
           WHERE id = ANY($1) AND material_id IS NOT NULL"#,
        &ids,
    )
    .fetch_all(&mut *conn)
    .await?
    {
        anchors.entry(r.material).or_default().insert(r.anchor);
    }
    let whole = match target {
        Target::Material(m) => Some(m.0),
        Target::Event(_) => None,
    };
    if let Some(m) = whole {
        anchors.entry(m).or_default();
    }
    let material_ids: Vec<Uuid> = anchors.keys().copied().collect();
    let rows = sqlx::query!(
        r#"SELECT m.id, m.blob, m.length, m.parser, m.manifest, i.binding AS "binding?", b.source AS "source?",
             EXISTS(SELECT 1 FROM privacy.data_keys k WHERE k.material_id = m.id) AS "sealed!"
           FROM raw.materials m
           LEFT JOIN raw.inputs i ON i.id = m.input_id
           LEFT JOIN raw.bindings b ON b.binding = i.binding
           WHERE m.id = ANY($1)
           ORDER BY m.id"#,
        &material_ids,
    )
    .fetch_all(&mut *conn)
    .await?;
    if let Some(missing) = material_ids
        .iter()
        .find(|id| !rows.iter().any(|r| r.id == **id))
    {
        return Err(Error::Invalid(format!("no stored material {missing}")));
    }
    let mut materials = Vec::with_capacity(rows.len());
    for r in rows {
        if r.sealed {
            return Err(Error::Invalid(format!(
                "material {} is sealed: excision of sealed source material is not supported",
                r.id
            )));
        }
        let is_whole = whole == Some(r.id);
        materials.push(Material {
            id: MaterialId(r.id),
            blob: r.blob,
            length: u64::try_from(r.length)
                .map_err(|_| Error::Invalid(format!("material {} has a negative length", r.id)))?,
            binding: r.binding,
            source: r.source,
            parser: r.parser,
            manifest: r.manifest,
            anchors: if is_whole {
                BTreeSet::new()
            } else {
                anchors.remove(&r.id).unwrap_or_default()
            },
            whole: is_whole,
        });
    }
    Ok(Plan { events, materials })
}

/// Apply `plan` inside the writer's transaction. `rewrites` holds the new
/// blob for every planned material.
pub async fn apply(
    conn: &mut PgConnection,
    target: Target,
    plan: &Plan,
    rewrites: &[Rewrite],
    operation: OperationId,
    actor: &str,
    keys: &dyn DirtyKeys,
) -> Result<Applied> {
    if self::plan(conn, target).await? != *plan {
        return Err(Error::Invalid(
            "the readings to excise changed after planning; plan the excision again".into(),
        ));
    }
    let ids: Vec<Uuid> = plan.events.iter().copied().collect();
    let mut applied = Applied::default();

    let mut scope = match target {
        Target::Event(e) => serde_json::json!({"event": e.0}),
        Target::Material(m) => serde_json::json!({"material": m.0}),
    };
    scope["materials"] = serde_json::json!(plan.materials.iter().map(|m| m.id).collect::<Vec<_>>());
    sqlx::query!(
        "INSERT INTO core.operations (id, kind, actor, scope) VALUES ($1, 'excise', $2, $3)",
        operation.0,
        actor,
        scope
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
    crate::admission::erase_archive_history(conn, &ids).await?;
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
    applied.events = usize::try_from(deleted.rows_affected()).unwrap_or(usize::MAX);

    // Bytes the excised events cited: gone with them unless something else
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
            mark_deleting(conn, &blob).await?;
            applied.blobs_deleted += 1;
            applied.delete_after_commit.push(blob);
        }
    }

    for m in &plan.materials {
        let removed = sqlx::query!("DELETE FROM raw.failures WHERE material_id = $1", m.id.0)
            .execute(&mut *conn)
            .await?;
        applied.failures_removed += usize::try_from(removed.rows_affected()).unwrap_or(usize::MAX);
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
            "UPDATE raw.materials SET blob = $2 WHERE id = $1",
            m.id.0,
            rewrite.blob,
        )
        .execute(&mut *conn)
        .await?;
        applied.materials_rewritten += 1;
        // The old bytes go once nothing refers to them; deduplication may
        // share a blob with a material outside the excision.
        if m.blob != rewrite.blob && unreferenced(conn, &m.blob).await? {
            mark_deleting(conn, &m.blob).await?;
            applied.delete_after_commit.push(m.blob.clone());
        }
    }
    sqlx::query!("SELECT set_config('sinex.excise_operation', '', true)")
        .fetch_one(&mut *conn)
        .await?;
    Ok(applied)
}

async fn mark_deleting(conn: &mut PgConnection, blob: &str) -> Result<()> {
    sqlx::query!(
        "UPDATE core.blobs SET status = 'deleting' WHERE hash = $1",
        blob
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Whether no material or event refers to `blob` any more.
pub(crate) async fn unreferenced(conn: &mut PgConnection, blob: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT NOT EXISTS (SELECT 1 FROM raw.materials WHERE blob = $1)
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
    let waiting: Vec<String> =
        sqlx::query_scalar!("SELECT hash FROM core.blobs WHERE status = 'deleting' ORDER BY hash")
            .fetch_all(&mut *conn)
            .await?;
    let (mut deleted, mut left) = (Vec::new(), Vec::new());
    for name in waiting {
        let mut tx = conn.begin().await?;
        // Each statement reads its own snapshot, so the reference check after
        // the row lock sees every reference committed while it waited; a new
        // reference waits on this lock until the file and the row are gone
        // together. The writer runs this and admission in turn, and admits no
        // row whose file is missing.
        sqlx::query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let status = sqlx::query_scalar!(
            "SELECT status FROM core.blobs WHERE hash = $1 FOR UPDATE",
            name
        )
        .fetch_optional(&mut *tx)
        .await?;
        if status.as_deref() != Some("deleting") || !unreferenced(&mut tx, &name).await? {
            tx.commit().await?;
            continue;
        }
        if blobs.delete(&name).is_err() {
            tx.commit().await?;
            left.push(name);
            continue;
        }
        sqlx::query!(
            "DELETE FROM core.blobs WHERE hash = $1 AND status = 'deleting'",
            name
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        deleted.push(name);
    }
    Ok((deleted, left))
}

#[cfg(test)]
mod tests;
