//! Replay's statements. A replay re-reads stored materials with the current
//! parsers and rules: the readings it re-reads are archived and the new ones
//! admitted, one transaction per part sequence, so a failure changes nothing
//! and a crash leaves every sequence either replayed or untouched. Finished sequences
//! are journaled; a resumed replay skips them.
//!
//! A comparison is the same transaction rolled back: the writer runs
//! the whole replay, reads the [`Diff`] between the archived and the new live
//! readings, and discards it, so a parser, rule or derivation change is seen
//! exactly as applying it would make it, without replacing anything.
use crate::Result;
use serde_json::Value;
use sinex_model::OperationId;
use sqlx::PgConnection;
use uuid::Uuid;

/// Record the replay operation (idempotent: every sequence's transaction calls
/// it, so the row exists once any sequence commits).
pub async fn begin(
    conn: &mut PgConnection,
    operation: OperationId,
    actor: &str,
    scope: &Value,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO core.operations (id, kind, actor, scope) VALUES ($1, 'replay', $2, $3) ON CONFLICT (id) DO NOTHING",
        operation.0,
        actor,
        scope,
    )
    .execute(&mut *conn)
    .await?;
    let saved = sqlx::query!(
        "SELECT kind,actor,scope FROM core.operations WHERE id=$1",
        operation.0
    )
    .fetch_one(&mut *conn)
    .await?;
    if saved.kind != "replay" || saved.actor != actor || saved.scope != *scope {
        return Err(crate::Error::Invalid(
            "replay_build_changed: operation scope differs".into(),
        ));
    }
    Ok(())
}

/// Mark a sequence replayed, in the sequence's own transaction.
pub async fn sequence_done(
    conn: &mut PgConnection,
    operation: OperationId,
    root: Uuid,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO core.replayed_sequences (operation_id, first_part) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        operation.0,
        root,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Sequences an earlier run of this operation already replayed.
pub async fn sequences_done(conn: &mut PgConnection, operation: OperationId) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar!(
        "SELECT first_part FROM core.replayed_sequences WHERE operation_id = $1",
        operation.0
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// The scope an operation was started with, for resuming it.
pub async fn scope_of(conn: &mut PgConnection, operation: OperationId) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar!(
        "SELECT scope FROM core.operations WHERE id = $1 AND kind = 'replay'",
        operation.0
    )
    .fetch_optional(&mut *conn)
    .await?)
}

/// One record whose live reading a replay would change.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub event_type: String,
    /// The equivalence key, or `material:anchor:output_index` for keyless types.
    pub record: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

/// What a replay changes in the live set.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Diff {
    pub unchanged: usize,
    /// Records whose payload differs, records only the new reading has, and
    /// records the new reading no longer produces.
    pub changed: Vec<Change>,
    pub added: Vec<Change>,
    pub removed: Vec<Change>,
}

/// Compare the readings `operation` archived with the new readings `admitted`
/// that are live now. Run inside the replay's transaction, before commit or
/// rollback.
pub async fn diff(
    conn: &mut PgConnection,
    operation: OperationId,
    admitted: &[Uuid],
) -> Result<Diff> {
    let rows = sqlx::query!(
        r#"WITH old AS (
             SELECT e.source, e.event_type,
               coalesce(e.equivalence_key, e.material_id::text || ':' || e.anchor || ':' || e.output_index) AS record,
               e.payload
             FROM audit.archived a JOIN core.events e ON e.id = a.event_id
             WHERE a.operation_id = $1 AND a.reason = 'replay'),
           new AS (
             SELECT e.source, e.event_type,
               coalesce(e.equivalence_key, e.material_id::text || ':' || e.anchor || ':' || e.output_index) AS record,
               e.payload
             FROM core.events e
             WHERE e.id = ANY($2)
               AND (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
                 OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)))
           SELECT coalesce(o.event_type, n.event_type) AS "event_type!",
                  coalesce(o.record, n.record) AS "record!",
                  o.payload AS "before?", n.payload AS "after?"
           FROM old o FULL JOIN new n
             ON o.source = n.source AND o.event_type = n.event_type AND o.record = n.record
           ORDER BY 1, 2"#,
        operation.0,
        admitted,
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut diff = Diff::default();
    for r in rows {
        let change = Change {
            event_type: r.event_type,
            record: r.record,
            before: r.before,
            after: r.after,
        };
        match (&change.before, &change.after) {
            (Some(a), Some(b)) if a == b => diff.unchanged += 1,
            (Some(_), Some(_)) => diff.changed.push(change),
            (None, Some(_)) => diff.added.push(change),
            (Some(_), None) => diff.removed.push(change),
            (None, None) => {}
        }
    }
    Ok(diff)
}

/// A stored material, as replay re-reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    pub id: Uuid,
    pub input: Option<Uuid>,
    pub previous: Option<Uuid>,
    pub part_index: i32,
    pub blob: String,
    pub length: i64,
    pub parser: String,
    pub parser_version: i32,
    pub manifest: sinex_model::Manifest,
    pub binding: Option<String>,
    pub host: Option<String>,
}

/// Materials in scope: the named ones, a binding's, or (when neither is
/// given) those with readings of the given types in the given window.
pub async fn select(
    conn: &mut PgConnection,
    materials: &[Uuid],
    binding: Option<&str>,
    event_types: Option<&[String]>,
    from: Option<time::OffsetDateTime>,
    until: Option<time::OffsetDateTime>,
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT m.id FROM raw.materials m LEFT JOIN raw.inputs i ON i.id = m.input_id
           WHERE m.id = ANY($1)
              OR ($2::text IS NOT NULL AND i.binding = $2)
              OR (cardinality($1) = 0 AND $2::text IS NULL AND EXISTS (
                    SELECT 1 FROM core.events e WHERE e.material_id = m.id
                      AND ($3::text[] IS NULL OR e.event_type = ANY($3))
                      AND ($4::timestamptz IS NULL OR e.ts_orig >= $4)
                      AND ($5::timestamptz IS NULL OR e.ts_orig < $5)))
           ORDER BY m.id"#,
        materials,
        binding,
        event_types,
        from,
        until,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Every material of the given inputs, plus the given input-less materials,
/// so the caller can walk each sequence from its first part.
pub async fn materials_of(
    conn: &mut PgConnection,
    inputs: &[Uuid],
    loose: &[Uuid],
) -> Result<Vec<Stored>> {
    sqlx::query!(
        r#"SELECT m.id, m.input_id, m.previous_part_id, m.part_index, m.blob, m.length, m.parser, m.parser_version,
                  m.manifest, i.binding AS "binding?", i.host AS "host?"
           FROM raw.materials m LEFT JOIN raw.inputs i ON i.id = m.input_id
           WHERE m.input_id = ANY($1) OR m.id = ANY($2)
           ORDER BY m.input_id, m.part_index, m.id"#,
        inputs,
        loose,
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| {
        Ok(Stored {
        id: r.id,
        input: r.input_id,
        previous: r.previous_part_id,
        part_index: r.part_index,
        blob: r.blob,
        length: r.length,
        parser: r.parser,
        parser_version: r.parser_version,
        manifest: serde_json::from_value(r.manifest)
            .map_err(|e| crate::Error::Invalid(format!("material {}: manifest: {e}", r.id)))?,
        binding: r.binding,
        host: r.host,
        })
    })
    .collect()
}

/// The inputs of the given materials.
pub async fn inputs_of(conn: &mut PgConnection, materials: &[Uuid]) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT DISTINCT input_id AS "input!" FROM raw.materials WHERE id = ANY($1) AND input_id IS NOT NULL"#,
        materials
    )
    .fetch_all(&mut *conn)
    .await?)
}

#[cfg(test)]
mod tests;

/// Part links and digests of materials this producer registered.
pub async fn producer_materials(
    conn: &mut PgConnection,
    producer: &str,
) -> Result<Vec<(Uuid, Option<Uuid>, String)>> {
    let rows = sqlx::query!("SELECT DISTINCT m.id,m.previous_part_id,m.blob FROM raw.materials m JOIN core.ingress_frames f ON f.material_id=m.id WHERE f.producer=$1 ORDER BY m.id",producer).fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.id, row.previous_part_id, row.blob))
        .collect())
}

/// The live readings in the exact producer replay scope, sorted by ID.
pub async fn producer_live_hash(
    conn: &mut PgConnection,
    producer: &str,
    scope: &sinex_model::ReplayScope,
) -> Result<(u64, String)> {
    let ids: Vec<_> = scope.materials.iter().map(|m| m.0).collect();
    let rows = sqlx::query!(
        "SELECT e.id,e.record_hash FROM core.events e WHERE e.producer_family=$1 AND e.material_id=ANY($2)
      AND ($3::text[] IS NULL OR e.event_type=ANY($3))
      AND ($4::timestamptz IS NULL OR e.ts_orig >= $4)
      AND ($5::timestamptz IS NULL OR e.ts_orig < $5)
      AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY e.id",
        producer,
        &ids,
        scope.event_types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0)
    )
    .fetch_all(conn)
    .await?;
    let mut hash = blake3::Hasher::new();
    for row in &rows {
        hash.update(row.id.as_bytes());
        hash.update(&row.record_hash);
    }
    Ok((rows.len() as u64, hash.finalize().to_hex().to_string()))
}

/// Verify the committed replacement has no readings outside its fixed scope.
pub async fn producer_outputs_in_scope(
    conn: &mut PgConnection,
    producer: &str,
    ids: &[Uuid],
    scope: &sinex_model::ReplayScope,
) -> Result<bool> {
    let materials: Vec<_> = scope.materials.iter().map(|m| m.0).collect();
    let outside = sqlx::query_scalar!(
        r#"SELECT EXISTS(SELECT 1 FROM core.events e WHERE e.id=ANY($1)
      AND (e.producer_family IS DISTINCT FROM $2 OR e.material_id IS NULL OR NOT e.material_id=ANY($3)
        OR ($4::text[] IS NOT NULL AND NOT e.event_type=ANY($4))
        OR ($5::timestamptz IS NOT NULL AND (e.ts_orig IS NULL OR e.ts_orig<$5))
        OR ($6::timestamptz IS NOT NULL AND (e.ts_orig IS NULL OR e.ts_orig>=$6)))) AS "outside!""#,
        ids,
        producer,
        &materials,
        scope.event_types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0)
    )
    .fetch_one(conn)
    .await?;
    Ok(!outside)
}

/// Refusals are stored even when no sequence can be parsed.
pub async fn record_refusals(
    conn: &mut PgConnection,
    operation: OperationId,
    refusals: &[Value],
) -> Result<()> {
    for refusal in refusals {
        let material: Uuid = serde_json::from_value(refusal["material"].clone())
            .map_err(|_| crate::Error::Invalid("replay refusal has no material".into()))?;
        sqlx::query!("INSERT INTO core.replay_refusals (operation_id,material_id,refusal) VALUES ($1,$2,$3) ON CONFLICT (operation_id,material_id) DO UPDATE SET refusal=EXCLUDED.refusal",operation.0,material,refusal).execute(&mut *conn).await?;
    }
    Ok(())
}
