//! Registering what a batch was read from (input, observation, revision,
//! material) and the acknowledgment of an ingress frame.
use super::*;
use crate::Error;
use sinex_model::{InputId, ObservationId, RevisionChange, RevisionId, SourceProgress, Timestamp};

/// Upsert the input, record the observation, open a revision when the content
/// changed in a way that is not a plain append, and register the material.
/// Bytes are already in the content store, so they exist before any row refers
/// to them.
pub(super) async fn source(conn: &mut PgConnection, p: &SourceProgress) -> Result<()> {
    if let Some(material) = &p.material
        && material_matches(conn, &p.binding, &p.input, material).await? == Some(false)
    {
        return Err(Error::Invalid(
            "material identity already belongs to different stored facts".into(),
        ));
    }

    let input = sqlx::query_scalar!(
        r#"INSERT INTO raw.inputs (id, binding, kind, locator, host) VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (binding, locator, host) DO UPDATE SET kind = EXCLUDED.kind
           RETURNING id"#,
        InputId::new().0,
        p.binding,
        p.input.kind,
        p.input.locator,
        p.input.host,
    )
    .fetch_one(&mut *conn)
    .await?;

    let at = p.observation.observed_at.unwrap_or_else(Timestamp::now).0;
    let o = &p.observation;
    if o.changed || o.unavailable.is_some() {
        sqlx::query!(
            r#"INSERT INTO raw.observations (id, input_id, observed_at, size, mtime_ns, ctime_ns, inode, device, unavailable, last_looked_at)
               VALUES ($1, $2, $3, $4::text::numeric, $5::text::numeric, $6::text::numeric, $7::text::numeric, $8::text::numeric, $9, $3)"#,
            ObservationId::new().0,
            input,
            at,
            o.size.map(|v| v.to_string()),
            o.mtime_ns.map(|v| v.to_string()),
            o.ctime_ns.map(|v| v.to_string()),
            o.inode.map(|v| v.to_string()),
            o.device.map(|v| v.to_string()),
            o.unavailable,
        )
        .execute(&mut *conn)
        .await?;
    } else {
        sqlx::query!(
            r#"UPDATE raw.observations SET unchanged_looks = unchanged_looks + 1, last_looked_at = $2
               WHERE id = (SELECT id FROM raw.observations WHERE input_id = $1 ORDER BY observed_at DESC LIMIT 1)"#,
            input,
            at,
        )
        .execute(&mut *conn)
        .await?;
    }

    let current = sqlx::query_scalar!(
        "SELECT id FROM raw.revisions WHERE input_id = $1 ORDER BY started_at DESC, id DESC LIMIT 1",
        input
    )
    .fetch_optional(&mut *conn)
    .await?;
    for key in &p.data_keys {
        crate::sealed::store_key(conn, key.material, &key.key_ref, &key.wrapped).await?;
    }
    let revision = match (p.revision, current) {
        (RevisionChange::Same | RevisionChange::Appended, Some(current)) => current,
        (change, previous) => {
            let kind = match change {
                RevisionChange::Same | RevisionChange::Appended => "appended",
                RevisionChange::Rewritten => "rewritten",
                RevisionChange::Replaced => "replaced",
                RevisionChange::Truncated => "truncated",
                RevisionChange::Snapshot => "snapshot",
            };
            sqlx::query_scalar!(
                "INSERT INTO raw.revisions (id, input_id, previous_id, kind) VALUES ($1, $2, $3, $4) RETURNING id",
                RevisionId::new().0,
                input,
                previous,
                kind,
            )
            .fetch_one(&mut *conn)
            .await?
        }
    };

    let Some(m) = &p.material else { return Ok(()) };
    material(conn, input, revision, m).await?;

    Ok(())
}

/// Whether a registered material with this ID has these facts; `None` when no
/// material has the ID. Checked before the input's kind is upserted.
async fn material_matches(
    conn: &mut PgConnection,
    binding: &str,
    input: &sinex_model::InputRef,
    material: &sinex_model::MaterialRef,
) -> Result<Option<bool>> {
    let length = i64::try_from(material.length)
        .map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    let part = i32::try_from(material.part_index)
        .map_err(|_| Error::Invalid("material part beyond i32".into()))?;
    let version = i32::try_from(material.parser_version)
        .map_err(|_| Error::Invalid("material parser version beyond i32".into()))?;
    Ok(sqlx::query_scalar!(
        r#"SELECT (m.blob = $2 AND m.length = $3 AND m.part_index = $4
                   AND m.previous_part_id IS NOT DISTINCT FROM $5
                   AND m.parser = $6 AND m.parser_version = $7 AND m.manifest = $8
                   AND i.binding = $9 AND i.kind = $10 AND i.locator = $11 AND i.host = $12) AS "matches!"
           FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id WHERE m.id = $1"#,
        material.id.0,
        material.blob,
        length,
        part,
        material.previous_part.map(|id| id.0),
        material.parser,
        version,
        serde_json::to_value(&material.manifest).expect("manifests serialize"),
        binding,
        input.kind,
        input.locator,
        input.host,
    )
    .fetch_optional(conn)
    .await?)
}

async fn material(
    conn: &mut PgConnection,
    input: Uuid,
    revision: Uuid,
    m: &sinex_model::MaterialRef,
) -> Result<()> {
    let length =
        i64::try_from(m.length).map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    let excised = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM core.excised_materials
             WHERE material_id = $1 OR (blob = $2 AND $3::bigint > 0)) AS "excised!""#,
        m.id.0,
        m.blob,
        length,
    )
    .fetch_one(&mut *conn)
    .await?;
    if excised {
        return Err(Error::Invalid(format!(
            "material {} holds bytes an excision removed; they are not stored again",
            m.id
        )));
    }
    sqlx::query!(
        r#"INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present')
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status = 'deleting'"#,
        m.blob,
        length,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO raw.materials (id, input_id, revision_id, part_index, previous_part_id, blob, length, manifest, parser, parser_version)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING"#,
        m.id.0,
        input,
        revision,
        i32::try_from(m.part_index).map_err(|_| Error::Invalid("part index beyond i32".into()))?,
        m.previous_part.map(|p| p.0),
        m.blob,
        length,
        serde_json::to_value(&m.manifest).expect("manifests serialize"),
        m.parser,
        i32::try_from(m.parser_version).map_err(|_| Error::Invalid("parser version beyond i32".into()))?,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The producer's persisted acknowledgment of one ingress frame, written with
/// its events.
pub(super) async fn frame(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
    outcome: &Outcome,
) -> Result<()> {
    let admitted: Vec<Uuid> = outcome.admitted_ids.iter().map(|id| id.0).collect();
    let counts = serde_json::json!({
        "registered_kinds": outcome.registered_kinds,
        "admitted": outcome.admitted,
        "repeats": outcome.repeats,
        "replaced": outcome.replaced,
        "older": outcome.older,
        "hidden": outcome.hidden,
        "superseded": outcome.superseded,
        "failures": outcome.failures,
        "refusal_kind": frame.refusal_kind,
        "source": frame.source,
        "event_types": frame.event_types,
    });
    sqlx::query!(
        r#"INSERT INTO core.ingress_frames (producer, frame, material_id, admitted, outcome, refusal, commit_seq)
           VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        frame.producer,
        frame.frame,
        frame
            .material
            .as_ref()
            .and_then(|m| m.material.as_ref())
            .map(|m| m.id.0),
        &admitted,
        counts,
        frame.refusal,
        outcome.commit_seq.map(|(_, last)| last),
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}
