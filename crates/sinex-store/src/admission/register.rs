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
    for material in p
        .material
        .iter()
        .chain(p.image.iter().map(|image| &image.material))
    {
        if material_matches(conn, &p.binding, &p.input, material).await? == Some(false) {
            return Err(Error::Invalid(
                "material identity already belongs to different retained facts".into(),
            ));
        }
    }

    if let Some(image) = &p.image {
        crate::sqlite_images::guard_acquisition(conn, &p.input, &p.observation, &image.metadata)
            .await?;
    }
    if let Some(replay) = &p.erased_replay
        && (p.material.as_ref().is_none_or(|m| m.id != replay.material)
            || p.excision_basis.as_ref().is_none_or(|b| b.count == 0)
            || !p
                .material_keys
                .iter()
                .any(|k| k.material == replay.material)
            || replay.length == 0
            || i64::try_from(replay.length).is_err())
    {
        return Err(Error::Invalid(
            "erased replay publication lacks its material, key or excision basis".into(),
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
    for key in &p.material_keys {
        crate::sealed::store_key(conn, key.material, &key.key_ref, &key.wrapped).await?;
    }
    let revision = if let Some(image) = &p.image {
        let m = &image.metadata;
        image.validate().map_err(Error::Invalid)?;
        if p.image_revision != Some(m.revision)
            || m.host != p.input.host
            || image.material.id != m.material
            || image.material.length != m.evidence_bytes
        {
            return Err(Error::Invalid(
                "SQLite image publication does not match source progress".into(),
            ));
        }
        let existing = sqlx::query_scalar!(
            "SELECT input_id FROM raw.revisions WHERE id=$1",
            m.revision.0
        )
        .fetch_optional(&mut *conn)
        .await?;
        if existing.is_some_and(|id| id != input) {
            return Err(Error::Invalid(
                "SQLite image revision belongs to another input".into(),
            ));
        }
        if let Some(previous) = m.previous {
            let id = sqlx::query_scalar!(
                "SELECT material_id FROM raw.sqlite_images WHERE revision_id=$1",
                previous.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid("SQLite image predecessor missing".into()))?;
            let (metadata, extents, _) =
                crate::sqlite_images::load(conn, sinex_model::MaterialId(id))
                    .await?
                    .ok_or_else(|| Error::Invalid("SQLite image predecessor missing".into()))?;
            image
                .validate_predecessor(&metadata, &extents)
                .map_err(Error::Invalid)?;
        }
        sqlx::query!("INSERT INTO raw.revisions (id,input_id,previous_id,kind) VALUES ($1,$2,$3,'snapshot') ON CONFLICT (id) DO NOTHING",m.revision.0,input,current).execute(&mut *conn).await?;
        material(conn, input, m.revision.0, &image.material).await?;
        if let Some(key) = &image.key {
            crate::sealed::store_key(conn, key.material, &key.key_ref, &key.wrapped).await?;
        }
        let metadata = serde_json::to_value(m).map_err(|e| Error::Invalid(e.to_string()))?;
        sqlx::query!("INSERT INTO raw.sqlite_images (revision_id,material_id,database,host,metadata) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (revision_id) DO NOTHING",m.revision.0,m.material.0,m.database,m.host,metadata).execute(&mut *conn).await?;
        let actual = sqlx::query_scalar!(
            "SELECT metadata FROM raw.sqlite_images WHERE revision_id=$1",
            m.revision.0
        )
        .fetch_one(&mut *conn)
        .await?;
        if actual != metadata {
            return Err(Error::Invalid(
                "SQLite image revision metadata changed".into(),
            ));
        }
        for (position, e) in image.extents.iter().enumerate() {
            sqlx::query!("INSERT INTO raw.revision_extents (revision_id,position,blob,blob_offset,length) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (revision_id,position) DO NOTHING",m.revision.0,i32::try_from(position).map_err(|_|Error::Invalid("too many SQLite extents".into()))?,e.blob,i64::try_from(e.offset).map_err(|_|Error::Invalid("SQLite extent offset beyond i64".into()))?,i64::try_from(e.length).map_err(|_|Error::Invalid("SQLite extent length beyond i64".into()))?).execute(&mut *conn).await?;
        }
        m.revision.0
    } else if let Some(revision) = p.image_revision {
        let owner =
            sqlx::query_scalar!("SELECT input_id FROM raw.revisions WHERE id=$1", revision.0)
                .fetch_one(&mut *conn)
                .await?;
        if owner != input {
            return Err(Error::Invalid(
                "SQLite image continuation belongs to another input".into(),
            ));
        }
        revision.0
    } else {
        match (p.revision, current) {
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
        }
    };

    let Some(m) = &p.material else { return Ok(()) };
    material(conn, input, revision, m).await?;
    if let Some(replay) = &p.erased_replay {
        crate::sealed::metadata::register_blob(conn, &replay.blob, replay.length).await?;
        sqlx::query("UPDATE raw.materials SET erased_replay_blob=$2 WHERE id=$1 AND erased_replay_blob IS NULL AND blob=$3 AND durable_bytes=$4 AND manifest=$5 AND parser=$6 AND parser_version=$7")
            .bind(m.id.0).bind(&replay.blob).bind(&m.blob).bind(m.length as i64).bind(&m.manifest).bind(&m.parser).bind(m.parser_version as i32).execute(&mut *conn).await?;
        let actual: Option<String> =
            sqlx::query_scalar("SELECT erased_replay_blob FROM raw.materials WHERE id=$1")
                .bind(m.id.0)
                .fetch_one(&mut *conn)
                .await?;
        if actual.as_deref() != Some(replay.blob.as_str()) {
            return Err(Error::Invalid(
                "material replay metadata changed before admission".into(),
            ));
        }
    }

    let role = sinex_model::MaterialRole::of(&m.manifest)
        .map_err(|e| Error::Invalid(format!("material {}: {e}", m.id)))?;
    // Decoded record parts retain parser coordinates, not additional input bytes.
    if matches!(
        role,
        sinex_model::MaterialRole::Records | sinex_model::MaterialRole::SqliteImage
    ) {
        return Ok(());
    }
    let length =
        i64::try_from(m.length).map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    sqlx::query!(
        r#"INSERT INTO raw.revision_extents (revision_id, position, blob, blob_offset, length)
           VALUES ($1, (SELECT coalesce(max(position) + 1, 0) FROM raw.revision_extents WHERE revision_id = $1), $2, 0, $3)"#,
        revision,
        m.blob,
        length,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Immutable material identity excludes the run/observation/revision attempt.
/// Check the existing input before its current kind can be upserted, and reuse
/// this comparison when a pending publication releases its last byte owner.
pub(crate) async fn material_matches(
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
    Ok(sqlx::query_scalar("SELECT m.blob=$2 AND m.durable_bytes=$3 AND m.part_index=$4 AND m.previous_part_id IS NOT DISTINCT FROM $5 AND m.parser=$6 AND m.parser_version=$7 AND m.manifest=$8 AND i.binding=$9 AND i.kind=$10 AND i.locator=$11 AND i.host=$12 FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1")
        .bind(material.id.0).bind(&material.blob).bind(length).bind(part).bind(material.previous_part.map(|id|id.0))
        .bind(&material.parser).bind(version).bind(&material.manifest).bind(binding).bind(&input.kind).bind(&input.locator).bind(&input.host)
        .fetch_optional(conn).await?)
}

async fn material(
    conn: &mut PgConnection,
    input: Uuid,
    revision: Uuid,
    m: &sinex_model::MaterialRef,
) -> Result<()> {
    let length =
        i64::try_from(m.length).map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    sqlx::query!(
        r#"INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present')
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status IN ('reserved','deleting')"#,
        m.blob,
        length,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO raw.materials (id, input_id, revision_id, part_index, previous_part_id, blob, durable_bytes, manifest, parser, parser_version)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING"#,
        m.id.0,
        input,
        revision,
        i32::try_from(m.part_index).map_err(|_| Error::Invalid("part index beyond i32".into()))?,
        m.previous_part.map(|p| p.0),
        m.blob,
        length,
        m.manifest,
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
        "admitted": outcome.admitted,
        "repeats": outcome.repeats,
        "replaced": outcome.replaced,
        "older": outcome.older,
        "hidden": outcome.hidden,
        "superseded": outcome.superseded,
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
