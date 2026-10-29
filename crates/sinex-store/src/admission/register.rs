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
    number: i64,
) -> Result<()> {
    let admitted: Vec<Uuid> = outcome.admitted_ids.iter().map(|id| id.0).collect();
    let counts = outcome.frame_counts(frame);
    sqlx::query!(
        r#"INSERT INTO core.ingress_frames (producer, frame, material_id, admitted, outcome, refusal, commit_seq, family, write_kind_prefixes, number, source_name, source_version, source_fingerprint, producer_host)
           OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"#,
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
        frame.family,
        &frame.write_kind_prefixes,
        number,
        frame.native_source.as_ref().map(|source|source.name.as_str()),
        frame.native_source.as_ref().map(|source|i64::from(source.version)),
        frame.source_fingerprint,
        frame.host,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Exact frame retries return the committed result before advancing any order.
pub(crate) async fn recorded(
    conn: &mut PgConnection,
    frame: &sinex_model::IngressFrame,
) -> Result<Option<Outcome>> {
    let row = sqlx::query!(
        "SELECT admitted,outcome,refusal FROM core.ingress_frames WHERE producer=$1 AND frame=$2",
        frame.producer,
        frame.frame
    )
    .fetch_optional(conn)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if let (Some(offered), Some(saved)) = (
        &frame.content_hash,
        row.outcome
            .get("frame_content_hash")
            .and_then(Value::as_str),
    ) && offered != saved
    {
        return Err(Error::Invalid("frame_body_conflict".into()));
    }
    if let Some(refusal) = row.refusal {
        return Err(Error::Invalid(format!("recorded_frame_refused: {refusal}")));
    }
    let count = |name: &str| -> Result<usize> {
        match row.outcome.get(name) {
            None => Ok(0),
            Some(value) => value
                .as_u64()
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(|| Error::Invalid(format!("stored frame has invalid {name} count"))),
        }
    };
    Ok(Some(Outcome {
        admitted: count("admitted")?,
        repeats: count("repeats")?,
        replaced: count("replaced")?,
        older: count("older")?,
        hidden: count("hidden")?,
        superseded: count("superseded")?,
        superseded_input: count("superseded_input")?,
        failures: count("failures")?,
        dirty_keys: count("dirty_keys")?,
        replay_archived: count("replay_archived")?,
        commit_seq: row
            .outcome
            .get("commit_range")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| Error::Invalid(format!("stored frame has invalid commit range: {e}")))?,
        admitted_ids: row.admitted.into_iter().map(EventId).collect(),
        registered_kinds: row
            .outcome
            .get("registered_kinds")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| {
                Error::Invalid(format!("stored frame has invalid kind registrations: {e}"))
            })?
            .unwrap_or_default(),
        timings: Default::default(),
    }))
}

#[derive(Default)]
pub(super) struct Provenance {
    pub family: Option<String>,
    pub identity: Option<String>,
    pub host: Option<String>,
    pub frame: Option<i64>,
    pub source_version: Option<i64>,
    pub source_fingerprint: Option<String>,
}

pub(super) async fn provenance(
    conn: &mut PgConnection,
    batch: &Batch,
    number: Option<i64>,
) -> Result<Provenance> {
    let (source, mut provenance) = match &batch.kind {
        BatchKind::Ingress(frame) => (
            frame.native_source.as_ref(),
            Provenance {
                family: frame.family.clone(),
                identity: Some(frame.producer.clone()),
                host: frame.host.clone(),
                frame: number,
                source_version: None,
                source_fingerprint: frame.source_fingerprint.clone(),
            },
        ),
        BatchKind::Source(progress) => (
            progress.source.as_ref(),
            Provenance {
                host: progress.host.clone(),
                ..Default::default()
            },
        ),
        BatchKind::Replay { source, host, .. } => (
            source.as_ref(),
            Provenance {
                host: host.clone(),
                ..Default::default()
            },
        ),
        BatchKind::Derived { .. } | BatchKind::Api { .. } => {
            return Ok(Provenance::default());
        }
    };
    let Some(source) = source else {
        return Ok(provenance);
    };
    let declaration = crate::source_declarations::load(conn, source)
        .await?
        .ok_or_else(|| {
            Error::Invalid(format!(
                "native source {} version {} is not registered",
                source.name, source.version
            ))
        })?;
    if provenance
        .family
        .as_ref()
        .is_some_and(|family| family != &declaration.family)
    {
        return Err(Error::Invalid(
            "native source belongs to another configured family".into(),
        ));
    }
    let fingerprint = declaration.fingerprint();
    if provenance
        .source_fingerprint
        .as_ref()
        .is_some_and(|known| known != &fingerprint)
    {
        return Err(Error::Invalid(
            "native source fingerprint differs from the owner catalog".into(),
        ));
    }
    let refs = batch.events().map(Event::kind_ref).collect::<Vec<_>>();
    let kinds = sinex_schema::kinds::load(conn, &refs).await?;
    for event in batch.events() {
        if event.source != source.name
            || kinds
                .get(&event.kind_ref())
                .is_none_or(|kind| !declaration.allows(&kind.kind, &kind.definition_hash()))
        {
            return Err(Error::Invalid(
                "event is outside its exact native source declaration".into(),
            ));
        }
    }
    provenance.family = Some(declaration.family);
    if matches!(batch.kind, BatchKind::Source(_) | BatchKind::Replay { .. }) {
        provenance.identity = Some("sinexd".into());
    }
    provenance.source_version = Some(i64::from(source.version));
    provenance.source_fingerprint = Some(fingerprint);
    Ok(provenance)
}

impl Outcome {
    pub(crate) fn frame_counts(&self, frame: &sinex_model::IngressFrame) -> Value {
        serde_json::json!({
            "frame_content_hash": frame.content_hash,
            "registered_kinds": self.registered_kinds,
            "commit_range": self.commit_seq,
            "superseded_input": self.superseded_input,
            "replay_archived": self.replay_archived,
            "dirty_keys": self.dirty_keys,
            "admitted": self.admitted,
            "repeats": self.repeats,
            "replaced": self.replaced,
            "older": self.older,
            "hidden": self.hidden,
            "superseded": self.superseded,
            "failures": self.failures,
            "refusal_kind": frame.refusal_kind,
            "source": frame.source,
            "event_types": frame.event_types,
        })
    }
}

/// A native field key names one immutable input copy. Neither a declared host
/// nor a caller-selected input UUID can join another copy's reading history.
pub(super) async fn native_keys(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &mut [Event],
) -> Result<()> {
    let native = match &batch.kind {
        BatchKind::Source(progress) => progress.source.as_ref(),
        BatchKind::Ingress(frame) => frame.native_source.as_ref(),
        BatchKind::Replay { source, .. } => source.as_ref(),
        _ => None,
    };
    if native.is_none() {
        return Ok(());
    }
    let ids = events
        .iter()
        .filter_map(|event| match &event.origin {
            Origin::Record { location, .. } if event.equivalence_key.is_some() => {
                Some(location.material.0)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let rows: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT id,input_id FROM raw.materials WHERE id=ANY($1)")
            .bind(&ids)
            .fetch_all(conn)
            .await?;
    let inputs = rows
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
    for event in events {
        if let Origin::Record { location, .. } = &event.origin
            && let Some(key) = &mut event.equivalence_key
        {
            let input = inputs.get(&location.material.0).ok_or_else(|| {
                Error::Invalid("native reading has no owner-registered immutable input".into())
            })?;
            let encoded = serde_json::to_vec(&(input, key.0.as_str()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            key.0 = blake3::derive_key("sinex.native_input_key.v1", &encoded)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
        }
    }
    Ok(())
}
