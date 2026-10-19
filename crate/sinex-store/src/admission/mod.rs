//! Admission: the one way events are stored (architecture §4.2). One call per
//! batch, inside the caller's transaction:
//!
//! 1. register the source's input, observation, revision and material;
//! 2. apply privacy rules to each payload;
//! 3. stage the events with one `COPY`;
//! 4. classify every staged event in one set-based query against the live side
//!    tables and the remembered removals: new, repeat, newer, older, hidden, or
//!    superseded by a newer reading in the same batch;
//! 5. allocate one `commit_seq` range for the admitted events;
//! 6. insert events, commit log and side-table rows, archive replaced readings;
//! 7. record failures and the source position;
//! 8. mark dirty keys for keyed derivations whose inputs changed;
//! 9. apply hide and unhide requests.
//!
//! A replay's request to archive the readings it re-reads runs first, before
//! staging, so the new readings are classified against what remains.
//!
//! The caller commits; events and progress are then durable together.
use crate::{Error, Result, privacy, stage};
use serde_json::Value;
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, EventId, InputId, ObservationId, OperationId,
    Origin, ReadingOrder, RecordOutcome, ReplayScope, RevisionChange, RevisionId, SourceProgress,
    Timestamp,
};
use sqlx::PgConnection;
use uuid::Uuid;

/// What a keyed derivation's key function sees of an admitted or archived event.
#[derive(Clone, Debug)]
pub struct KeyInput<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<time::OffsetDateTime>,
    pub payload: &'a Value,
}

/// The key functions of the keyed derivations, supplied by the daemon, which
/// owns the derivation registry. Admission marks every returned key dirty.
pub trait DirtyKeys: Send + Sync {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(&'static str, Value)>;
}

/// No keyed derivations registered.
pub struct NoDerivations;

impl DirtyKeys for NoDerivations {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        Vec::new()
    }
}

/// Counts of what admission did with one batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    pub admitted: usize,
    /// Same content as the live reading: suppressed.
    pub repeats: usize,
    /// Newer readings that replaced (and archived) a live one.
    pub replaced: usize,
    /// Older readings of a record that has a newer live reading: suppressed.
    pub older: usize,
    /// Readings of hidden or excised records, or of bytes an excision
    /// removed: suppressed.
    pub hidden: usize,
    /// Superseded by a newer reading of the same record in this batch.
    pub superseded: usize,
    pub failures: usize,
    pub dirty_keys: usize,
    /// Live readings a replay archived before admitting its new readings.
    pub replay_archived: usize,
    /// The commit sequence range given to admitted events.
    pub commit_seq: Option<(i64, i64)>,
    pub admitted_ids: Vec<EventId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    New,
    Repeat,
    Newer,
    Older,
    Hidden,
    Superseded,
}

/// Admit `batch` inside the caller's open transaction.
pub async fn admit(
    conn: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    // Live readings a repeat or older reading left in place: for a derived
    // batch they are still the key's outputs.
    let mut kept: Vec<Uuid> = Vec::new();

    match &batch.kind {
        BatchKind::Source(progress) => register_source(conn, progress).await?,
        BatchKind::Ingress(frame) => {
            if let Some(progress) = &frame.material {
                register_source(conn, progress).await?;
            }
        }
        BatchKind::Derived { .. } | BatchKind::Api { .. } | BatchKind::Replay { .. } => {}
    }
    for request in &batch.archive {
        if let ArchiveRequest::Replay { scope, operation } = request {
            let (archived, dirty) = archive_replay_scope(conn, scope, *operation, keys).await?;
            outcome.replay_archived += archived;
            outcome.dirty_keys += dirty;
        }
    }

    // Privacy rules apply once, here; reads never apply a rule again.
    let mut events: Vec<Event> = batch.events().cloned().collect();
    if !events.is_empty() {
        // A derived event is never below the highest class of its parents
        // (spec `privacy.md` §4), so derivation cannot launder a class away.
        let parents: Vec<Uuid> = events
            .iter()
            .filter_map(|e| match &e.origin {
                Origin::Derived { parents } => Some(parents.iter().map(|p| p.0)),
                Origin::Record { .. } => None,
            })
            .flatten()
            .collect();
        if !parents.is_empty() {
            let classes: std::collections::HashMap<Uuid, Class> = sqlx::query!(
                "SELECT id, class FROM core.events WHERE id = ANY($1)",
                &parents
            )
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(|r| Ok((r.id, privacy::parse_class(&r.class)?)))
            .collect::<Result<_>>()?;
            for event in &mut events {
                if let Origin::Derived { parents } = &event.origin {
                    for p in parents {
                        if let Some(&c) = classes.get(&p.0) {
                            event.class = event.class.max(c);
                        }
                    }
                }
            }
        }
        let rules = privacy::load(conn, events.iter().map(|e| e.source.as_str())).await?;
        for event in &mut events {
            rules.apply(event)?;
        }
        sqlx::query!(
            "UPDATE core.runs SET privacy_rules_seq = $2 WHERE id = $1 AND privacy_rules_seq IS DISTINCT FROM $2",
            batch.run.0,
            rules.seq
        )
        .execute(&mut *conn)
        .await?;
    }
    // Records the source dropped: a newer reading of each live one, removed.
    // Built from the stored reading, whose privacy rules already applied.
    for request in &batch.archive {
        if let ArchiveRequest::RemovedAtSource { locations } = request {
            for live in crate::derived::live_at(conn, locations).await? {
                if live.record_state == sinex_model::RecordState::Removed {
                    continue;
                }
                let mut hash = blake3::Hasher::new();
                hash.update(&live.record_hash);
                hash.update(b"removed at source");
                events.push(Event {
                    id: sinex_model::EventId::new(),
                    reading_order: ReadingOrder(live.reading_order.0.saturating_add(1)),
                    record_hash: *hash.finalize().as_bytes(),
                    record_state: sinex_model::RecordState::Removed,
                    ..live
                });
            }
        }
    }

    if !events.is_empty() {
        sqlx::query!("TRUNCATE core.admission_stage")
            .execute(&mut *conn)
            .await?;
        let numbered: Vec<(i32, &Event)> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (i32::try_from(i).expect("batch fits i32"), e))
            .collect();
        stage::copy(conn, &numbered).await?;

        let rows = sqlx::query!(
            r#"WITH s AS (
                 SELECT st.*,
                   row_number() OVER (
                     PARTITION BY st.source, st.event_type,
                       coalesce(st.equivalence_key, st.material_id::text || ':' || st.anchor || ':' || st.ordinal)
                     ORDER BY st.reading_order DESC, st.ord DESC) AS rank
                 FROM core.admission_stage st)
               SELECT s.ord AS "ord!",
                 CASE
                   WHEN s.rank > 1 THEN 'superseded'
                   WHEN hk.id IS NOT NULL OR hl.id IS NOT NULL OR xd.record_hash IS NOT NULL THEN 'hidden'
                   WHEN coalesce(le.event_id, lc.event_id) IS NULL THEN 'new'
                   WHEN coalesce(le.record_hash, lc.record_hash) = s.record_hash THEN 'repeat'
                   -- The source dropping a record is a fact, not another
                   -- reading of the same content: it replaces even where
                   -- repeats are suppressed.
                   WHEN s.record_state = 'removed'
                     AND s.reading_order > coalesce(le.reading_order, lc.reading_order) THEN 'newer'
                   WHEN s.replace_rule = 'suppress_repeat' THEN 'repeat'
                   WHEN s.reading_order > coalesce(le.reading_order, lc.reading_order) THEN 'newer'
                   ELSE 'older'
                 END AS "state!",
                 coalesce(le.event_id, lc.event_id) AS live_id
               FROM s
               LEFT JOIN core.hidden_occurrences hk
                 ON s.equivalence_key IS NOT NULL AND hk.source = s.source
                 AND hk.event_type = s.event_type AND hk.equivalence_key = s.equivalence_key
               LEFT JOIN core.hidden_occurrences hl
                 ON s.equivalence_key IS NULL AND hl.material_id = s.material_id
                 AND hl.anchor = s.anchor AND hl.event_type = s.event_type AND hl.ordinal = s.ordinal
               LEFT JOIN core.excised_digests xd
                 ON s.material_id IS NOT NULL AND xd.source = s.source
                 AND xd.event_type = s.event_type AND xd.record_hash = s.record_hash
               LEFT JOIN core.live_equivalence le
                 ON s.equivalence_key IS NOT NULL AND le.source = s.source
                 AND le.event_type = s.event_type AND le.equivalence_key = s.equivalence_key
               LEFT JOIN core.live_coordinates lc
                 ON s.equivalence_key IS NULL AND s.material_id IS NOT NULL
                 AND lc.material_id = s.material_id AND lc.anchor = s.anchor
                 AND lc.event_type = s.event_type AND lc.ordinal = s.ordinal
               ORDER BY s.ord"#
        )
        .fetch_all(&mut *conn)
        .await?;

        let mut admit_ords: Vec<i32> = Vec::new();
        let mut replaced: Vec<(i32, Uuid)> = Vec::new();
        for row in rows {
            let state = match row.state.as_str() {
                "new" => State::New,
                "repeat" => State::Repeat,
                "newer" => State::Newer,
                "older" => State::Older,
                "hidden" => State::Hidden,
                "superseded" => State::Superseded,
                other => return Err(Error::Invalid(format!("unknown admission state {other}"))),
            };
            match state {
                State::New => admit_ords.push(row.ord),
                State::Newer => {
                    admit_ords.push(row.ord);
                    replaced.push((row.ord, row.live_id.expect("newer has a live reading")));
                }
                State::Repeat => {
                    outcome.repeats += 1;
                    kept.extend(row.live_id);
                }
                State::Older => {
                    outcome.older += 1;
                    kept.extend(row.live_id);
                }
                State::Hidden => outcome.hidden += 1,
                State::Superseded => outcome.superseded += 1,
            }
        }

        // A newer reading keeps the replaced reading's value for the fields
        // its type declares `keep_known` when it leaves them unknown.
        for (ord, live) in &replaced {
            let event_type = &events[usize::try_from(*ord).expect("ord is an index")].event_type;
            let keep = sinex_model::kinds::find(event_type).map_or(&[][..], |k| k.keep_known);
            if keep.is_empty() {
                continue;
            }
            let keep: Vec<String> = keep.iter().map(|f| (*f).to_owned()).collect();
            sqlx::query!(
                r#"UPDATE core.admission_stage st
                   SET payload = st.payload || coalesce((
                     SELECT jsonb_object_agg(f, e.payload->f)
                     FROM unnest($3::text[]) f
                     WHERE coalesce(st.payload->f, 'null'::jsonb) = 'null'::jsonb
                       AND coalesce(e.payload->f, 'null'::jsonb) <> 'null'::jsonb), '{}'::jsonb)
                   FROM core.events e
                   WHERE st.ord = $1 AND e.id = $2"#,
                ord,
                live,
                &keep,
            )
            .execute(&mut *conn)
            .await?;
        }

        if !admit_ords.is_empty() {
            let n = i64::try_from(admit_ords.len()).expect("count fits i64");
            let last = sqlx::query_scalar!(
                "UPDATE core.counters SET value = value + $1 WHERE name = 'commit_seq' RETURNING value",
                n
            )
            .fetch_one(&mut *conn)
            .await?;
            let first = last - n + 1;
            outcome.commit_seq = Some((first, last));

            sqlx::query!(
                r#"WITH a AS (
                     SELECT s.*, $2::bigint + row_number() OVER (ORDER BY s.ord) - 1 AS seq
                     FROM core.admission_stage s WHERE s.ord = ANY($1))
                   INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, ordinal,
                     parents, ts_orig, ts_quality, commit_seq, payload, equivalence_key, reading_order,
                     record_hash, record_state, class, support)
                   SELECT id, event_type, source, $3, material_id, anchor, ordinal, parents, ts_orig,
                     ts_quality, seq, payload, equivalence_key, reading_order, record_hash, record_state,
                     class, support
                   FROM a"#,
                &admit_ords,
                first,
                batch.run.0,
            )
            .execute(&mut *conn)
            .await?;

            sqlx::query!(
                r#"INSERT INTO core.commit_log (commit_seq, event_id, event_type)
                   SELECT $2::bigint + row_number() OVER (ORDER BY s.ord) - 1, s.id, s.event_type
                   FROM core.admission_stage s WHERE s.ord = ANY($1)"#,
                &admit_ords,
                first,
            )
            .execute(&mut *conn)
            .await?;

            sqlx::query!(
                r#"INSERT INTO core.live_equivalence (source, event_type, equivalence_key, event_id, reading_order, record_hash)
                   SELECT s.source, s.event_type, s.equivalence_key, s.id, s.reading_order, s.record_hash
                   FROM core.admission_stage s
                   WHERE s.ord = ANY($1) AND s.equivalence_key IS NOT NULL
                   ON CONFLICT (source, event_type, equivalence_key) DO UPDATE
                   SET event_id = EXCLUDED.event_id, reading_order = EXCLUDED.reading_order,
                       record_hash = EXCLUDED.record_hash"#,
                &admit_ords,
            )
            .execute(&mut *conn)
            .await?;

            sqlx::query!(
                r#"INSERT INTO core.live_coordinates (material_id, anchor, event_type, ordinal, event_id, reading_order, record_hash)
                   SELECT s.material_id, s.anchor, s.event_type, s.ordinal, s.id, s.reading_order, s.record_hash
                   FROM core.admission_stage s
                   WHERE s.ord = ANY($1) AND s.equivalence_key IS NULL AND s.material_id IS NOT NULL
                   ON CONFLICT (material_id, anchor, event_type, ordinal) DO UPDATE
                   SET event_id = EXCLUDED.event_id, reading_order = EXCLUDED.reading_order,
                       record_hash = EXCLUDED.record_hash"#,
                &admit_ords,
            )
            .execute(&mut *conn)
            .await?;

            if !replaced.is_empty() {
                let (ords, live): (Vec<i32>, Vec<Uuid>) = replaced.iter().copied().unzip();
                sqlx::query!(
                    r#"INSERT INTO audit.archived (event_id, archived_seq, reason, replaced_by)
                       SELECT r.live, $3, 'newer_reading', s.id
                       FROM unnest($1::int[], $2::uuid[]) AS r(ord, live)
                       JOIN core.admission_stage s ON s.ord = r.ord
                       ON CONFLICT (event_id) DO NOTHING"#,
                    &ords,
                    &live,
                    last,
                )
                .execute(&mut *conn)
                .await?;
                outcome.replaced = replaced.len();
                outcome.dirty_keys += mark_archived_dirty(conn, &live, last, keys).await?;
            }

            outcome.admitted = admit_ords.len();
            let mut dirty = Vec::new();
            for ord in &admit_ords {
                let event = &events[usize::try_from(*ord).expect("ord is non-negative")];
                outcome.admitted_ids.push(event.id);
                let input = KeyInput {
                    event_type: event.event_type,
                    ts_orig: event.ts_orig.at.map(|t| t.0),
                    payload: &event.payload,
                };
                dirty.extend(keys.keys(&input));
            }
            outcome.dirty_keys += mark_dirty(conn, dirty, last).await?;
            let admitted: Vec<Uuid> = outcome.admitted_ids.iter().map(|id| id.0).collect();
            crate::analysis::mark_stale(conn, &admitted, last).await?;
        }
    }

    if let BatchKind::Derived { derivation, scope } = &batch.kind {
        outcome.dirty_keys +=
            settle_derived(conn, derivation, scope, &outcome.admitted_ids, &kept, keys).await?;
    }

    let failures: Vec<(Uuid, i64, &str)> = batch
        .records
        .iter()
        .filter_map(|r| match r {
            RecordOutcome::Failure { location, reason } => Some((
                location.material.0,
                i64::try_from(location.anchor).unwrap_or(i64::MAX),
                reason.as_str(),
            )),
            RecordOutcome::Events { .. } => None,
        })
        .collect();
    if !failures.is_empty() {
        let materials: Vec<Uuid> = failures.iter().map(|f| f.0).collect();
        let anchors: Vec<i64> = failures.iter().map(|f| f.1).collect();
        let reasons: Vec<String> = failures.iter().map(|f| f.2.to_owned()).collect();
        sqlx::query!(
            r#"INSERT INTO raw.failures (material_id, anchor, reason, run_id)
               SELECT * , $4::uuid FROM unnest($1::uuid[], $2::bigint[], $3::text[])"#,
            &materials,
            &anchors,
            &reasons,
            batch.run.0,
        )
        .execute(&mut *conn)
        .await?;
        outcome.failures = failures.len();
    }

    if let BatchKind::Source(progress) = &batch.kind {
        let committed =
            sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
                .fetch_one(&mut *conn)
                .await?;
        sqlx::query!(
            r#"INSERT INTO raw.positions (binding, cursor, carry, committed_seq) VALUES ($1, $2, $3, $4)
               ON CONFLICT (binding) DO UPDATE SET cursor = EXCLUDED.cursor, carry = EXCLUDED.carry,
                 committed_seq = EXCLUDED.committed_seq, updated_at = now()"#,
            progress.binding,
            progress.cursor,
            progress.carry,
            committed,
        )
        .execute(&mut *conn)
        .await?;
    }

    for request in &batch.archive {
        outcome.dirty_keys += archive_request(conn, request, keys).await?;
    }

    match &batch.kind {
        BatchKind::Ingress(frame) => record_frame(conn, frame, &outcome).await?,
        _ => {}
    }
    Ok(outcome)
}

/// Archive the live readings a replay re-reads: those read from the scope's
/// materials (narrowed by type and time), and the live derived events that
/// descend from them. Their keys become dirty, so derivations recompute. The
/// materials' failure rows go too when whole materials are re-read: the
/// replay records its own.
async fn archive_replay_scope(
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
              OR EXISTS (SELECT 1 FROM core.live_coordinates l WHERE l.event_id = x.id)"#,
        &materials,
        types.as_deref(),
        scope.from.map(|t| t.0),
        scope.until.map(|t| t.0),
    )
    .fetch_all(&mut *conn)
    .await?;
    let seq = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
        .fetch_one(&mut *conn)
        .await?;
    let dirty = mark_archived_dirty(conn, &live, seq, keys).await?;
    sqlx::query!(
        r#"INSERT INTO audit.archived (event_id, archived_seq, reason, operation_id)
           SELECT id, $2, 'replay', $3 FROM unnest($1::uuid[]) AS id ON CONFLICT (event_id) DO NOTHING"#,
        &live,
        seq,
        operation.0,
    )
    .execute(&mut *conn)
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

/// The producer's persisted acknowledgment of one ingress frame, written with
/// its events (architecture §4.3, `mn8u`).
async fn record_frame(
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

/// Upsert the input, record the observation, open a revision when the content
/// changed in a way that is not a plain append, and register the material.
/// Bytes are already in the content store, so they exist before any row refers
/// to them.
async fn register_source(conn: &mut PgConnection, p: &SourceProgress) -> Result<()> {
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
    let length =
        i64::try_from(m.length).map_err(|_| Error::Invalid("material length beyond i64".into()))?;
    sqlx::query!(
        r#"INSERT INTO core.blobs (hash, size, status) VALUES ($1, $2, 'present')
           ON CONFLICT (hash) DO UPDATE SET status = 'present' WHERE core.blobs.status = 'deleting'"#,
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

/// What a derived batch covers (`BatchKind::Derived::scope`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedScope {
    /// A keyed derivation's complete outputs for one key, computed after its
    /// dirty mark reached `dirty_seq`. Outputs the key produced before and no
    /// longer produces are archived; the dirty mark clears unless the key was
    /// dirtied again since.
    Key {
        key: Value,
        dirty_seq: i64,
        /// Some output still waits for evidence outside the key's window.
        waiting: bool,
    },
    /// A per-event derivation's outputs for committed events up to `position`.
    Position { position: i64 },
    /// Recompute these keys (a version change, `sinexctl recompute`). Carries
    /// no events; the keys are marked dirty like admission marks them.
    Recompute { keys: Vec<Value> },
}

impl DerivedScope {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("scopes serialize")
    }
}

/// Settle a derived batch: replace the key's output set, clear its dirty
/// mark, record whether it waits; or advance a per-event position.
async fn settle_derived(
    conn: &mut PgConnection,
    derivation: &str,
    scope: &Value,
    admitted: &[EventId],
    kept: &[Uuid],
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let scope: DerivedScope = serde_json::from_value(scope.clone())
        .map_err(|e| Error::Invalid(format!("derived batch scope: {e}")))?;
    match scope {
        DerivedScope::Recompute { keys: marked } => {
            let seq =
                sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
                    .fetch_one(&mut *conn)
                    .await?;
            let name: &'static str = crate::derived::static_type(derivation);
            mark_dirty(conn, marked.into_iter().map(|k| (name, k)).collect(), seq).await
        }
        DerivedScope::Position { position } => {
            sqlx::query!(
                r#"INSERT INTO derivation.positions (derivation, commit_seq) VALUES ($1, $2)
                   ON CONFLICT (derivation) DO UPDATE SET commit_seq = greatest(derivation.positions.commit_seq, EXCLUDED.commit_seq)"#,
                derivation,
                position,
            )
            .execute(&mut *conn)
            .await?;
            Ok(0)
        }
        DerivedScope::Key {
            key,
            dirty_seq,
            waiting,
        } => {
            let live: Vec<Uuid> = admitted
                .iter()
                .map(|e| e.0)
                .chain(kept.iter().copied())
                .collect();
            // Previous outputs not produced again and still live go to the archive.
            let gone = sqlx::query_scalar!(
                r#"SELECT o.event_id AS "event_id!" FROM derivation.outputs o
                   WHERE o.derivation = $1 AND o.key = $2 AND NOT (o.event_id = ANY($3))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = o.event_id)"#,
                derivation,
                key,
                &live,
            )
            .fetch_all(&mut *conn)
            .await?;
            let mut dirty = 0;
            if !gone.is_empty() {
                let seq = sqlx::query_scalar!(
                    "SELECT value FROM core.counters WHERE name = 'commit_seq'"
                )
                .fetch_one(&mut *conn)
                .await?;
                dirty += mark_archived_dirty(conn, &gone, seq, keys).await?;
                sqlx::query!(
                    r#"INSERT INTO audit.archived (event_id, archived_seq, reason)
                       SELECT id, $2, 'recomputed' FROM unnest($1::uuid[]) AS id ON CONFLICT (event_id) DO NOTHING"#,
                    &gone,
                    seq,
                )
                .execute(&mut *conn)
                .await?;
                remove_live(conn, &gone).await?;
            }
            sqlx::query!(
                "DELETE FROM derivation.outputs WHERE derivation = $1 AND key = $2",
                derivation,
                key,
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                r#"INSERT INTO derivation.outputs (derivation, key, event_id)
                   SELECT $1, $2, id FROM unnest($3::uuid[]) AS id ON CONFLICT DO NOTHING"#,
                derivation,
                key,
                &live,
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                "DELETE FROM derivation.dirty WHERE derivation = $1 AND key = $2 AND last_dirty_seq <= $3",
                derivation,
                key,
                dirty_seq,
            )
            .execute(&mut *conn)
            .await?;
            if waiting {
                sqlx::query!(
                    "INSERT INTO derivation.waiting (derivation, key) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    derivation,
                    key,
                )
                .execute(&mut *conn)
                .await?;
            } else {
                sqlx::query!(
                    "DELETE FROM derivation.waiting WHERE derivation = $1 AND key = $2",
                    derivation,
                    key,
                )
                .execute(&mut *conn)
                .await?;
            }
            Ok(dirty)
        }
    }
}

async fn mark_dirty(
    conn: &mut PgConnection,
    dirty: Vec<(&'static str, Value)>,
    seq: i64,
) -> Result<usize> {
    if dirty.is_empty() {
        return Ok(0);
    }
    let (names, keys): (Vec<String>, Vec<Value>) =
        dirty.into_iter().map(|(n, k)| (n.to_owned(), k)).unzip();
    let n = names.len();
    sqlx::query!(
        r#"INSERT INTO derivation.dirty (derivation, key, first_dirty_seq, last_dirty_seq)
           SELECT DISTINCT d, k, $3::bigint, $3::bigint FROM unnest($1::text[], $2::jsonb[]) AS u(d, k)
           ON CONFLICT (derivation, key) DO UPDATE SET last_dirty_seq = EXCLUDED.last_dirty_seq"#,
        &names,
        &keys,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(n)
}

/// Archived readings make their keys dirty too: a derivation's output must stop
/// counting them.
pub(crate) async fn mark_archived_dirty(
    conn: &mut PgConnection,
    ids: &[Uuid],
    seq: i64,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let rows = sqlx::query!(
        "SELECT event_type, ts_orig, payload FROM core.events WHERE id = ANY($1)",
        ids
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut dirty = Vec::new();
    for row in &rows {
        dirty.extend(keys.keys(&KeyInput {
            event_type: &row.event_type,
            ts_orig: row.ts_orig,
            payload: &row.payload,
        }));
    }
    // Results whose inputs were archived are stale too.
    crate::analysis::mark_stale(conn, ids, seq).await?;
    mark_dirty(conn, dirty, seq).await
}

/// Hide and unhide work on equivalence keys, or record locations for
/// keyless types, and need nothing per source (architecture §5.5).
async fn archive_request(
    conn: &mut PgConnection,
    request: &ArchiveRequest,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let seq = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
        .fetch_one(&mut *conn)
        .await?;
    match request {
        ArchiveRequest::Replay { .. } | ArchiveRequest::RemovedAtSource { .. } => Ok(0),
        ArchiveRequest::Hide { target, operation } => {
            let kind = "hide";
            let event = sqlx::query!(
                "SELECT source, event_type, equivalence_key, material_id, anchor, ordinal FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            sqlx::query!(
                r#"INSERT INTO core.hidden_occurrences (source, event_type, equivalence_key, material_id, anchor, ordinal, kind, operation_id)
                   VALUES ($1, $2, $3,
                     CASE WHEN $3::text IS NULL THEN $4::uuid END,
                     CASE WHEN $3::text IS NULL THEN $5::bigint END,
                     CASE WHEN $3::text IS NULL THEN $6::smallint END, $7, $8)
                   ON CONFLICT DO NOTHING"#,
                event.source,
                event.event_type,
                event.equivalence_key,
                event.material_id,
                event.anchor,
                event.ordinal,
                kind,
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
            let dirty = mark_archived_dirty(conn, &live, seq, keys).await?;
            sqlx::query!(
                r#"INSERT INTO audit.archived (event_id, archived_seq, reason, operation_id)
                   SELECT id, $2, $3, $4 FROM unnest($1::uuid[]) AS id ON CONFLICT (event_id) DO NOTHING"#,
                &live,
                seq,
                kind,
                operation.0,
            )
            .execute(&mut *conn)
            .await?;
            remove_live(conn, &live).await?;
            Ok(dirty)
        }
        ArchiveRequest::Unhide {
            target,
            operation: _,
        } => {
            let event = sqlx::query!(
                "SELECT source, event_type, equivalence_key, material_id, anchor, ordinal FROM core.events WHERE id = $1",
                target.0
            )
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| Error::Invalid(format!("no event {target}")))?;
            let removed = sqlx::query_scalar!(
                r#"DELETE FROM core.hidden_occurrences WHERE kind = 'hide' AND source = $1 AND event_type = $2
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
            sqlx::query!("DELETE FROM audit.archived WHERE event_id = $1", r.id)
                .execute(&mut *conn)
                .await?;
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
                        "INSERT INTO core.live_coordinates (material_id, anchor, event_type, ordinal, event_id, reading_order, record_hash) VALUES ($1,$2,$3,$4,$5,$6,$7)",
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
            "SELECT event_id FROM core.live_coordinates WHERE material_id = $1 AND anchor = $2 AND event_type = $3 AND ordinal = $4",
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
        "DELETE FROM core.live_coordinates WHERE event_id = ANY($1)",
        ids
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod privacy_tests;
