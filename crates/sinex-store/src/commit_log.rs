//! Committed events and lifecycle changes share the global commit position.
//! [`changes_page`] is the canonical ordered feed, including completion markers
//! for scalar checkpoints and named gaps for unknown historical metadata.
//! [`after`] is the event-only projection used by internal event derivations.
//! A tombstone retains positions and payload-free lifecycle history; exact
//! stream placements are tracked per native stream incarnation for erasure.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{
    AdmissionCounts, Class, DbCheck, Event, EventId, FrameRefusal, KindRef, MaterialId,
    OperationId, Origin, ReadingOrder, RecordLocation, RecordState, TimeQuality, Timestamp, TsOrig,
};
use sqlx::PgConnection;
use sqlx::types::Json;
use time::OffsetDateTime;
use uuid::Uuid;

mod changes;
mod origin;
pub use changes::{
    Change, ChangeCursor, ChangePage, CommittedChange, EventReference, HistoryGap, LifecycleChange,
    Tombstone, changes_page, changes_page_after_origin, references_after_origin,
};
pub use origin::HistoryOrigin;

/// Take a lifecycle position while holding the same counter lock as admission.
pub(crate) async fn allocate_position(conn: &mut PgConnection) -> Result<i64> {
    let position = sqlx::query_scalar!(
        "UPDATE core.counters SET value=value+1 WHERE name='commit_seq' RETURNING value"
    )
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query!(
        "INSERT INTO core.commit_log(commit_seq) VALUES($1)",
        position
    )
    .execute(conn)
    .await?;
    Ok(position)
}

/// Payload-free retained erasure metadata. Historical missing values stay unknown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErasedReading {
    pub event_type: String,
    pub class: Option<String>,
    pub commit_seq: Option<i64>,
    pub original_commit_seq: Option<i64>,
}

pub async fn erased_reading(
    conn: &mut PgConnection,
    event: EventId,
) -> Result<Option<ErasedReading>> {
    let row=sqlx::query!("SELECT event_type,class,commit_seq,original_commit_seq FROM core.event_tombstones WHERE event_id=$1",event.0)
        .fetch_optional(conn).await?;
    Ok(row.map(|row| ErasedReading {
        event_type: row.event_type,
        class: row.class,
        commit_seq: row.commit_seq,
        original_commit_seq: row.original_commit_seq,
    }))
}

/// One committed event as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct Committed {
    pub commit_seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub kind_version: i64,
    pub validation: String,
    pub search: sinex_model::kinds::Search,
    pub replace_rule: Option<String>,
    pub source: String,
    pub run_id: Uuid,
    pub material_id: Option<Uuid>,
    pub anchor: Option<i64>,
    pub output_index: i64,
    pub parents: Option<Vec<Uuid>>,
    pub ts_orig: Option<OffsetDateTime>,
    pub ts_quality: String,
    pub ts_coided: Option<OffsetDateTime>,
    pub ts_persisted: OffsetDateTime,
    pub payload: Value,
    pub equivalence_key: Option<String>,
    pub reading_order: i64,
    pub record_hash: Vec<u8>,
    pub record_state: String,
    pub class: String,
}

/// A page of committed events and the commit position it covers.
#[derive(Clone, Debug, Default)]
pub struct Page {
    pub events: Vec<Committed>,
    /// The last commit position this page covered, including tombstoned events
    /// it skipped; `None` when there was nothing after the position.
    pub up_to: Option<i64>,
}

/// Up to `limit` positions after `after`, in commit order, ending early once
/// the events' stored payloads pass `bytes` (the first position is always
/// read). Event positions are on the events; the commit log holds the
/// positions that admitted no event.
pub async fn after(conn: &mut PgConnection, after: i64, limit: i64, bytes: i64) -> Result<Page> {
    if limit <= 0 {
        return Err(Error::Invalid("commit page limit must be positive".into()));
    }
    let rows = sqlx::query_file!("src/commit_log/page.sql", after, limit, bytes,)
        .fetch_all(&mut *conn)
        .await?;
    let mut page = Page::default();
    for row in rows {
        let Some(position) = row.commit_seq else {
            if row.head > after {
                page.up_to = Some(row.head);
            }
            continue;
        };
        page.up_to = Some(position);
        // Tombstoned: the content is gone, the position is not.
        let (Some(id), Some(event_type), Some(source), Some(run_id)) =
            (row.id, row.event_type, row.source, row.run_id)
        else {
            continue;
        };
        page.events.push(Committed {
            search: sinex_model::kinds::Search::None,
            commit_seq: position,
            id,
            event_type,
            kind_version: row
                .kind_version
                .ok_or_else(|| Error::Invalid("missing kind version".into()))?,
            validation: row
                .validation
                .ok_or_else(|| Error::Invalid("missing validation".into()))?,
            replace_rule: None,
            source,
            run_id,
            material_id: row.material_id,
            anchor: row.anchor,
            output_index: row
                .output_index
                .ok_or_else(|| Error::Invalid("event has no output index".into()))?,
            parents: row.parents,
            ts_orig: row.ts_orig,
            ts_quality: row.ts_quality.unwrap_or_default(),
            ts_coided: row.ts_coided,
            ts_persisted: row.ts_persisted.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            payload: row.payload.unwrap_or(Value::Null),
            equivalence_key: row.equivalence_key,
            reading_order: row.reading_order.unwrap_or(0),
            record_hash: row.record_hash.unwrap_or_default(),
            record_state: row.record_state.unwrap_or_default(),
            class: row.class.unwrap_or_default(),
        });
    }
    let references: Vec<_> = page
        .events
        .iter()
        .map(|e| sinex_model::KindRef {
            name: e.event_type.clone(),
            version: e.kind_version as u64,
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let declarations = sinex_schema::kinds::load(conn, &references).await?;
    for e in &mut page.events {
        let reference = sinex_model::KindRef {
            name: e.event_type.clone(),
            version: e.kind_version as u64,
        };
        let d = declarations
            .get(&reference)
            .ok_or_else(|| Error::Invalid(format!("missing stored declaration {reference:?}")))?;
        e.replace_rule = d.replace_rule.map(|r| r.as_db().to_string());
        e.search = d.search;
    }
    Ok(page)
}

impl Committed {
    /// Reconstruct the exact stored reading, including outside kinds.
    pub fn to_event(&self) -> Result<Event> {
        let origin = match (&self.material_id, self.anchor, &self.parents) {
            (Some(material), Some(anchor), _) => Origin::Record {
                location: RecordLocation {
                    material: MaterialId(*material),
                    anchor: u64::try_from(anchor)
                        .map_err(|_| Error::Invalid(format!("negative anchor on {}", self.id)))?,
                },
                output_index: u64::try_from(self.output_index)
                    .map_err(|_| Error::Invalid(format!("negative output_index on {}", self.id)))?,
            },
            (_, _, Some(parents)) => Origin::Derived {
                parents: parents.iter().map(|p| EventId(*p)).collect(),
            },
            _ => return Err(Error::Invalid(format!("event {} has no origin", self.id))),
        };
        let quality: TimeQuality = from_db(&self.ts_quality)?;
        let record_hash: [u8; 32] =
            self.record_hash.as_slice().try_into().map_err(|_| {
                Error::Invalid(format!("record hash of {} is not 32 bytes", self.id))
            })?;
        Ok(Event {
            id: EventId(self.id),
            event_type: self.event_type.clone(),
            kind_version: self.kind_version as u64,
            validation: from_db(&self.validation)?,
            source: self.source.clone(),
            origin,
            ts_orig: TsOrig::maybe(self.ts_orig.map(Timestamp), quality),
            payload: self.payload.clone(),
            equivalence_key: self
                .equivalence_key
                .clone()
                .map(sinex_model::EquivalenceKey),
            replace: self.replace_rule.as_deref().map(from_db).transpose()?,
            reading_order: ReadingOrder(self.reading_order),
            record_hash,
            record_state: from_db::<RecordState>(&self.record_state)?,
            class: from_db::<Class>(&self.class)?,
            owned_blobs: Vec::new(),
        })
    }
}

fn from_db<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_value(Value::String(text.to_owned()))
        .map_err(|e| Error::Invalid(format!("stored value {text:?}: {e}")))
}

/// A reading taken out of or put back into the live set.
#[derive(Clone, Debug, PartialEq)]
pub struct ArchiveChange {
    pub change_seq: i64,
    pub event_id: Uuid,
    /// The writer position at which availability changed.
    pub commit_seq: i64,
    pub archived: bool,
    pub reason: String,
    pub replaced_by: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub recorded_at: OffsetDateTime,
}

/// Up to `limit` archive changes after `after`, in the order they committed.
pub async fn changes_after(
    conn: &mut PgConnection,
    after: i64,
    limit: i64,
) -> Result<Vec<ArchiveChange>> {
    Ok(sqlx::query_as!(
        ArchiveChange,
        r#"SELECT change_seq, event_id, commit_seq, archived, reason, replaced_by, operation_id, recorded_at
           FROM audit.archive_changes WHERE change_seq > $1 ORDER BY change_seq LIMIT $2"#,
        after,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// The highest commit position so far.
pub async fn latest(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_one(&mut *conn)
            .await?,
    )
}

pub async fn consumer_exists(conn: &mut PgConnection, consumer: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        "SELECT EXISTS(SELECT 1 FROM core.consumer_positions WHERE consumer=$1)",
        consumer
    )
    .fetch_one(conn)
    .await?
    .unwrap_or(false))
}

/// A non-derivation consumer's saved position: (commit_seq, change_seq).
pub async fn consumer_position(conn: &mut PgConnection, consumer: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query!(
        "SELECT commit_seq, change_seq FROM core.consumer_positions WHERE consumer = $1",
        consumer
    )
    .fetch_optional(&mut *conn)
    .await?
    .map_or((0, 0), |r| (r.commit_seq, r.change_seq)))
}

/// Save a consumer's position. Positions only move forward.
pub async fn set_consumer_position(
    conn: &mut PgConnection,
    consumer: &str,
    commit_seq: i64,
    change_seq: i64,
) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO core.consumer_positions (consumer, commit_seq, change_seq) VALUES ($1, $2, $3)
           ON CONFLICT (consumer) DO UPDATE SET
             commit_seq = greatest(core.consumer_positions.commit_seq, EXCLUDED.commit_seq),
             change_seq = greatest(core.consumer_positions.change_seq, EXCLUDED.change_seq),
             updated_at = now()"#,
        consumer,
        commit_seq,
        change_seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Move a consumer's saved position back to `commit_seq`. Used only when the
/// confirmed stream lost messages the position covered (a host crash before
/// JetStream synced them), so the lost positions are published again.
pub async fn rewind_consumer_position(
    conn: &mut PgConnection,
    consumer: &str,
    commit_seq: i64,
) -> Result<()> {
    sqlx::query!(
        r#"UPDATE core.consumer_positions SET commit_seq = $2, change_seq = 0, updated_at = now()
           WHERE consumer = $1 AND commit_seq > $2"#,
        consumer,
        commit_seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// What Sinex recorded for one ingress frame.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredFrame {
    pub number: i64,
    /// The material's read authority, one descriptor per extent in order.
    pub material_extents: Option<Vec<Vec<u8>>>,
    pub producer: String,
    pub frame: String,
    pub material_id: Option<Uuid>,
    pub admitted: Vec<Uuid>,
    pub outcome: Json<FrameOutcome>,
    pub refusal: Option<String>,
    pub commit_seq: Option<i64>,
}

/// The `outcome` column of `core.ingress_frames`. The frame's material, its
/// admitted events, its last commit position and a refusal's reason are
/// columns beside it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameOutcome {
    /// `blake3:<hex>` of the logical frame body recorded under this name;
    /// unknown for frames recorded before Sinex kept it.
    pub content_hash: Option<String>,
    #[serde(flatten)]
    pub result: FrameResult,
}

/// What admission did with one frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum FrameResult {
    /// Events, material, signal and chunk frames.
    Admitted {
        counts: AdmissionCounts,
        commit_range: Option<(i64, i64)>,
    },
    KindsRegistered {
        kinds: Vec<(KindRef, String)>,
    },
    /// A producer replay's replacement, admitted in its own transactions.
    Replayed {
        operation: OperationId,
    },
    /// Refused as a whole: no events, the reason in the `refusal` column.
    Refused {
        kind: FrameRefusal,
        /// Record failures the refusal left on the cited materials.
        failures: usize,
        /// The source named at the start of a capture binding.
        source: Option<String>,
        event_types: Vec<String>,
    },
}

impl StoredFrame {
    pub fn content_hash(&self) -> Option<&str> {
        self.outcome.content_hash.as_deref()
    }

    pub fn refusal_kind(&self) -> Option<FrameRefusal> {
        match self.outcome.result {
            FrameResult::Refused { kind, .. } => Some(kind),
            _ => None,
        }
    }
}

/// The recorded frames among `frames` from `producer`.
pub async fn frames(
    conn: &mut PgConnection,
    producer: &str,
    frames: &[String],
) -> Result<Vec<StoredFrame>> {
    Ok(sqlx::query_as!(
        StoredFrame,
        r#"SELECT f.number, f.producer, f.frame, f.material_id, f.admitted, f.outcome AS "outcome: Json<FrameOutcome>", f.refusal, f.commit_seq, core.blob_descriptors(m.blob) AS "material_extents?"
           FROM core.ingress_frames f LEFT JOIN raw.materials m ON m.id=f.material_id
           WHERE f.producer = $1 AND f.frame = ANY($2)
           ORDER BY f.received_at, f.frame"#,
        producer,
        frames,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// A transfer page of immutable receipts owned by this authenticated producer.
pub async fn frames_page(
    conn: &mut PgConnection,
    producer: &str,
    frames: &[String],
    numbers: &[i64],
    after: i64,
) -> Result<(Vec<StoredFrame>, Option<i64>)> {
    const PAGE: i64 = 64;
    let rows=sqlx::query_as!(StoredFrame,r#"SELECT f.number,f.producer,f.frame,f.material_id,f.admitted,f.outcome AS "outcome: Json<FrameOutcome>",f.refusal,f.commit_seq,core.blob_descriptors(m.blob) AS "material_extents?"
      FROM core.ingress_frames f LEFT JOIN raw.materials m ON m.id=f.material_id
      WHERE f.producer=$1 AND (f.frame=ANY($2) OR f.number=ANY($3)) AND f.number>$4 ORDER BY f.number LIMIT $5"#,producer,frames,numbers,after,PAGE).fetch_all(conn).await?;
    let next = (rows.len() == PAGE as usize).then(|| rows.last().expect("full page").number);
    Ok((rows, next))
}

/// Whether this producer registered the material named by its events frame.
pub async fn producer_material_registered(
    conn: &mut PgConnection,
    producer: &str,
    material: Uuid,
) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT EXISTS (
        SELECT 1 FROM core.ingress_frames WHERE producer = $1 AND material_id = $2
    ) AS "registered!""#,
        producer,
        material
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// One material a producer registered, as its frame described it.
#[derive(Clone, Debug, PartialEq)]
pub struct ProducerMaterial {
    /// One descriptor per extent, in reading order.
    pub extents: Vec<Vec<u8>>,
    pub number: i64,
    pub frame: String,
    pub material_id: Uuid,
    pub kind: String,
    pub locator: String,
    pub host: String,
    pub blob: String,
    pub length: i64,
    pub part_index: i32,
    pub previous_part: Option<Uuid>,
    pub parser: String,
    pub parser_version: i32,
    pub manifest: serde_json::Value,
}

/// Up to `limit` materials `producer` registered after the frame numbered
/// `after`, that Sinex still holds (not tombstoned), in the order their frames
/// were recorded.
pub async fn producer_materials(
    conn: &mut PgConnection,
    producer: &str,
    after: i64,
    limit: i64,
) -> Result<Vec<ProducerMaterial>> {
    Ok(sqlx::query_as!(
        ProducerMaterial,
        r#"SELECT f.number AS "number!", f.frame, m.id AS material_id, i.kind, i.locator, i.host,
                  m.blob, m.length AS length, m.part_index, m.previous_part_id AS previous_part,
                  m.parser AS "parser!", m.parser_version AS "parser_version!", m.manifest, core.blob_descriptors(m.blob) AS "extents!"
           FROM core.ingress_frames f
           JOIN raw.materials m ON m.id = f.material_id
           JOIN raw.inputs i ON i.id = m.input_id
           WHERE core.blob_descriptors(m.blob) IS NOT NULL AND f.producer = $1 AND f.refusal IS NULL AND f.number > $2
             AND m.status <> 'tombstoned'
           ORDER BY f.number LIMIT $3"#,
        producer,
        after,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Record where published events sit in the confirmed stream.
#[cfg(test)]
pub async fn record_published(conn: &mut PgConnection, published: &[(Uuid, i64)]) -> Result<()> {
    record_published_in(conn, "", published).await
}

/// Forget every recorded stream position above `last_seq`: the stream no
/// longer holds those messages, and its next messages will reuse the numbers.
pub async fn forget_published_above(
    conn: &mut PgConnection,
    stream_instance: &str,
    last_seq: i64,
) -> Result<u64> {
    Ok(sqlx::query!(
        "DELETE FROM core.published WHERE stream_instance = $1 AND stream_seq > $2",
        stream_instance,
        last_seq,
    )
    .execute(&mut *conn)
    .await?
    .rows_affected())
}

pub async fn record_published_in(
    conn: &mut PgConnection,
    stream_instance: &str,
    published: &[(Uuid, i64)],
) -> Result<()> {
    if published.is_empty() {
        return Ok(());
    }
    let mut pairs = published.to_vec();
    pairs.sort_unstable();
    pairs.dedup();
    let (ids, seqs): (Vec<Uuid>, Vec<i64>) = pairs.into_iter().unzip();
    sqlx::query!(
        r#"INSERT INTO core.published (event_id, stream_seq, stream_instance)
           SELECT event_id,stream_seq,$3 FROM unnest($1::uuid[], $2::bigint[]) AS p(event_id,stream_seq)
           ON CONFLICT (event_id, stream_seq, stream_instance) DO NOTHING"#,
        &ids,
        &seqs,
        stream_instance,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A tombstoned event whose message is still on the confirmed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstonedOnStream {
    pub commit_seq: Option<i64>,
    pub event_id: Uuid,
    pub stream_seq: i64,
    pub operation_id: Uuid,
}

/// Up to `limit` tombstoned events whose messages are still on the stream.
#[cfg(test)]
pub async fn tombstoned_on_stream(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<TombstonedOnStream>> {
    tombstoned_on_stream_in(conn, "", limit).await
}

pub async fn tombstoned_on_stream_in(
    conn: &mut PgConnection,
    stream_instance: &str,
    limit: i64,
) -> Result<Vec<TombstonedOnStream>> {
    Ok(sqlx::query_as!(
        TombstonedOnStream,
        r#"SELECT p.event_id, p.stream_seq, t.operation_id,t.commit_seq
           FROM core.event_tombstones t
           JOIN core.published p ON p.event_id = t.event_id
           WHERE p.stream_instance=$2
           ORDER BY p.stream_seq, p.event_id LIMIT $1"#,
        limit,
        stream_instance,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Forget only acknowledged erasures, keeping every other acknowledged copy.
#[cfg(test)]
pub async fn forget_published(conn: &mut PgConnection, erased: &[(Uuid, i64)]) -> Result<()> {
    forget_published_in(conn, "", erased).await
}

pub async fn forget_published_in(
    conn: &mut PgConnection,
    stream_instance: &str,
    erased: &[(Uuid, i64)],
) -> Result<()> {
    if erased.is_empty() {
        return Ok(());
    }
    let mut pairs = erased.to_vec();
    pairs.sort_unstable();
    pairs.dedup();
    let (ids, seqs): (Vec<Uuid>, Vec<i64>) = pairs.into_iter().unzip();
    sqlx::query!(
        r#"DELETE FROM core.published p
           USING unnest($1::uuid[], $2::bigint[]) AS e(event_id, stream_seq)
           WHERE p.event_id = e.event_id AND p.stream_seq = e.stream_seq AND p.stream_instance=$3"#,
        &ids,
        &seqs,
        stream_instance,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Record a producer replay's acknowledgment in its replacement transaction.
pub async fn record_replay_frame(
    conn: &mut PgConnection,
    producer: &str,
    frame: &str,
    family: &str,
    write_kind_prefixes: &[String],
    operation: sinex_model::OperationId,
    ids: &[Uuid],
    last: Option<i64>,
    number: i64,
    content_hash: &str,
    source: &sinex_model::SourceRef,
    fingerprint: &str,
    host: Option<&str>,
) -> Result<()> {
    let outcome = Json(FrameOutcome {
        content_hash: Some(content_hash.to_owned()),
        result: FrameResult::Replayed { operation },
    });
    sqlx::query("INSERT INTO core.ingress_frames (producer,frame,admitted,outcome,commit_seq,family,write_kind_prefixes,number,source_name,source_version,source_fingerprint,producer_host) OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)")
        .bind(producer).bind(frame).bind(ids).bind(outcome).bind(last).bind(family).bind(write_kind_prefixes).bind(number).bind(&source.name).bind(i64::from(source.version)).bind(fingerprint).bind(host).execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests;

/// Reserve the existing receipt identity inside the owning admission transaction.
pub async fn reserve_frame_number(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar(
            "SELECT nextval(pg_get_serial_sequence('core.ingress_frames','number'))",
        )
        .fetch_one(conn)
        .await?,
    )
}

/// Original own MATERIAL custody and its immutable receipt provenance. Nullable
/// legacy fields remain nullable; current grants never backfill their meaning.
pub struct RecordedMaterial {
    pub receipt: StoredFrame,
    pub blob: Option<String>,
    pub length: Option<i64>,
    pub source_name: Option<String>,
    pub source_version: Option<i64>,
    pub source_fingerprint: Option<String>,
    pub family: Option<String>,
    pub write_kind_prefixes: Option<Vec<String>>,
    pub producer_host: Option<String>,
    pub available: bool,
}

pub async fn recorded_material(
    conn: &mut PgConnection,
    producer: &str,
    frame: &str,
) -> Result<Option<RecordedMaterial>> {
    let row = sqlx::query!(r#"SELECT f.number,f.producer,f.frame,f.material_id,f.admitted,f.outcome AS "outcome: Json<FrameOutcome>",f.refusal,f.commit_seq,
        core.blob_descriptors(m.blob) AS "material_extents?",m.blob AS "blob?",m.length AS "length?",
        f.source_name,f.source_version,f.source_fingerprint,f.family,f.write_kind_prefixes,f.producer_host,
        (m.id IS NOT NULL AND core.blob_descriptors(m.blob) IS NOT NULL AND m.status <> 'tombstoned') AS "available!"
        FROM core.ingress_frames f LEFT JOIN raw.materials m ON m.id=f.material_id
        WHERE f.producer=$1 AND f.frame=$2"#,producer,frame)
        .fetch_optional(conn).await?;
    Ok(row.map(|row| RecordedMaterial {
        receipt: StoredFrame {
            number: row.number,
            material_extents: row.material_extents,
            producer: row.producer,
            frame: row.frame,
            material_id: row.material_id,
            admitted: row.admitted,
            outcome: row.outcome,
            refusal: row.refusal,
            commit_seq: row.commit_seq,
        },
        blob: row.blob,
        length: row.length,
        source_name: row.source_name,
        source_version: row.source_version,
        source_fingerprint: row.source_fingerprint,
        family: row.family,
        write_kind_prefixes: row.write_kind_prefixes,
        producer_host: row.producer_host,
        available: row.available,
    }))
}
