//! Committed events and lifecycle changes share the global commit position.
//! [`changes_page`] is the ordered feed, including completion markers
//! for scalar checkpoints and named gaps for unknown historical metadata.
//! [`after`] is the event-only view used by internal event derivations.
//! A tombstone keeps positions and payload-free lifecycle history; each
//! event's stream sequences are tracked per stream instance for erasure.
use crate::event_row::EventRow;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::{AdmissionCounts, EventId, FrameRefusal, KindRef, OperationId};
use sqlx::PgConnection;
use sqlx::types::Json;
use time::OffsetDateTime;
use uuid::Uuid;

mod changes;
mod stream_start;
pub use changes::{
    Change, ChangeCursor, ChangePage, CommittedChange, EventHeader, HistoryGap, LifecycleChange,
    StreamEvent, Tombstone, changes_page, stream_changes_after_start,
};
pub use stream_start::StreamStart;

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

/// Payload-free metadata kept for erasure. Historical missing values stay unknown.
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
    let row=sqlx::query_as!(ErasedReading, "SELECT event_type,class,commit_seq,original_commit_seq FROM core.event_tombstones WHERE event_id=$1",event.0)
        .fetch_optional(conn).await?;
    Ok(row)
}

/// One committed event as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct Committed {
    pub row: EventRow,
    pub search: sinex_model::kinds::Search,
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
/// the events' payloads pass `bytes` (the first position is always read).
/// Event positions are on the events; the commit log holds the positions that
/// admitted no event. The page ends at the last position it read a row for,
/// never at a later head, so a commit whose rows this read could not see is
/// read by the next page.
pub async fn after(conn: &mut PgConnection, after: i64, limit: i64, bytes: i64) -> Result<Page> {
    if limit <= 0 {
        return Err(Error::Invalid("commit page limit must be positive".into()));
    }
    let selected: Vec<Selected> = sqlx::query_file!("src/commit_log/page.sql", after, limit)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| Selected {
            commit_seq: row.commit_seq,
            event: row.id.map(|id| SelectedEvent {
                id,
                event_type: row.event_type.unwrap_or_default(),
                ts_orig: row.ts_orig,
            }),
        })
        .collect();
    let (rows, up_to) = fetch_selected(conn, &selected, bytes).await?;
    let mut events: Vec<_> = rows
        .into_iter()
        .map(|row| Committed {
            row,
            search: sinex_model::kinds::Search::None,
        })
        .collect();
    add_declarations(conn, &mut events).await?;
    Ok(Page { events, up_to })
}

/// [`after`], read in one repeatable-read snapshot of its own. Its statements
/// are planned after that snapshot is taken, so the chunks they scan include
/// every chunk holding rows the snapshot sees; a read-committed statement
/// planned before a commit and run after it could miss that commit's new chunk.
pub async fn after_in_one_snapshot(
    conn: &mut PgConnection,
    from: i64,
    limit: i64,
    bytes: i64,
) -> Result<Page> {
    let mut snapshot = sqlx::Connection::begin(&mut *conn).await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *snapshot)
        .await?;
    let page = after(&mut snapshot, from, limit, bytes).await?;
    snapshot.commit().await?;
    Ok(page)
}

/// Fill each event's search mode from its stored kind declaration.
pub(crate) async fn add_declarations(
    conn: &mut PgConnection,
    events: &mut [Committed],
) -> Result<()> {
    let references: Vec<_> = events
        .iter()
        .map(|e| sinex_model::KindRef {
            name: e.row.event_type.clone(),
            version: e.row.kind_version as u64,
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let declarations = sinex_schema::kinds::load(conn, &references).await?;
    for e in events {
        let reference = sinex_model::KindRef {
            name: e.row.event_type.clone(),
            version: e.row.kind_version as u64,
        };
        let d = declarations
            .get(&reference)
            .ok_or_else(|| Error::Invalid(format!("missing stored declaration {reference:?}")))?;
        e.search = d.search;
    }
    Ok(())
}

/// One position a page selected, with the event at it when there is one.
pub(crate) struct Selected {
    pub commit_seq: i64,
    pub event: Option<SelectedEvent>,
}

/// What bounds the read of a selected event's row.
pub(crate) struct SelectedEvent {
    pub id: Uuid,
    pub event_type: String,
    pub ts_orig: Option<OffsetDateTime>,
}

/// How many selected events one row read covers. Reading a page in groups
/// keeps the payloads read past its byte bound to one group.
const ROWS_PER_READ: usize = 64;

/// Read the selected events' rows, each payload once, in the selected order,
/// stopping once the payloads read pass `bytes` (the first is always read).
/// Returns the events and the last position covered. A selected event whose
/// row is gone (tombstoned since) covers its position without an event.
pub(crate) async fn fetch_selected(
    conn: &mut PgConnection,
    selected: &[Selected],
    bytes: i64,
) -> Result<(Vec<EventRow>, Option<i64>)> {
    let mut events = Vec::new();
    let mut up_to = None;
    let mut read = 0i64;
    for group in selected.chunks(ROWS_PER_READ) {
        let mut rows = read_rows(conn, group).await?;
        for position in group {
            if read >= bytes {
                return Ok((events, up_to));
            }
            up_to = Some(position.commit_seq);
            let Some(event) = &position.event else {
                continue;
            };
            let Some(mut row) = rows.remove(&event.id) else {
                continue;
            };
            read = read.saturating_add(crate::payload_bytes(&row.payload));
            row.commit_seq = position.commit_seq;
            events.push(row);
        }
    }
    Ok((events, up_to))
}

/// The rows of one group's events. Inclusive ID and time bounds and the kinds
/// let compressed batches be skipped by their order and segment columns; the
/// ID list keeps exact membership.
pub(crate) async fn read_rows(
    conn: &mut PgConnection,
    group: &[Selected],
) -> Result<std::collections::HashMap<Uuid, EventRow>> {
    let events: Vec<&SelectedEvent> = group.iter().filter_map(|p| p.event.as_ref()).collect();
    let mut rows = std::collections::HashMap::with_capacity(events.len());
    let kinds: Vec<String> = events
        .iter()
        .map(|e| e.event_type.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let (timed, untimed): (Vec<&SelectedEvent>, Vec<&SelectedEvent>) =
        events.into_iter().partition(|e| e.ts_orig.is_some());
    if !timed.is_empty() {
        let ids: Vec<Uuid> = timed.iter().map(|e| e.id).collect();
        let times = timed.iter().filter_map(|e| e.ts_orig);
        let (earliest, latest) = (times.clone().min(), times.max());
        let found = sqlx::query_file_as!(
            EventRow,
            "src/commit_log/rows.sql",
            &ids,
            ids.iter().min().copied(),
            ids.iter().max().copied(),
            &kinds,
            earliest,
            latest,
        )
        .fetch_all(&mut *conn)
        .await?;
        for row in found {
            rows.insert(row.id, row);
        }
    }
    if !untimed.is_empty() {
        let ids: Vec<Uuid> = untimed.iter().map(|e| e.id).collect();
        let found = sqlx::query_file_as!(
            EventRow,
            "src/commit_log/rows_without_time.sql",
            &ids,
            ids.iter().min().copied(),
            ids.iter().max().copied(),
            &kinds,
        )
        .fetch_all(&mut *conn)
        .await?;
        for row in found {
            rows.insert(row.id, row);
        }
    }
    Ok(rows)
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
    /// Where the material's bytes are read from, one descriptor per extent in order.
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
    /// `blake3:<hex>` of the frame body recorded under this name;
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

/// A transfer page of the frames recorded for this authenticated producer.
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

/// Reserve a frame number inside the admission transaction that records the frame.
pub async fn reserve_frame_number(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar(
            "SELECT nextval(pg_get_serial_sequence('core.ingress_frames','number'))",
        )
        .fetch_one(conn)
        .await?,
    )
}

/// A MATERIAL frame's stored material and the frame Sinex recorded for it.
/// Fields that older-format frames lack stay null; current grants never fill them in.
pub struct RecordedMaterial {
    pub stored_frame: StoredFrame,
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
        stored_frame: StoredFrame {
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
