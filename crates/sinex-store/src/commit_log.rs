//! Committed events and lifecycle changes share the global commit position.
//! [`changes_page`] is the canonical ordered feed, including completion markers
//! for scalar checkpoints and named gaps for unknown historical metadata.
//! [`after`] is the event-only projection used by internal event derivations.
//! Excision retains positions and payload-free lifecycle history; exact stream
//! placements are tracked per native stream incarnation for physical erasure.
use crate::{Error, Result};
use serde_json::Value;
use sinex_model::{
    Class, DbCheck, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation, RecordState,
    TimeQuality, Timestamp, TsOrig,
};
use sqlx::PgConnection;
use time::OffsetDateTime;
use uuid::Uuid;

mod changes;
pub use changes::{
    Change, ChangeCursor, ChangePage, CommittedChange, HistoryGap, LifecycleChange, Tombstone,
    changes_page,
};

/// Take a lifecycle position while holding the same counter lock as admission.
pub(crate) async fn allocate_position(conn: &mut PgConnection) -> Result<i64> {
    let position = sqlx::query_scalar!(
        "UPDATE core.counters SET value=value+1 WHERE name='commit_seq' RETURNING value"
    )
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query!(
        "INSERT INTO core.commit_log(commit_seq,event_id,event_type) VALUES($1,NULL,NULL)",
        position
    )
    .execute(conn)
    .await?;
    Ok(position)
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
    /// The last commit position this page covered, including excised events
    /// it skipped; `None` when there was nothing after the position.
    pub up_to: Option<i64>,
}

/// Up to `limit` commit-log entries after `after`, in commit order, ending
/// early once the events' stored payloads pass `bytes` (the first entry is
/// always read).
pub async fn after(conn: &mut PgConnection, after: i64, limit: i64, bytes: i64) -> Result<Page> {
    if limit <= 0 {
        return Err(Error::Invalid("commit page limit must be positive".into()));
    }
    let rows = sqlx::query!(
        r#"SELECT head.value AS "head!", c.commit_seq AS "commit_seq?", e.id AS "id?", e.event_type AS "event_type?",
             e.source AS "source?", e.run_id AS "run_id?", e.material_id, e.anchor,
             e.output_index AS "output_index?", e.parents, e.ts_orig, e.ts_quality AS "ts_quality?",
             e.ts_coided, e.ts_persisted AS "ts_persisted?", e.payload AS "payload?",
             e.equivalence_key, e.reading_order AS "reading_order?",
             e.record_hash AS "record_hash?", e.record_state AS "record_state?", e.class AS "class?", e.kind_version AS "kind_version?", e.validation AS "validation?"
           FROM core.counters head
           LEFT JOIN LATERAL (
             SELECT p.commit_seq, p.event_id FROM (
               SELECT q.commit_seq, q.event_id,
                      (sum(q.size) OVER (ORDER BY q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT c.commit_seq, c.event_id, coalesce(pg_column_size(s.payload), 0)::bigint AS size
                 FROM core.commit_log c LEFT JOIN core.events s ON s.id = c.event_id
                 WHERE c.commit_seq > $1 AND c.commit_seq <= head.value
                 ORDER BY c.commit_seq LIMIT $2
               ) q
             ) p WHERE p.before < $3::bigint
           ) c ON true
           LEFT JOIN core.events e ON e.id = c.event_id
           WHERE head.name='commit_seq'
           ORDER BY c.commit_seq"#,
        after,
        limit,
        bytes,
    )
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
        // Excised: the content is gone, the position is not.
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

/// A reading taken out of the live set: replaced by a newer reading, hidden or
/// excised.
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

/// What Sinex recorded for one ingress frame.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredFrame {
    pub number: i64,
    pub material_extent: Option<Vec<u8>>,
    pub producer: String,
    pub frame: String,
    pub material_id: Option<Uuid>,
    pub admitted: Vec<Uuid>,
    pub outcome: Value,
    pub refusal: Option<String>,
    pub commit_seq: Option<i64>,
}

/// The recorded frames among `frames` from `producer`.
pub async fn frames(
    conn: &mut PgConnection,
    producer: &str,
    frames: &[String],
) -> Result<Vec<StoredFrame>> {
    Ok(sqlx::query_as!(
        StoredFrame,
        r#"SELECT f.number, f.producer, f.frame, f.material_id, f.admitted, f.outcome, f.refusal, f.commit_seq, x.descriptor AS material_extent
           FROM core.ingress_frames f LEFT JOIN raw.materials m ON m.id=f.material_id
           LEFT JOIN core.blob_extents x ON x.hash=m.blob AND NOT x.deleting
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
    let rows=sqlx::query_as!(StoredFrame,r#"SELECT f.number,f.producer,f.frame,f.material_id,f.admitted,f.outcome,f.refusal,f.commit_seq,x.descriptor AS material_extent
      FROM core.ingress_frames f LEFT JOIN raw.materials m ON m.id=f.material_id
      LEFT JOIN core.blob_extents x ON x.hash=m.blob AND NOT x.deleting
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
    pub extent: Vec<u8>,
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
/// `after`, that Sinex still holds (not excised), in the order their frames
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
                  m.parser, m.parser_version, m.manifest, x.descriptor AS extent
           FROM core.ingress_frames f
           JOIN raw.materials m ON m.id = f.material_id
           JOIN raw.inputs i ON i.id = m.input_id
           JOIN core.blob_extents x ON x.hash=m.blob AND NOT x.deleting
           WHERE f.producer = $1 AND f.refusal IS NULL AND f.number > $2
             AND NOT EXISTS (SELECT 1 FROM core.operations o
                             WHERE o.kind = 'excise' AND o.scope->'materials' ? m.id::text)
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

/// An excised event whose message is still on the confirmed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExcisedOnStream {
    pub commit_seq: Option<i64>,
    pub event_id: Uuid,
    pub stream_seq: i64,
    pub operation_id: Uuid,
}

/// Up to `limit` excised events whose messages are still on the stream.
#[cfg(test)]
pub async fn excised_on_stream(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<ExcisedOnStream>> {
    excised_on_stream_in(conn, "", limit).await
}

pub async fn excised_on_stream_in(
    conn: &mut PgConnection,
    stream_instance: &str,
    limit: i64,
) -> Result<Vec<ExcisedOnStream>> {
    Ok(sqlx::query_as!(
        ExcisedOnStream,
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
) -> Result<()> {
    let outcome = serde_json::json!({"admitted":ids.len(),"replay":operation});
    sqlx::query!("INSERT INTO core.ingress_frames (producer,frame,admitted,outcome,commit_seq,family,write_kind_prefixes) VALUES ($1,$2,$3,$4,$5,$6,$7)",producer,frame,ids,outcome,last,family,write_kind_prefixes).execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
