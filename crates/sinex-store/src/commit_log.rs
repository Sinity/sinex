//! Reading committed events in commit order: the commit-log reader and the
//! confirmed-stream publisher page through `core.commit_log` from a saved
//! position. Archive notices (a reading replaced, hidden or restored) use the
//! existing archival notice sequence; `audit.archive_changes` keeps their
//! committed history.
//!
//! An excision removes events and their commit-log rows, leaving gaps in the
//! commit positions; pages read past gaps, and an event row missing behind a
//! commit-log row is skipped, so consumers never see excised content. The
//! publisher erases the stream messages of events excised after they were
//! published ([`excised_on_stream`]).
use crate::{Error, Result};
use serde_json::Value;
use sinex_model::{
    Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation, RecordState,
    TimeQuality, Timestamp, TsOrig, event_types,
};
use sqlx::PgConnection;
use time::OffsetDateTime;
use uuid::Uuid;

/// One committed event as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct Committed {
    pub commit_seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub source: String,
    pub run_id: Uuid,
    pub material_id: Option<Uuid>,
    pub anchor: Option<i64>,
    pub ordinal: i16,
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

/// Up to `limit` commit-log entries after `after`, in commit order.
pub async fn after(conn: &mut PgConnection, after: i64, limit: i64) -> Result<Page> {
    if limit <= 0 {
        return Err(Error::Invalid("commit page limit must be positive".into()));
    }
    let rows = sqlx::query!(
        r#"SELECT head.value AS "head!", c.commit_seq AS "commit_seq?", e.id AS "id?", e.event_type AS "event_type?",
             e.source AS "source?", e.run_id AS "run_id?", e.material_id, e.anchor,
             e.ordinal AS "ordinal?", e.parents, e.ts_orig, e.ts_quality AS "ts_quality?",
             e.ts_coided, e.ts_persisted AS "ts_persisted?", e.payload AS "payload?",
             e.equivalence_key, e.reading_order AS "reading_order?",
             e.record_hash AS "record_hash?", e.record_state AS "record_state?", e.class AS "class?"
           FROM core.counters head
           LEFT JOIN LATERAL (
             SELECT c.commit_seq,c.event_id FROM core.commit_log c
             WHERE c.commit_seq > $1 AND c.commit_seq <= head.value
             ORDER BY c.commit_seq LIMIT $2
           ) c ON true
           LEFT JOIN core.events e ON e.id = c.event_id
           WHERE head.name='commit_seq'
           ORDER BY c.commit_seq"#,
        after,
        limit,
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
            commit_seq: position,
            id,
            event_type,
            source,
            run_id,
            material_id: row.material_id,
            anchor: row.anchor,
            ordinal: row.ordinal.unwrap_or(0),
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
    Ok(page)
}

impl Committed {
    /// The event as the model types describe it, for derivations. `None` when
    /// this build does not register the event's type.
    pub fn to_event(&self) -> Result<Option<Event>> {
        let Some(kind) = event_types::find(&self.event_type) else {
            return Ok(None);
        };
        let origin = match (&self.material_id, self.anchor, &self.parents) {
            (Some(material), Some(anchor), _) => Origin::Record {
                location: RecordLocation {
                    material: MaterialId(*material),
                    anchor: u64::try_from(anchor)
                        .map_err(|_| Error::Invalid(format!("negative anchor on {}", self.id)))?,
                },
                ordinal: u16::try_from(self.ordinal)
                    .map_err(|_| Error::Invalid(format!("negative ordinal on {}", self.id)))?,
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
        Ok(Some(Event {
            id: EventId(self.id),
            event_type: kind.event_type,
            source: self.source.clone(),
            origin,
            ts_orig: TsOrig::maybe(self.ts_orig.map(Timestamp), quality),
            payload: self.payload.clone(),
            equivalence_key: self
                .equivalence_key
                .clone()
                .map(sinex_model::EquivalenceKey),
            replace: kind.replace,
            reading_order: ReadingOrder(self.reading_order),
            record_hash,
            record_state: from_db::<RecordState>(&self.record_state)?,
            class: from_db::<Class>(&self.class)?,
            support: None,
            cites: Vec::new(),
        }))
    }
}

fn from_db<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_value(Value::String(text.to_owned()))
        .map_err(|e| Error::Invalid(format!("stored value {text:?}: {e}")))
}

/// A reading taken out of the live set: replaced by a newer reading, hidden or
/// excised.
#[derive(Clone, Debug, PartialEq)]
pub struct Notice {
    pub notice_seq: i64,
    pub event_id: Uuid,
    /// The writer position at which availability changed.
    pub commit_seq: i64,
    pub archived: bool,
    pub reason: String,
    pub replaced_by: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub recorded_at: OffsetDateTime,
}

/// Up to `limit` archive notices after `after`, in the order they committed.
pub async fn notices_after(conn: &mut PgConnection, after: i64, limit: i64) -> Result<Vec<Notice>> {
    Ok(sqlx::query_as!(
        Notice,
        r#"SELECT notice_seq, event_id, commit_seq, archived, reason, replaced_by, operation_id, recorded_at
           FROM audit.archive_changes WHERE notice_seq > $1 ORDER BY notice_seq LIMIT $2"#,
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

/// A non-derivation consumer's saved position: (commit_seq, notice_seq).
pub async fn consumer_position(conn: &mut PgConnection, consumer: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query!(
        "SELECT commit_seq, notice_seq FROM core.consumer_positions WHERE consumer = $1",
        consumer
    )
    .fetch_optional(&mut *conn)
    .await?
    .map_or((0, 0), |r| (r.commit_seq, r.notice_seq)))
}

/// Save a consumer's position. Positions only move forward.
pub async fn set_consumer_position(
    conn: &mut PgConnection,
    consumer: &str,
    commit_seq: i64,
    notice_seq: i64,
) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO core.consumer_positions (consumer, commit_seq, notice_seq) VALUES ($1, $2, $3)
           ON CONFLICT (consumer) DO UPDATE SET
             commit_seq = greatest(core.consumer_positions.commit_seq, EXCLUDED.commit_seq),
             notice_seq = greatest(core.consumer_positions.notice_seq, EXCLUDED.notice_seq),
             updated_at = now()"#,
        consumer,
        commit_seq,
        notice_seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// What Sinex recorded for one ingress frame.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredFrame {
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
        r#"SELECT producer, frame, material_id, admitted, outcome, refusal, commit_seq
           FROM core.ingress_frames WHERE producer = $1 AND frame = ANY($2)
           ORDER BY received_at, frame"#,
        producer,
        frames,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Record where published events sit in the confirmed stream.
pub async fn record_published(conn: &mut PgConnection, published: &[(Uuid, i64)]) -> Result<()> {
    if published.is_empty() {
        return Ok(());
    }
    let (ids, seqs): (Vec<Uuid>, Vec<i64>) = published.iter().copied().unzip();
    sqlx::query!(
        r#"INSERT INTO core.published (event_id, stream_seq)
           SELECT * FROM unnest($1::uuid[], $2::bigint[])
           ON CONFLICT (event_id) DO NOTHING"#,
        &ids,
        &seqs,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// An excised event whose message is still on the confirmed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExcisedOnStream {
    pub event_id: Uuid,
    pub stream_seq: i64,
    pub operation_id: Uuid,
}

/// Up to `limit` excised events whose messages are still on the stream.
pub async fn excised_on_stream(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<ExcisedOnStream>> {
    Ok(sqlx::query_as!(
        ExcisedOnStream,
        r#"SELECT p.event_id, p.stream_seq, t.operation_id
           FROM core.published p JOIN core.event_tombstones t ON t.event_id = p.event_id
           ORDER BY p.stream_seq LIMIT $1"#,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Forget events whose messages have been erased from the stream.
pub async fn forget_published(conn: &mut PgConnection, ids: &[Uuid]) -> Result<()> {
    sqlx::query!("DELETE FROM core.published WHERE event_id = ANY($1)", ids)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
