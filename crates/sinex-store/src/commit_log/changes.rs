//! One ordered feed of reading and lifecycle changes. Pagination may divide a
//! position; only its terminal marker permits a scalar consumer checkpoint.
use super::*;
use sqlx::Connection;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChangeCursor {
    pub position: i64,
    kind: i16,
    change: i64,
    event: Uuid,
}
impl ChangeCursor {
    /// Start strictly after a fully committed scalar consumer position.
    pub fn after(position: i64) -> Self {
        Self {
            position,
            kind: 9,
            change: i64::MAX,
            event: Uuid::from_u128(u128::MAX),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryGap {
    pub event: Uuid,
    pub operation: Option<Uuid>,
    pub reason: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tombstone {
    pub event_id: Uuid,
    pub operation_id: Uuid,
    pub original_commit_seq: i64,
    pub source: String,
    pub event_type: String,
    pub class: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct LifecycleChange {
    pub archive: ArchiveChange,
    pub event_type: String,
    pub class: String,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Change<E = Committed> {
    Event(Box<E>),
    Archive(LifecycleChange),
    Tombstoned(Tombstone),
    PositionComplete,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedChange<E = Committed> {
    pub position: i64,
    pub change: Change<E>,
    cursor: ChangeCursor,
}
impl<E> CommittedChange<E> {
    pub fn cursor(&self) -> ChangeCursor {
        self.cursor
    }
}
#[derive(Clone, Debug)]
pub struct ChangePage<E = Committed> {
    pub entries: Vec<CommittedChange<E>>,
    pub next: Option<ChangeCursor>,
    pub gap: Option<HistoryGap>,
}

impl<E> Default for ChangePage<E> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next: None,
            gap: None,
        }
    }
}

/// The routing fields of a committed event. The publisher loads complete
/// events only for those it sends inline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventHeader {
    pub commit_seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub class: String,
}

trait EventRows: Sized + Send {
    const PAYLOAD: bool;
    fn id(&self) -> Uuid;
    fn load(
        conn: &mut PgConnection,
        ids: &[Uuid],
    ) -> impl std::future::Future<Output = Result<Vec<Self>>> + Send;
}
impl EventRows for Committed {
    const PAYLOAD: bool = true;
    fn id(&self) -> Uuid {
        self.row.id
    }
    async fn load(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        load_events(conn, ids).await
    }
}
impl EventRows for EventHeader {
    const PAYLOAD: bool = false;
    fn id(&self) -> Uuid {
        self.id
    }
    async fn load(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        let rows = sqlx::query_as!(
            EventHeader,
            "SELECT e.commit_seq,e.id,e.event_type,e.class FROM core.events e
            WHERE e.id=ANY($1)
              AND NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)",
            ids
        )
        .fetch_all(conn)
        .await?;
        let headers: BTreeMap<_, _> = rows.into_iter().map(|row| (row.id, row)).collect();
        // The batch lookup may reorder or deduplicate keys; callers keep
        // their requested order and multiplicity, with missing rows omitted.
        Ok(ids
            .iter()
            .filter_map(|id| headers.get(id).cloned())
            .collect())
    }
}

/// A committed event as the confirmed publisher sends it: complete when its
/// class is open, by its routing fields otherwise.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    Open(Box<crate::confirmed::Row>),
    Header(EventHeader),
}
impl StreamEvent {
    pub fn id(&self) -> Uuid {
        match self {
            Self::Open(row) => row.id,
            Self::Header(header) => header.id,
        }
    }
}
impl EventRows for StreamEvent {
    const PAYLOAD: bool = true;
    fn id(&self) -> Uuid {
        StreamEvent::id(self)
    }
    async fn load(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        let headers = EventHeader::load(conn, ids).await?;
        let open: Vec<_> = headers
            .iter()
            .filter(|header| header.class == "open")
            .map(|header| header.id)
            .collect();
        let rows: BTreeMap<_, _> = crate::confirmed::rows(conn, &open)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect();
        Ok(headers
            .into_iter()
            .map(|header| match rows.get(&header.id) {
                Some(row) => Self::Open(Box::new(row.clone())),
                None => Self::Header(header),
            })
            .collect())
    }
}

/// Read the confirmed stream's work after its start position, in the same order and
/// with the same history checks as the full feed. Pages are bounded by stored
/// payload bytes.
pub async fn stream_changes_after_start(
    conn: &mut PgConnection,
    after: ChangeCursor,
    start: &StreamStart,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage<StreamEvent>> {
    changes_page_from(conn, after, limit, bytes, Some(start)).await
}

/// Read bounded transfer work at one database snapshot. No public position
/// advances through a partial position, including a position larger than a page.
pub async fn changes_page(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage> {
    changes_page_from(conn, after, limit, bytes, None).await
}

async fn changes_page_from<E: EventRows>(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
    start: Option<&StreamStart>,
) -> Result<ChangePage<E>> {
    if limit <= 0 || bytes <= 0 {
        return Err(Error::Invalid(
            "commit change page budgets must be positive".into(),
        ));
    }
    let mut tx = conn.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    if let Some(start) = start {
        start.validate(&mut tx).await?;
        if after.position < start.position {
            return Err(Error::Invalid(
                "change cursor precedes the confirmed stream's start position".into(),
            ));
        }
    }
    let page = read_page::<E>(&mut tx, after, limit, bytes, start.is_none()).await?;
    tx.commit().await?;
    Ok(page)
}
async fn read_page<E: EventRows>(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
    require_history: bool,
) -> Result<ChangePage<E>> {
    if require_history && let Some(row)=sqlx::query!("SELECT event_id,operation_id FROM core.event_tombstones WHERE commit_seq IS NULL OR original_commit_seq IS NULL OR class IS NULL ORDER BY event_id LIMIT 1")
        .fetch_optional(&mut *conn).await?{
        return Ok(ChangePage{gap:Some(HistoryGap{event:row.event_id,operation:Some(row.operation_id),
            reason:format!("commit_history_position_unknown: tombstoned event {} lacks admission/lifecycle position or class metadata",row.event_id)}),..Default::default()})
    }
    // History completeness is checked above even when the consumer has reached the head.
    let head = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='commit_seq'")
        .fetch_one(&mut *conn)
        .await?;
    if after.position == head && after.kind == 9 {
        return Ok(ChangePage::default());
    }
    let rows = sqlx::query_file!(
        "src/commit_log/changes_page.sql",
        after.position,
        after.kind,
        after.change,
        after.event,
        limit,
        bytes,
        head,
        E::PAYLOAD
    )
    .fetch_all(&mut *conn)
    .await?;
    let ids: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 0)
        .map(|row| row.event_id)
        .collect();
    let mut events: BTreeMap<_, _> = E::load(conn, &ids)
        .await?
        .into_iter()
        .map(|event| (event.id(), event))
        .collect();
    let archive_ids: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 1)
        .map(|row| row.change_seq)
        .collect();
    let archive_events: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 1)
        .map(|row| row.event_id)
        .collect();
    let archived_rows = sqlx::query_file!(
        "src/commit_log/archive_headers.sql",
        &archive_ids,
        &archive_events
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut archived = BTreeMap::new();
    for row in archived_rows {
        let (Some(event_type), Some(class)) = (row.event_type, row.class) else {
            return Ok(ChangePage {
                gap: Some(HistoryGap {
                    event: row.event_id,
                    operation: row.operation_id,
                    reason: format!(
                        "commit_history_metadata_unknown: archive change {} has no kind/class",
                        row.change_seq
                    ),
                }),
                ..Default::default()
            });
        };
        archived.insert(
            row.change_seq,
            LifecycleChange {
                event_type,
                class,
                archive: ArchiveChange {
                    change_seq: row.change_seq,
                    event_id: row.event_id,
                    commit_seq: row.commit_seq,
                    archived: row.archived,
                    reason: row.reason,
                    replaced_by: row.replaced_by,
                    operation_id: row.operation_id,
                    recorded_at: row.recorded_at,
                },
            },
        );
    }
    let erased_ids: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 2)
        .map(|row| row.event_id)
        .collect();
    let mut erased:BTreeMap<_,_>=sqlx::query_as!(Tombstone, r#"SELECT event_id,operation_id,original_commit_seq AS "original_commit_seq!",source,event_type,class AS "class!" FROM core.event_tombstones WHERE event_id=ANY($1)"#,&erased_ids)
        .fetch_all(&mut *conn).await?.into_iter().map(|row|(row.event_id,row)).collect();
    let mut page = ChangePage::default();
    for row in rows {
        let cursor = ChangeCursor {
            position: row.position,
            kind: row.kind,
            change: row.change_seq,
            event: row.event_id,
        };
        let missing = || {
            Error::Invalid(format!(
                "commit_history_gap: missing change at position {} for event {}",
                row.position, row.event_id
            ))
        };
        let change = match row.kind {
            0 => Change::Event(Box::new(events.remove(&row.event_id).ok_or_else(missing)?)),
            1 => Change::Archive(archived.remove(&row.change_seq).ok_or_else(missing)?),
            2 => Change::Tombstoned(erased.remove(&row.event_id).ok_or_else(missing)?),
            9 => Change::PositionComplete,
            _ => return Err(Error::Invalid("unknown commit change category".into())),
        };
        page.entries.push(CommittedChange {
            position: row.position,
            change,
            cursor,
        });
        page.next = Some(cursor);
    }
    Ok(page)
}

async fn load_events(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Committed>> {
    let rows = sqlx::query_as!(EventRow, r#"SELECT e.id AS "id!", e.event_type AS "event_type!", e.kind_version AS "kind_version!", e.validation AS "validation!", e.source AS "source!", e.run_id AS "run_id!", e.material_id, e.record_offset, e.output_index AS "output_index!", e.parents, e.ts_orig, e.ts_quality AS "ts_quality!", e.ts_coided, e.ts_persisted AS "ts_persisted!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!", e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
      FROM unnest($1::uuid[]) selected(id) CROSS JOIN LATERAL (
        SELECT e.* FROM core.events e WHERE e.id=selected.id OFFSET 0) e
      WHERE NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)"#, ids)
      .fetch_all(&mut *conn).await?;
    let mut events: Vec<_> = rows
        .into_iter()
        .map(|row| Committed {
            row,
            search: sinex_model::kinds::Search::None,
        })
        .collect();
    add_declarations(conn, &mut events).await?;
    Ok(events)
}

#[cfg(test)]
mod tests;
