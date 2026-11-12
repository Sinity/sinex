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
    /// Start strictly after a fully committed scalar consumer watermark.
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

/// Routing fields needed by the confirmed publisher. Payloads are fetched by
/// authorized readers, not loaded again while publishing their references.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventReference {
    pub commit_seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub class: String,
}

trait EventProjection: Sized + Send {
    const PAYLOAD: bool;
    fn id(&self) -> Uuid;
    fn load(
        conn: &mut PgConnection,
        ids: &[Uuid],
    ) -> impl std::future::Future<Output = Result<Vec<Self>>> + Send;
}
impl EventProjection for Committed {
    const PAYLOAD: bool = true;
    fn id(&self) -> Uuid {
        self.id
    }
    async fn load(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        load_events(conn, ids).await
    }
}
impl EventProjection for EventReference {
    const PAYLOAD: bool = false;
    fn id(&self) -> Uuid {
        self.id
    }
    async fn load(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Self>> {
        let rows = sqlx::query!(
            "SELECT e.commit_seq,e.id,e.event_type,e.class FROM unnest($1::uuid[]) selected(id)
            CROSS JOIN LATERAL (SELECT e.commit_seq,e.id,e.event_type,e.class FROM core.events e
              WHERE e.id=selected.id OFFSET 0) e
            WHERE NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)",
            ids
        )
        .fetch_all(conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self {
                commit_seq: row.commit_seq,
                id: row.id,
                event_type: row.event_type,
                class: row.class,
            })
            .collect())
    }
}

/// Read payload-free publication work with the same ordering and authority as
/// the full canonical feed.
pub async fn references_after_origin(
    conn: &mut PgConnection,
    after: ChangeCursor,
    origin: &HistoryOrigin,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage<EventReference>> {
    changes_page_from(conn, after, limit, bytes, Some(origin)).await
}

/// Read bounded transfer work at one database snapshot. No public watermark
/// advances through a partial position, including a position larger than a page.
pub async fn changes_page(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage> {
    changes_page_from(conn, after, limit, bytes, None).await
}

/// Post-origin deltas are valid only after a complete certified projection.
/// The schema excludes new incomplete tombstones; predecessor rows are closed.
pub async fn changes_page_after_origin(
    conn: &mut PgConnection,
    after: ChangeCursor,
    origin: &HistoryOrigin,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage> {
    changes_page_from(conn, after, limit, bytes, Some(origin)).await
}
async fn changes_page_from<E: EventProjection>(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
    origin: Option<&HistoryOrigin>,
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
    if let Some(origin) = origin {
        origin.validate(&mut tx).await?;
        if after.position < origin.position {
            return Err(Error::Invalid(
                "change cursor precedes confirmed stream origin".into(),
            ));
        }
    }
    let page = read_page::<E>(&mut tx, after, limit, bytes, origin.is_none()).await?;
    tx.commit().await?;
    Ok(page)
}
async fn read_page<E: EventProjection>(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
    require_history: bool,
) -> Result<ChangePage<E>> {
    if require_history && let Some(row)=sqlx::query!("SELECT event_id,operation_id FROM core.event_tombstones WHERE commit_seq IS NULL OR original_commit_seq IS NULL OR class IS NULL ORDER BY event_id LIMIT 1")
        .fetch_optional(&mut *conn).await?{
        return Ok(ChangePage{gap:Some(HistoryGap{event:row.event_id,operation:Some(row.operation_id),
            reason:format!("commit_history_position_unknown: tombstoned event {} lacks exact admission/lifecycle position or class metadata",row.event_id)}),..Default::default()})
    }
    // Authority is checked above even when the consumer has reached the head.
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
    let archived_rows=sqlx::query!(r#"SELECT a.change_seq,a.event_id,a.commit_seq,a.archived,a.reason,a.replaced_by,a.operation_id,a.recorded_at,
        coalesce(e.event_type,t.event_type) AS event_type,coalesce(e.class,t.class) AS class
        FROM audit.archive_changes a LEFT JOIN LATERAL (SELECT e.event_type,e.class FROM core.events e WHERE e.id=a.event_id OFFSET 0) e ON true
        LEFT JOIN core.event_tombstones t ON t.event_id=a.event_id WHERE a.change_seq=ANY($1)"#,&archive_ids)
        .fetch_all(&mut *conn).await?;
    let mut archived = BTreeMap::new();
    for row in archived_rows {
        let (Some(event_type), Some(class)) = (row.event_type, row.class) else {
            return Ok(ChangePage {
                gap: Some(HistoryGap {
                    event: row.event_id,
                    operation: row.operation_id,
                    reason: format!(
                        "commit_history_metadata_unknown: archive change {} has no exact kind/class",
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
    let mut erased:BTreeMap<_,_>=sqlx::query!(r#"SELECT event_id,operation_id,original_commit_seq AS "original_commit_seq!",source,event_type,class AS "class!" FROM core.event_tombstones WHERE event_id=ANY($1)"#,&erased_ids)
        .fetch_all(&mut *conn).await?.into_iter().map(|row|(row.event_id,Tombstone{event_id:row.event_id,operation_id:row.operation_id,original_commit_seq:row.original_commit_seq,source:row.source,event_type:row.event_type,class:row.class})).collect();
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
    let rows=sqlx::query!(r#"SELECT e.commit_seq,e.id,e.event_type,e.kind_version,e.validation,e.source,e.run_id,e.material_id,e.anchor,
      e.output_index,e.parents,e.ts_orig,e.ts_quality,e.ts_coided,e.ts_persisted AS "ts_persisted!",e.payload,
      e.equivalence_key,e.reading_order,e.record_hash,e.record_state,e.class FROM unnest($1::uuid[]) selected(id) CROSS JOIN LATERAL (
        SELECT e.* FROM core.events e WHERE e.id=selected.id OFFSET 0) e WHERE true
        AND NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)"#,ids)
      .fetch_all(&mut *conn).await?;
    let mut events: Vec<_> = rows
        .into_iter()
        .map(|row| Committed {
            commit_seq: row.commit_seq,
            id: row.id,
            event_type: row.event_type,
            kind_version: row.kind_version,
            validation: row.validation,
            search: sinex_model::kinds::Search::None,
            replace_rule: None,
            source: row.source,
            run_id: row.run_id,
            material_id: row.material_id,
            anchor: row.anchor,
            output_index: row.output_index,
            parents: row.parents,
            ts_orig: row.ts_orig,
            ts_quality: row.ts_quality,
            ts_coided: row.ts_coided,
            ts_persisted: row.ts_persisted,
            payload: row.payload,
            equivalence_key: row.equivalence_key,
            reading_order: row.reading_order,
            record_hash: row.record_hash,
            record_state: row.record_state,
            class: row.class,
        })
        .collect();
    let references: Vec<_> = events
        .iter()
        .map(|event| sinex_model::KindRef {
            name: event.event_type.clone(),
            version: event.kind_version as u64,
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let declarations = sinex_schema::kinds::load(conn, &references).await?;
    for event in &mut events {
        let key = sinex_model::KindRef {
            name: event.event_type.clone(),
            version: event.kind_version as u64,
        };
        let declaration = declarations
            .get(&key)
            .ok_or_else(|| Error::Invalid(format!("missing stored declaration {key:?}")))?;
        event.replace_rule = declaration.replace_rule.map(|rule| rule.as_db().into());
        event.search = declaration.search;
    }
    Ok(events)
}
