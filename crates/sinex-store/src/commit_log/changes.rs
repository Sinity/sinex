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
pub enum Change {
    Event(Box<Committed>),
    Archive(LifecycleChange),
    Excised(Tombstone),
    PositionComplete,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedChange {
    pub position: i64,
    pub change: Change,
    cursor: ChangeCursor,
}
impl CommittedChange {
    pub fn cursor(&self) -> ChangeCursor {
        self.cursor
    }
}
#[derive(Clone, Debug, Default)]
pub struct ChangePage {
    pub entries: Vec<CommittedChange>,
    pub next: Option<ChangeCursor>,
    pub gap: Option<HistoryGap>,
}

/// Read bounded transfer work at one database snapshot. No public watermark
/// advances through a partial position, including a position larger than a page.
pub async fn changes_page(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage> {
    if limit <= 0 || bytes <= 0 {
        return Err(Error::Invalid(
            "commit change page budgets must be positive".into(),
        ));
    }
    let mut tx = conn.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let page = read_page(&mut tx, after, limit, bytes).await?;
    tx.commit().await?;
    Ok(page)
}
async fn read_page(
    conn: &mut PgConnection,
    after: ChangeCursor,
    limit: i64,
    bytes: i64,
) -> Result<ChangePage> {
    if let Some(row)=sqlx::query!("SELECT event_id,operation_id FROM core.event_tombstones WHERE commit_seq IS NULL OR original_commit_seq IS NULL OR class IS NULL ORDER BY event_id LIMIT 1")
        .fetch_optional(&mut *conn).await?{
        return Ok(ChangePage{gap:Some(HistoryGap{event:row.event_id,operation:Some(row.operation_id),
            reason:format!("commit_history_position_unknown: tombstoned event {} lacks exact admission/lifecycle position or class metadata",row.event_id)}),..Default::default()})
    }
    let rows=sqlx::query!(r#"WITH head AS MATERIALIZED (
        SELECT value FROM core.counters WHERE name='commit_seq'
      ), positions AS NOT MATERIALIZED (
        SELECT commit_seq FROM core.commit_log WHERE commit_seq >= $1
        UNION SELECT commit_seq FROM core.events WHERE commit_seq >= $1
        UNION SELECT commit_seq FROM audit.archive_changes WHERE commit_seq >= $1
        UNION SELECT commit_seq FROM core.event_tombstones WHERE commit_seq >= $1
        UNION SELECT original_commit_seq FROM core.event_tombstones WHERE original_commit_seq >= $1
        UNION SELECT value FROM head WHERE value >= $1 AND value>0
      ), changes AS NOT MATERIALIZED (
        SELECT e.commit_seq,0::smallint AS kind,0::bigint AS change_seq,e.id AS event_id,
            (pg_column_size(e)::bigint+octet_length(e.payload::text)::bigint+
             coalesce(cardinality(e.parents),0)::bigint*40+1024) AS size
            FROM core.events e WHERE e.commit_seq >= $1
            AND NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)
        UNION ALL SELECT a.commit_seq,1::smallint,a.change_seq,a.event_id,256::bigint
            FROM audit.archive_changes a WHERE a.commit_seq >= $1
        UNION ALL SELECT t.commit_seq,2::smallint,0::bigint,t.event_id,256::bigint
            FROM core.event_tombstones t WHERE t.commit_seq >= $1
        UNION ALL SELECT p.commit_seq,9::smallint,0::bigint,'00000000-0000-0000-0000-000000000000'::uuid,64::bigint
            FROM positions p
      ), selected AS MATERIALIZED (
        SELECT c.* FROM changes c,head h WHERE c.commit_seq <= h.value
          AND (c.commit_seq,c.kind,c.change_seq,c.event_id)>($1,$2,$3,$4)
        ORDER BY c.commit_seq,c.kind,c.change_seq,c.event_id LIMIT $5
      ), bounded AS (
        SELECT s.*,sum(size) OVER(ORDER BY commit_seq,kind,change_seq,event_id)-size AS before FROM selected s
      ) SELECT commit_seq AS "position!",kind AS "kind!",change_seq AS "change_seq!",event_id AS "event_id!"
        FROM bounded WHERE before < $6::bigint ORDER BY commit_seq,kind,change_seq,event_id"#,
        after.position,after.kind,after.change,after.event,limit,bytes).fetch_all(&mut *conn).await?;
    let ids: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 0)
        .map(|row| row.event_id)
        .collect();
    let mut events: BTreeMap<_, _> = load_events(conn, &ids)
        .await?
        .into_iter()
        .map(|event| (event.id, event))
        .collect();
    let archive_ids: Vec<_> = rows
        .iter()
        .filter(|row| row.kind == 1)
        .map(|row| row.change_seq)
        .collect();
    let archived_rows=sqlx::query!(r#"SELECT a.change_seq,a.event_id,a.commit_seq,a.archived,a.reason,a.replaced_by,a.operation_id,a.recorded_at,
        coalesce(e.event_type,t.event_type) AS event_type,coalesce(e.class,t.class) AS class
        FROM audit.archive_changes a LEFT JOIN core.events e ON e.id=a.event_id
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
            2 => Change::Excised(erased.remove(&row.event_id).ok_or_else(missing)?),
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
      e.equivalence_key,e.reading_order,e.record_hash,e.record_state,e.class FROM core.events e WHERE e.id=ANY($1)
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
