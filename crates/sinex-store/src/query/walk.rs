//! Citations, `show`, provenance walks and recall's per-source account. These
//! are fixed queries, checked at compile time.
use crate::{Error, Result};
use serde_json::{Value as Json, json};
use sinex_model::query::{Reference, Stage, Walk};
use sqlx::PgConnection;
use std::collections::{BTreeMap, btree_map::Entry};
use uuid::Uuid;

/// Event citations keep live, archived and tombstoned identities. Record and
/// key citations resolve the live reading.
pub async fn resolve(conn: &mut PgConnection, reference: &Reference) -> Result<Uuid> {
    match reference {
        Reference::Window { .. } => Err(Error::Invalid(
            "a time window is a recall query, not one event".into(),
        )),
        Reference::At { at, event_type } => resolve_at(conn, *at, event_type).await,
        Reference::Event(id) => {
            let found = sqlx::query_scalar!(r#"SELECT id AS "id!" FROM core.visible_events WHERE id = $1 UNION SELECT event_id FROM core.event_tombstones WHERE event_id = $1"#, id)
                .fetch_optional(&mut *conn)
                .await?;
            found.ok_or_else(|| Error::Invalid(format!("no event {id}")))
        }
        Reference::Record {
            material,
            offset,
            event_type,
            output,
        } => {
            let output = output
                .map(i64::try_from)
                .transpose()
                .map_err(|_| Error::Invalid("output index beyond i64".into()))?;
            let live = sqlx::query!(
                r#"SELECT e.id AS "event_id!", e.event_type AS "event_type!" FROM core.visible_events e
                   WHERE e.material_id = $1 AND e.anchor = $2
                   AND ($3::text IS NULL OR e.event_type = $3)
                   AND ($4::bigint IS NULL OR e.output_index = $4)
                   AND (EXISTS (SELECT 1 FROM core.live_locations c WHERE c.event_id = e.id)
                        OR EXISTS (SELECT 1 FROM core.live_equivalence q WHERE q.event_id = e.id))"#,
                material,
                offset,
                event_type.as_deref(),
                output
            )
            .fetch_all(&mut *conn)
            .await?;
            match live.as_slice() {
                [one] => Ok(one.event_id),
                [] => {
                    let archived = sqlx::query_scalar!(
                        r#"SELECT e.id AS "id!" FROM core.visible_events e JOIN audit.archived a ON a.event_id = e.id
                           WHERE e.material_id = $1 AND e.anchor = $2
                           AND ($3::text IS NULL OR e.event_type = $3)
                           AND ($4::bigint IS NULL OR e.output_index = $4)
                           ORDER BY a.archived_seq DESC"#,
                        material,
                        offset,
                        event_type.as_deref(),
                        output
                    )
                    .fetch_all(&mut *conn)
                    .await?;
                    Err(Error::Invalid(if archived.is_empty() {
                        "no reading of this record exists".to_string()
                    } else {
                        let ids: Vec<String> = archived.iter().map(Uuid::to_string).collect();
                        format!(
                            "no live reading of this record; archived: {}",
                            ids.join(", ")
                        )
                    }))
                }
                many => {
                    let kinds: Vec<String> = many.iter().map(|r| r.event_type.clone()).collect();
                    Err(Error::Invalid(format!(
                        "this record has several live outputs ({}); name the event type",
                        kinds.join(", ")
                    )))
                }
            }
        }
        Reference::Key {
            source,
            event_type,
            key,
        } => {
            let found = sqlx::query_scalar!(
                r#"SELECT e.id AS "id!" FROM core.visible_events e
                   WHERE e.source=$1 AND e.event_type=$2 AND e.equivalence_key=$3
                     AND EXISTS (SELECT 1 FROM core.live_equivalence live WHERE live.event_id=e.id) LIMIT 2"#,
                source,
                event_type,
                key
            )
            .fetch_all(&mut *conn)
            .await?;
            match found.as_slice() {
                [id] => Ok(*id),
                [] => Err(Error::Invalid(format!(
                    "no live {event_type} of {source} with key {key}"
                ))),
                _ => Err(Error::Invalid(
                    "several live readings share the visible key; use an exact event citation"
                        .into(),
                )),
            }
        }
    }
}

/// One event in full: the reading, whether it is live, where it came from,
/// and its parents.
pub async fn show(conn: &mut PgConnection, reference: &Reference) -> Result<Json> {
    let id = resolve(conn, reference).await?;
    let row = sqlx::query_scalar!(
        r#"SELECT jsonb_build_object(
             'id', e.id, 'event_type', e.event_type, 'kind_version', e.kind_version, 'validation', e.validation, 'source', e.source,
             'when', e.ts_orig, 'time_quality', e.ts_quality, 'seen', e.ts_coided,
             'written', e.ts_persisted, 'commit_seq', e.commit_seq,
             'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
             'archived', a.archived_at, 'archive_reason', a.reason,
             'record_state', e.record_state, 'class', e.class, 'key', e.equivalence_key,
             'reading_order',e.reading_order,'record_hash',encode(e.record_hash,'hex'),
             'material', e.material_id, 'offset', e.anchor, 'output', e.output_index,
             'parents', to_jsonb(e.parents), 'run', e.run_id, 'producer', r.name, 'host', r.host,
             'parser', m.parser, 'parser_version', m.parser_version, 'blob', m.blob,
             'citation', CASE WHEN e.material_id IS NOT NULL
               THEN 'sinex:record/' || e.material_id || '/' || e.anchor || '/' || e.event_type || '/' || e.output_index
               ELSE 'sinex:ev/' || e.id END,
             'payload', e.payload) AS "row!"
           FROM core.visible_events e
           LEFT JOIN audit.archived a ON a.event_id = e.id
           LEFT JOIN core.runs r ON r.id = e.run_id
           LEFT JOIN raw.materials m ON m.id = e.material_id
           WHERE e.id = $1"#,
        id
    )
    .fetch_optional(&mut *conn)
    .await?;
    if let Some(row) = row {
        return Ok(row);
    }
    Ok(sqlx::query_scalar!(
        r#"SELECT jsonb_build_object('id',event_id,'source',source,'event_type',event_type,
             'when',ts_orig,'state','tombstoned','class',class,'payload',NULL,
             'tombstone_operation',operation_id,'tombstoned',removed_at,
             'citation','sinex:ev/'||event_id,
             'unavailable','tombstoned reading has no payload') AS "row!"
           FROM core.event_tombstones WHERE event_id=$1"#,
        id,
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// A provenance walk's rows.
pub(crate) async fn walk(
    conn: &mut PgConnection,
    walk: Walk,
    reference: &Reference,
) -> Result<Vec<Json>> {
    let id = resolve(conn, reference).await?;
    let depths = lineage(conn, id, walk).await?;
    let selected: Vec<Json> = depths
        .into_iter()
        .map(|(id, depth)| json!({"id":id,"depth":depth}))
        .collect();
    // The visible view owns payload masking. Edges and tombstone skeletons
    // retain identity after excision, without reconstructing cleared content.
    let rows = sqlx::query_scalar!(
        r#"WITH selected AS (
             SELECT * FROM jsonb_to_recordset($1::jsonb) AS r(id uuid, depth numeric))
           SELECT jsonb_build_object(
             'depth', f.depth, 'id', f.id,
             'event_type', coalesce(e.event_type,t.event_type),
             'kind_version', e.kind_version, 'validation', e.validation,
             'source', coalesce(e.source,t.source), 'when', coalesce(e.ts_orig,t.ts_orig),
             'state', CASE WHEN e.id IS NULL AND t.event_id IS NOT NULL THEN 'tombstoned'
               WHEN e.id IS NULL THEN 'unavailable'
               WHEN a.event_id IS NOT NULL THEN 'archived' ELSE 'live' END,
             'unavailable', CASE WHEN e.id IS NULL AND t.event_id IS NULL THEN 'event record missing'
               WHEN t.event_id IS NOT NULL AND e.id IS NULL THEN 'tombstoned reading has no payload' END,
             'withheld', e.payload->'withheld', 'class', coalesce(e.class,t.class),
             'origin', CASE WHEN e.id IS NULL THEN NULL
               WHEN e.material_id IS NULL THEN 'derived' ELSE 'material' END,
             'material', e.material_id, 'offset', e.anchor, 'output', e.output_index,
             'parents', coalesce(to_jsonb(e.parents),
               (SELECT jsonb_agg(c.parent ORDER BY c.parent) FROM core.event_children c WHERE c.child=f.id)),
             'bytes', CASE WHEN e.material_id IS NULL THEN NULL
               WHEN m.id IS NULL THEN 'unavailable: material record missing' ELSE m.blob END,
             'parser', m.parser, 'parser_version', m.parser_version,
             'run', e.run_id, 'producer', r.name,
             'tombstone_operation', t.operation_id, 'tombstoned', t.removed_at,
             'payload', e.payload) AS "row!"
           FROM selected f
           LEFT JOIN core.visible_events e ON e.id=f.id
           LEFT JOIN core.event_tombstones t ON t.event_id=f.id
           LEFT JOIN audit.archived a ON a.event_id=e.id
           LEFT JOIN raw.materials m ON m.id=e.material_id
           LEFT JOIN core.runs r ON r.id=e.run_id
           ORDER BY f.depth,
             CASE WHEN $2 THEN coalesce(e.ts_orig,t.ts_orig) END NULLS LAST, f.id"#,
        Json::Array(selected),
        !matches!(walk, Walk::Trace),
    ).fetch_all(&mut *conn).await?;
    Ok(rows)
}

/// Breadth-first frontiers make the first visit the shortest distance. Keeping
/// only visited IDs avoids both cycles and an exponential list of branch paths.
async fn lineage(
    conn: &mut PgConnection,
    root: Uuid,
    direction: Walk,
) -> Result<BTreeMap<Uuid, u64>> {
    let mut visited = BTreeMap::from([(root, 0_u64)]);
    let mut frontier = vec![root];
    let mut depth = 0_u64;
    while !frontier.is_empty() {
        let adjacent = match direction {
            Walk::Descendants => sqlx::query_scalar!(
                "SELECT DISTINCT child FROM core.event_children WHERE parent=ANY($1) ORDER BY child",
                &frontier,
            ).fetch_all(&mut *conn).await?,
            Walk::Trace | Walk::Parents => sqlx::query_scalar!(
                "SELECT DISTINCT parent FROM core.event_children WHERE child=ANY($1) ORDER BY parent",
                &frontier,
            ).fetch_all(&mut *conn).await?,
        };
        depth = depth
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("lineage depth exceeds u64".into()))?;
        if matches!(direction, Walk::Parents) {
            return Ok(adjacent.into_iter().map(|id| (id, depth)).collect());
        }
        frontier.clear();
        for id in adjacent {
            if let Entry::Vacant(entry) = visited.entry(id) {
                entry.insert(depth);
                frontier.push(id);
            }
        }
    }
    if matches!(direction, Walk::Descendants) {
        visited.remove(&root);
    }
    Ok(visited)
}

/// What each source recorded in a recall window, and which sources had no
/// data there: "no data", never "nothing happened".
pub(super) fn recall_rows(rows: &sinex_model::query::Query) -> sinex_model::query::Query {
    let mut selector = rows.clone();
    selector.stages = rows
        .stages
        .iter()
        .take_while(|stage| !matches!(stage, Stage::Group(_)))
        .filter(|stage| !matches!(stage, Stage::Fields(_)))
        .cloned()
        .collect();
    selector
}

pub(crate) async fn recall_summary(
    conn: &mut PgConnection,
    from: jiff::Timestamp,
    to: jiff::Timestamp,
    seq: i64,
    inclusive_end: bool,
    rows: &sinex_model::query::Query,
    ctx: &sinex_model::query::Context,
) -> Result<Json> {
    use sinex_model::query::{AggFn, Aggregate, FieldRef, Group, Operand, Table};
    let field = |name| FieldRef::Field(Table::Events.field(name).expect("event field declared"));
    let selector = recall_rows(rows);
    let mut summary = selector.clone();
    summary.stages.push(Stage::Group(Group {
        keys: vec![Operand::Field(field("source"))],
        aggregates: vec![
            Aggregate {
                function: AggFn::Count,
                field: None,
            },
            Aggregate {
                function: AggFn::Min,
                field: Some(field("when")),
            },
            Aggregate {
                function: AggFn::Max,
                field: Some(field("when")),
            },
        ],
        fill: None,
    }));
    let (sql, values) = {
        let compiled = super::compile_query(conn, ctx, seq, &summary).await?;
        let mut rows = super::SqlFragment::text(
            "SELECT jsonb_build_object('source',q.source,'events',q.count,'first',q.min_when,'last',q.max_when) FROM (",
        );
        rows.append(compiled.answer).push(") q");
        rows.render()
    };
    let by_source: Vec<Json> = super::sql::bind_scalar(sqlx::query_scalar(&sql), values)
        .fetch_all(&mut *conn)
        .await?;
    let latest_by_source = {
        let (sql, values) = {
            let compiled = super::compile_query(conn, ctx, seq, &selector).await?;
            let mut rows = super::SqlFragment::text(format!(
                "SELECT DISTINCT ON (q.source) {} FROM (",
                super::json_object(&compiled.columns)
            ));
            rows.append(compiled.answer)
                .push(") q ORDER BY q.source, q.\"when\" DESC NULLS LAST, q.id DESC");
            rows.render()
        };
        super::sql::bind_scalar(sqlx::query_scalar::<_, Json>(&sql), values)
            .fetch_all(&mut *conn)
            .await?
    };
    let bookkeeping_types: Vec<String> = super::recall::bookkeeping_types()
        .iter()
        .map(|kind| (*kind).into())
        .collect();
    let known = sqlx::query_scalar!(
        r#"SELECT c.source AS "name!" FROM core.source_counts c
           WHERE EXISTS (SELECT 1 FROM core.visible_events e WHERE e.source=c.source
             AND NOT e.event_type = ANY($1))
           UNION SELECT name FROM registry.sources
           WHERE NOT event_types <@ $1::text[]
           ORDER BY 1"#,
        &bookkeeping_types,
    )
    .fetch_all(&mut *conn)
    .await?;
    let present: std::collections::HashSet<&str> = by_source
        .iter()
        .filter_map(|r| r["source"].as_str())
        .collect();
    let no_data: Vec<&String> = known
        .iter()
        .filter(|s| !present.contains(s.as_str()))
        .collect();
    Ok(json!({
        "from": from.to_string(),
        "to": to.to_string(),
        "inclusive_end": inclusive_end,
        "by_source": by_source,
        "latest_by_source": latest_by_source,
        "no_data": no_data,
    }))
}

/// A time citation names the current spanning output, independent of its ID.
async fn resolve_at(
    conn: &mut PgConnection,
    at: jiff::Timestamp,
    event_type: &str,
) -> Result<Uuid> {
    let alias = sinex_model::query::ALIASES
        .iter()
        .find(|a| a.spans && a.kinds.contains(&event_type))
        .ok_or_else(|| Error::Invalid(format!("{event_type} has no declared query span")))?;
    let (start, end, inclusive) =
        super::fields::span(sinex_model::query::Table::Events, Some(alias.name))
            .ok_or_else(|| Error::Invalid("event type has no query span".into()))?;
    let (start, end) = (start.replace("{t}", "e"), end.replace("{t}", "e"));
    let at = time::OffsetDateTime::from_unix_timestamp_nanos(at.as_nanosecond())
        .map_err(|e| Error::Invalid(e.to_string()))?;
    let sql = format!(
        "SELECT e.id FROM core.visible_events e WHERE e.event_type=$1 AND {start} <= $2 AND ({end} IS NULL OR {end} {} $2) AND e.payload IS NOT NULL AND NOT e.payload ? 'withheld' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY e.id",
        if inclusive { ">=" } else { ">" }
    );
    let ids: Vec<Uuid> = sqlx::query_scalar(&sql)
        .bind(event_type)
        .bind(at)
        .fetch_all(conn)
        .await?;
    match ids.as_slice() {
        [id] => Ok(*id),
        [] => Err(Error::Invalid(
            "no visible spanning output contains this time".into(),
        )),
        _ => Err(Error::Invalid(
            "several visible outputs contain this time; cite an event or record instead".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
