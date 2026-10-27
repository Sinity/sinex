//! Citations, `show`, provenance walks and recall's per-source account. These
//! are fixed queries, checked at compile time.
use crate::{Error, Result};
use serde_json::{Value as Json, json};
use sinex_model::query::{Reference, Walk};
use sqlx::PgConnection;
use uuid::Uuid;

/// Walks follow at most this many levels: the bound the archive cascade uses,
/// reported when reached rather than cut silently.
const WALK_DEPTH: i32 = 100;

/// The event a citation points at: for `ev`, that reading, live or archived;
/// for `occ` and `eq`, the live reading of the occurrence.
pub async fn resolve(conn: &mut PgConnection, reference: &Reference) -> Result<Uuid> {
    match reference {
        Reference::Window { .. } => Err(Error::Invalid(
            "a time window is a recall query, not one event".into(),
        )),
        Reference::At { at, event_type } => resolve_at(conn, *at, event_type).await,
        Reference::Event(id) => {
            let found = sqlx::query_scalar!("SELECT id FROM core.events WHERE id = $1", id)
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
            let live = sqlx::query!(
                r#"SELECT e.id AS "event_id!", e.event_type AS "event_type!" FROM core.events e
                   WHERE e.material_id = $1 AND e.anchor = $2
                   AND ($3::text IS NULL OR e.event_type = $3)
                   AND ($4::smallint IS NULL OR e.output_index = $4)
                   AND (EXISTS (SELECT 1 FROM core.live_locations c WHERE c.event_id = e.id)
                        OR EXISTS (SELECT 1 FROM core.live_equivalence q WHERE q.event_id = e.id))"#,
                material,
                offset,
                event_type.as_deref(),
                *output
            )
            .fetch_all(&mut *conn)
            .await?;
            match live.as_slice() {
                [one] => Ok(one.event_id),
                [] => {
                    let archived = sqlx::query_scalar!(
                        r#"SELECT e.id FROM core.events e JOIN audit.archived a ON a.event_id = e.id
                           WHERE e.material_id = $1 AND e.anchor = $2
                           AND ($3::text IS NULL OR e.event_type = $3)
                           ORDER BY a.archived_seq DESC"#,
                        material,
                        offset,
                        event_type.as_deref()
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
                "SELECT event_id FROM core.live_equivalence WHERE source = $1 AND event_type = $2 AND equivalence_key = $3",
                source,
                event_type,
                key
            )
            .fetch_optional(&mut *conn)
            .await?;
            found.ok_or_else(|| {
                Error::Invalid(format!("no live {event_type} of {source} with key {key}"))
            })
        }
    }
}

/// One event in full: the reading, whether it is live, where it came from,
/// and its parents.
pub async fn show(conn: &mut PgConnection, reference: &Reference) -> Result<Json> {
    let id = resolve(conn, reference).await?;
    let row = sqlx::query_scalar!(
        r#"SELECT jsonb_build_object(
             'id', e.id, 'event_type', e.event_type, 'source', e.source,
             'when', e.ts_orig, 'time_quality', e.ts_quality, 'seen', e.ts_coided,
             'written', e.ts_persisted, 'commit_seq', e.commit_seq,
             'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
             'archived', a.archived_at, 'archive_reason', a.reason,
             'record_state', e.record_state, 'class', e.class, 'key', e.equivalence_key,
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
    .fetch_one(&mut *conn)
    .await?;
    Ok(row)
}

/// A provenance walk's rows.
pub(crate) async fn walk(
    conn: &mut PgConnection,
    walk: Walk,
    reference: &Reference,
) -> Result<Vec<Json>> {
    let id = resolve(conn, reference).await?;
    let rows = match walk {
        Walk::Parents => {
            sqlx::query_scalar!(
                r#"SELECT jsonb_build_object(
                     'id', e.id, 'event_type', e.event_type, 'source', e.source, 'when', e.ts_orig,
                     'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
                     'material', e.material_id, 'offset', e.anchor, 'payload', e.payload) AS "row!"
                   FROM core.visible_events c
                   CROSS JOIN LATERAL unnest(c.parents) AS p(id)
                   JOIN core.visible_events e ON e.id = p.id
                   LEFT JOIN audit.archived a ON a.event_id = e.id
                   WHERE c.id = $1
                   ORDER BY e.ts_orig NULLS LAST, e.id"#,
                id
            )
            .fetch_all(&mut *conn)
            .await?
        }
        Walk::Descendants => {
            sqlx::query_scalar!(
                r#"WITH RECURSIVE d(id, depth) AS (
                     SELECT e.id, 1 FROM core.visible_events e WHERE e.parents @> ARRAY[$1::uuid]
                     UNION
                     SELECT e.id, d.depth + 1 FROM d JOIN core.visible_events e ON e.parents @> ARRAY[d.id]
                     WHERE d.depth < $2
                   ), first AS (SELECT id, min(depth) AS depth FROM d GROUP BY id)
                   SELECT jsonb_build_object(
                     'depth', f.depth, 'id', e.id, 'event_type', e.event_type, 'source', e.source,
                     'when', e.ts_orig,
                     'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
                     'depth_bound_reached', f.depth >= $2,
                     'payload', e.payload) AS "row!"
                   FROM first f JOIN core.visible_events e ON e.id = f.id
                   LEFT JOIN audit.archived a ON a.event_id = e.id
                   ORDER BY f.depth, e.ts_orig NULLS LAST, e.id"#,
                id,
                WALK_DEPTH
            )
            .fetch_all(&mut *conn)
            .await?
        }
        Walk::Trace => {
            sqlx::query_scalar!(
                r#"WITH RECURSIVE up(id, depth) AS (
                     SELECT $1::uuid, 0
                     UNION
                     SELECT p.id, up.depth + 1 FROM up
                     JOIN core.visible_events e ON e.id = up.id
                     CROSS JOIN LATERAL unnest(e.parents) AS p(id)
                     WHERE up.depth < $2
                   ), first AS (SELECT id, min(depth) AS depth FROM up GROUP BY id)
                   SELECT jsonb_build_object(
                     'depth', f.depth, 'id', e.id, 'event_type', e.event_type, 'source', e.source,
                     'when', e.ts_orig,
                     'state', CASE WHEN a.event_id IS NULL THEN 'live' ELSE 'archived' END,
                     'origin', CASE WHEN e.material_id IS NULL THEN 'derived' ELSE 'material' END,
                     'material', e.material_id, 'offset', e.anchor,
                     'bytes', CASE WHEN e.material_id IS NULL THEN NULL
                       WHEN m.id IS NULL THEN 'unavailable: material record missing'
                       ELSE m.blob END,
                     'parser', m.parser, 'parser_version', m.parser_version,
                     'run', e.run_id, 'producer', r.name,
                     'depth_bound_reached', f.depth >= $2,
                     'payload', e.payload) AS "row!"
                   FROM first f JOIN core.visible_events e ON e.id = f.id
                   LEFT JOIN audit.archived a ON a.event_id = e.id
                   LEFT JOIN raw.materials m ON m.id = e.material_id
                   LEFT JOIN core.runs r ON r.id = e.run_id
                   ORDER BY f.depth, e.id"#,
                id,
                WALK_DEPTH
            )
            .fetch_all(&mut *conn)
            .await?
        }
    };
    Ok(rows)
}

/// What each source recorded in a recall window, and which sources had no
/// data there: "no data", never "nothing happened".
pub(crate) async fn recall_summary(
    conn: &mut PgConnection,
    from: jiff::Timestamp,
    to: jiff::Timestamp,
    seq: i64,
    inclusive_end: bool,
    rows: &sinex_model::query::Query,
    ctx: &sinex_model::query::Context,
) -> Result<Json> {
    use sinex_model::query::{AggFn, Aggregate, FieldRef, Group, Operand, Stage, Table};
    let field = |name| FieldRef::Field(Table::Events.field(name).expect("event field declared"));
    let mut selector = rows.clone();
    selector.stages = rows
        .stages
        .iter()
        .take_while(|s| !matches!(s, Stage::Group(_)))
        .filter(|s| !matches!(s, Stage::Fields(_)))
        .cloned()
        .collect();
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
        let compiled = super::Compiler::new(ctx, seq).compile(&summary)?;
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
            let compiled = super::Compiler::new(ctx, seq).compile(&selector)?;
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
    let known = sqlx::query_scalar!(
        r#"SELECT source AS "name!" FROM core.source_counts
           UNION SELECT name FROM registry.sources ORDER BY 1"#
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
