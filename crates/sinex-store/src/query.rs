//! Running queries (architecture §7.1; spec `query.md`): the language
//! compiled to SQL, provenance walks, recall, `show`, and read-only SQL.
//! Every read runs in a read-only transaction at one commit position, so the
//! pages of an answer agree, and its rows come back as JSON objects with the
//! unit's field names; times are UTC.
mod compile;
mod fields;
mod sql;
mod walk;

pub mod read_sql;

use crate::{Error, Result};
use compile::{Compiled, Compiler};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sinex_model::query::{
    AsOf, Context, Expr, FieldRef, Operand, Query, Source, Stage, Unit, read_as,
};
use sql::Frag;
use sqlx::{Connection, PgConnection};

pub use walk::{resolve, show};

/// Which page of an answer to return.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Page {
    /// Rows per page; 50 when not given. The page is not the answer: every
    /// row is reachable through the cursor.
    pub size: Option<u64>,
    /// The cursor from the previous page.
    pub cursor: Option<String>,
}

pub const DEFAULT_PAGE: u64 = 50;

/// One page of an answer (spec `query.md` §10).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Answer {
    pub unit: String,
    pub query: String,
    pub read_as: String,
    pub resolved: Resolved,
    pub columns: Vec<String>,
    pub rows: Vec<Json>,
    /// Rows in the whole answer.
    pub total: i64,
    pub page: PageInfo,
    /// What a recall found per source, and which sources had no data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recall: Option<Json>,
}

/// What the query used: `now`, the zone, the day start and the commit
/// position it read at.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Resolved {
    pub now: String,
    pub zone: String,
    pub day_start: String,
    pub commit_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PageInfo {
    pub size: u64,
    /// `None` on the last page.
    pub cursor: Option<String>,
}

/// A cursor: the commit position the answer is read at and the rows already
/// returned.
fn encode_cursor(seq: i64, offset: u64) -> String {
    format!("{seq}.{offset}")
}

fn decode_cursor(c: &str) -> Result<(i64, u64)> {
    let (s, o) = c
        .split_once('.')
        .ok_or_else(|| Error::Invalid(format!("`{c}` is not a cursor")))?;
    Ok((
        s.parse()
            .map_err(|_| Error::Invalid(format!("`{c}` is not a cursor")))?,
        o.parse()
            .map_err(|_| Error::Invalid(format!("`{c}` is not a cursor")))?,
    ))
}

/// The newest commit position.
pub async fn current_seq(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_one(&mut *conn)
            .await?,
    )
}

/// Run a bound query and return one page of its answer.
pub async fn run(
    conn: &mut PgConnection,
    text: &str,
    query: &Query,
    ctx: &Context,
    page: &Page,
) -> Result<Answer> {
    let mut tx = conn.begin().await?;
    sqlx::query!("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query!("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await?;
    let size = page.size.unwrap_or(DEFAULT_PAGE).max(1);
    let (mut seq, offset) = match &page.cursor {
        Some(c) => decode_cursor(c)?,
        None => (current_seq(&mut tx).await?, 0),
    };
    if page.cursor.is_none() {
        for stage in &query.stages {
            match stage {
                Stage::AsOf(AsOf::Seq(s)) => seq = *s,
                Stage::AsOf(AsOf::Time(t)) => seq = seq_at(&mut tx, *t).await?,
                _ => {}
            }
        }
    }
    check_kinds(&mut tx, query).await?;
    let resolved = Resolved {
        now: ctx.now.to_string(),
        zone: ctx.zone_name(),
        day_start: ctx.day_start.to_string(),
        commit_seq: seq,
    };
    let unit = match &query.source {
        Source::Unit { alias: Some(a), .. } => a.name.to_string(),
        Source::Unit { unit, .. } => unit.name().to_string(),
        Source::Walk { walk, .. } => format!("{walk:?}").to_lowercase(),
        Source::Recall { .. } => "recall".to_string(),
    };
    let mut answer = match &query.source {
        Source::Walk { walk, reference } => {
            let rows = walk::walk(&mut tx, *walk, reference).await?;
            let total = rows.len() as i64;
            let columns = rows
                .first()
                .and_then(Json::as_object)
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default();
            let start = (offset as usize).min(rows.len());
            let end = (start + size as usize).min(rows.len());
            Answer {
                unit,
                query: text.to_string(),
                read_as: read_as(query),
                resolved,
                columns,
                rows: rows[start..end].to_vec(),
                total,
                page: PageInfo {
                    size,
                    cursor: (end < rows.len()).then(|| encode_cursor(seq, end as u64)),
                },
                recall: None,
            }
        }
        Source::Unit { .. } => {
            let rendered = render_page(Compiler::new(ctx, seq).compile(query)?, offset, size);
            page_of(
                &mut tx, rendered, unit, text, query, resolved, seq, offset, size,
            )
            .await?
        }
        Source::Recall { from, to } => {
            // Recall is a fixed query: live events of every source in the
            // window, plus what each source contributed.
            let window = Expr::Within {
                lhs: Operand::Field(FieldRef::Field(
                    Unit::Events.field("when").expect("events have when"),
                )),
                from: *from,
                to: *to,
            };
            let as_events = Query {
                source: Source::Unit {
                    unit: Unit::Events,
                    alias: None,
                },
                filter: Some(window),
                stages: query.stages.clone(),
                ranked: query.ranked,
            };
            let rendered = render_page(Compiler::new(ctx, seq).compile(&as_events)?, offset, size);
            let mut a = page_of(
                &mut tx, rendered, unit, text, query, resolved, seq, offset, size,
            )
            .await?;
            a.recall = Some(walk::recall_summary(&mut tx, *from, *to, seq).await?);
            a
        }
    };
    answer.read_as = read_as(query);
    tx.commit().await?;
    Ok(answer)
}

/// The two statements of one page, rendered before anything is awaited:
/// the answer's size and the page's rows.
struct Rendered {
    total: (String, Vec<sql::Param>),
    rows: (String, Vec<sql::Param>),
    columns: Vec<String>,
}

fn render_page(compiled: Compiled, offset: u64, size: u64) -> Rendered {
    let mut total = Frag::text("SELECT count(*) FROM (");
    total.append(compiled.answer.clone()).push(") q");
    let object = json_object(&compiled.columns);
    let mut rows = Frag::text(format!("SELECT {object} FROM ("));
    rows.append(compiled.answer)
        .push(") q WHERE q.\"_n\" > ")
        .param(sea_query::Value::BigInt(Some(offset as i64)))
        .push(" ORDER BY q.\"_n\" LIMIT ")
        .param(sea_query::Value::BigInt(Some(size as i64)));
    Rendered {
        total: total.render(),
        rows: rows.render(),
        columns: compiled.columns,
    }
}

#[allow(clippy::too_many_arguments)]
async fn page_of(
    tx: &mut PgConnection,
    rendered: Rendered,
    unit: String,
    text: &str,
    query: &Query,
    resolved: Resolved,
    seq: i64,
    offset: u64,
    size: u64,
) -> Result<Answer> {
    let (sql_text, values) = rendered.total;
    let total: i64 = sql::bind_scalar(sqlx::query_scalar(&sql_text), values)
        .fetch_one(&mut *tx)
        .await?;
    let (sql_text, values) = rendered.rows;
    let rows: Vec<Json> = sql::bind_scalar(sqlx::query_scalar(&sql_text), values)
        .fetch_all(&mut *tx)
        .await?;
    let next = offset + rows.len() as u64;
    Ok(Answer {
        unit,
        query: text.to_string(),
        read_as: read_as(query),
        resolved,
        columns: rendered.columns,
        rows,
        total,
        page: PageInfo {
            size,
            cursor: ((next as i64) < total).then(|| encode_cursor(seq, next)),
        },
        recall: None,
    })
}

/// `jsonb_build_object` over the columns, in order, in chunks the function's
/// argument limit allows.
fn json_object(columns: &[String]) -> String {
    if columns.is_empty() {
        return "'{}'::jsonb".to_string();
    }
    let parts: Vec<String> = columns
        .chunks(50)
        .map(|chunk| {
            let pairs: Vec<String> = chunk
                .iter()
                .map(|c| {
                    format!(
                        "'{}', q.\"{}\"",
                        c.replace('\'', "''"),
                        c.replace('"', "\"\"")
                    )
                })
                .collect();
            format!("jsonb_build_object({})", pairs.join(", "))
        })
        .collect();
    parts.join(" || ")
}

/// The commit position Sinex had reached at a time.
async fn seq_at(conn: &mut PgConnection, t: jiff::Timestamp) -> Result<i64> {
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(t.as_nanosecond())
        .map_err(|e| Error::Invalid(format!("time out of range: {e}")))?;
    Ok(sqlx::query_scalar!(
        r#"SELECT coalesce(max(commit_seq), 0) AS "seq!" FROM core.events WHERE ts_persisted <= $1"#,
        odt
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// An alias over kinds nothing produces yet is refused, never answered empty.
async fn check_kinds(conn: &mut PgConnection, query: &Query) -> Result<()> {
    let mut aliases = Vec::new();
    collect_aliases(query, &mut aliases);
    for alias in aliases {
        let kinds: Vec<String> = alias.kinds.iter().map(|k| k.to_string()).collect();
        let registered = sqlx::query_scalar!(
            r#"SELECT EXISTS (SELECT 1 FROM registry.event_types WHERE name = ANY($1::text[])) AS "registered!""#,
            &kinds
        )
        .fetch_one(&mut *conn)
        .await?;
        if !registered {
            return Err(Error::Invalid(format!(
                "`{}` reads {}, which no source or derivation produces yet",
                alias.name,
                kinds.join(", ")
            )));
        }
    }
    Ok(())
}

fn collect_aliases(q: &Query, out: &mut Vec<&'static sinex_model::query::Alias>) {
    if let Source::Unit { alias: Some(a), .. } = &q.source {
        out.push(a);
    }
    for s in &q.stages {
        if let Stage::Join(j) = s {
            collect_aliases(&j.inner, out);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
