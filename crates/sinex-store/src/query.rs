//! Running queries (architecture §7.1; spec `query.md`): the language
//! compiled to SQL, provenance walks, recall, `show`, and read-only SQL.
//! Each page uses one read-only repeatable snapshot. Continuations retain the
//! original question, clock and event position; a changed visible ordered
//! answer requires a fresh query. Rows use the unit's field names; times are UTC.
mod compile;
mod cursor;
mod fields;
mod recall;
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

pub use cursor::context_for_page;
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
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query!("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await?;
    let size = page.size.unwrap_or(DEFAULT_PAGE).max(1);
    if size > i64::MAX as u64 {
        return Err(Error::Invalid("page size is out of range".into()));
    }
    let ctx = context_for_page(ctx, page)?;
    let scope: (String, String) = sqlx::query_as(
        "SELECT current_user::text, coalesce(current_setting('sinex.grants', true), '')",
    )
    .fetch_one(&mut *tx)
    .await?;
    let question = cursor::question(text, query, &ctx, &scope)?;
    let current = current_seq(&mut tx).await?;
    let prior = page
        .cursor
        .as_deref()
        .map(cursor::Cursor::decode)
        .transpose()?;
    let (mut seq, offset) = match &prior {
        Some(cursor) => {
            cursor.check_question(&question)?;
            if cursor.seq > current {
                return Err(Error::Invalid(
                    "query cursor is ahead of the archive".into(),
                ));
            }
            (cursor.seq, cursor.offset)
        }
        None => (current, 0),
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
    if seq < 0 || seq > current {
        return Err(Error::Invalid(
            "as-of position is outside the known archive".into(),
        ));
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
        Source::Recall { .. } | Source::RecallLast { .. } => "recall".to_string(),
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
            let interpretation = read_as(query);
            let mut hash = blake3::Hasher::new();
            for (ordinal, row) in rows.iter().enumerate() {
                let bytes = serde_json::to_vec(row)
                    .map_err(|_| Error::Invalid("query row cannot be encoded".into()))?;
                hash.update(&(ordinal as u64).to_le_bytes());
                hash.update(&(bytes.len() as u64).to_le_bytes());
                hash.update(&bytes);
            }
            let view = cursor::digest(&serde_json::json!([
                unit,
                interpretation,
                columns,
                total,
                hash.finalize().to_hex().to_string()
            ]))?;
            if let Some(cursor) = &prior {
                cursor.check_view(&view, total)?;
            }
            let start = (offset as usize).min(rows.len());
            let end = start
                .saturating_add(usize::try_from(size).unwrap_or(usize::MAX))
                .min(rows.len());
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
                    cursor: (end < rows.len())
                        .then(|| cursor::Cursor::encode(&question, &ctx, seq, end as u64, &view))
                        .transpose()?,
                },
                recall: None,
            }
        }
        Source::Unit { .. } => {
            let rendered = render_page(Compiler::new(&ctx, seq).compile(query)?, offset, size);
            page_of(
                &mut tx,
                rendered,
                unit,
                text,
                query,
                resolved,
                seq,
                offset,
                size,
                &question,
                &ctx,
                prior.as_ref(),
                None,
            )
            .await?
        }
        Source::Recall { .. } | Source::RecallLast { .. } => {
            let selection = recall::select(&mut tx, &query.source, &ctx, seq).await?;
            let as_events = Query {
                source: Source::Unit {
                    unit: Unit::Events,
                    alias: None,
                },
                filter: Some(selection.filter()),
                stages: query.stages.clone(),
                ranked: query.ranked,
            };
            let rendered = render_page(Compiler::new(&ctx, seq).compile(&as_events)?, offset, size);
            let account = selection.account(&mut tx, seq).await?;
            page_of(
                &mut tx,
                rendered,
                unit,
                text,
                query,
                resolved,
                seq,
                offset,
                size,
                &question,
                &ctx,
                prior.as_ref(),
                Some(account),
            )
            .await?
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
    let object = json_object(&compiled.columns);
    // Ordinary change detection, never an authority or integrity proof. Two
    // seeded row hashes keep constant aggregate state; including _n avoids XOR
    // cancellation when projected rows happen to be equal.
    let row = format!("q.\"_n\"::text || ':' || ({object})::text");
    let mut total = Frag::text(format!(
        "SELECT jsonb_build_array(count(*), coalesce(bit_xor(hashtextextended(({row}) COLLATE \"C\", 0)), 0), coalesce(bit_xor(hashtextextended(({row}) COLLATE \"C\", 982451653)), 0)) FROM ("
    ));
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
    question: &str,
    ctx: &Context,
    prior: Option<&cursor::Cursor>,
    recall: Option<Json>,
) -> Result<Answer> {
    let (sql_text, values) = rendered.total;
    let sqlx::types::Json((total, first, second)): sqlx::types::Json<(i64, i64, i64)> =
        sql::bind_scalar(sqlx::query_scalar(&sql_text), values)
            .fetch_one(&mut *tx)
            .await?;
    let view = cursor::digest(&serde_json::json!([
        unit,
        read_as(query),
        rendered.columns,
        total,
        first,
        second,
        recall
    ]))?;
    if let Some(cursor) = prior {
        cursor.check_view(&view, total)?;
    }
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
            cursor: ((next as i64) < total)
                .then(|| cursor::Cursor::encode(question, ctx, seq, next, &view))
                .transpose()?,
        },
        recall,
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
    if matches!(q.source, Source::RecallLast { .. }) {
        out.push(
            sinex_model::query::Alias::from_name("shell-sessions")
                .expect("shell sessions declared"),
        );
    }
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
