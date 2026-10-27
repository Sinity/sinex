//! Running queries: the language compiled to SQL, provenance walks, recall,
//! `show`, and read-only SQL. Each page uses one read-only repeatable snapshot.
//! Continuations keep the original question, clock and event position; a
//! changed visible ordered answer requires a fresh query. Rows use the table's
//! field names; times are UTC.
mod compile;
mod cursor;
mod fields;
mod recall;
mod source_views;
mod sql;
mod walk;

pub mod read_sql;

use crate::{Error, Result};
use compile::{Compiled, Compiler};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sinex_model::query::{
    AsOf, Context, Expr, FieldRef, Operand, Query, Source, Stage, Table, read_as,
};
use sql::SqlFragment;
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

/// One page of an answer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Answer {
    pub table: String,
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

/// Let this session see the classes in `grants` (comma-separated) through the
/// visibility views.
pub async fn set_grants(conn: &mut PgConnection, grants: &str) -> sqlx::Result<()> {
    sqlx::query_scalar!("SELECT set_config('sinex.grants', $1, false)", grants)
        .fetch_one(&mut *conn)
        .await?;
    Ok(())
}

/// Make the transaction just begun on `conn` read-only, with every statement
/// seeing the same snapshot.
pub async fn read_one_snapshot(conn: &mut PgConnection) -> Result<()> {
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *conn)
        .await?;
    Ok(())
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
    read_one_snapshot(&mut tx).await?;
    sqlx::query!("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await?;
    let size = page.size.unwrap_or(DEFAULT_PAGE).max(1);
    if size > i64::MAX as u64 {
        return Err(Error::Invalid("page size is out of range".into()));
    }
    let ctx = context_for_page(ctx, page)?;
    let scope = sqlx::query!(
        r#"SELECT current_user::text AS "user!", coalesce(current_setting('sinex.grants', true), '') AS "grants!""#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let scope = (scope.user, scope.grants);
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
    if matches!(query.source, Source::Walk { .. })
        && (query.filter.is_some() || !query.stages.is_empty())
    {
        return Err(Error::Invalid(
            "provenance walks do not support query stages; use a separate event query".into(),
        ));
    }
    let read = ReadPage {
        question,
        ctx,
        seq,
        offset,
        size,
        prior,
    };
    let table = match &query.source {
        Source::Table { alias: Some(a), .. } => a.name.to_string(),
        Source::Table { table, .. } => table.name().to_string(),
        Source::Walk { walk, .. } => format!("{walk:?}").to_lowercase(),
        Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. } => {
            "recall".to_string()
        }
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
            let reading = read_as(query);
            let mut hash = blake3::Hasher::new();
            for (output_index, row) in rows.iter().enumerate() {
                let bytes = serde_json::to_vec(row)
                    .map_err(|_| Error::Invalid("query row cannot be encoded".into()))?;
                hash.update(&(output_index as u64).to_le_bytes());
                hash.update(&(bytes.len() as u64).to_le_bytes());
                hash.update(&bytes);
            }
            let view = cursor::digest(&serde_json::json!([
                table,
                reading,
                columns,
                total,
                hash.finalize().to_hex().to_string()
            ]))?;
            read.check_view(&view)?;
            let start = (read.offset as usize).min(rows.len());
            let end = start
                .saturating_add(usize::try_from(read.size).unwrap_or(usize::MAX))
                .min(rows.len());
            Answer {
                table,
                query: text.to_string(),
                read_as: read_as(query),
                resolved: read.resolved(),
                columns,
                rows: rows[start..end].to_vec(),
                total,
                page: read.info(&view, total, end - start)?,
                recall: None,
            }
        }
        Source::Table { .. } => {
            let rendered = render_page(
                Compiler::new(&read.ctx, read.seq).compile(query)?,
                read.offset,
                read.size,
                read.prior.is_none(),
            );
            page_of(&mut tx, rendered, table, text, query, &read, None).await?
        }
        Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. } => {
            let selection = recall::select(&mut tx, &query.source, &read.ctx, read.seq).await?;
            let as_events = Query {
                source: Source::Table {
                    table: Table::Events,
                    alias: None,
                },
                filter: Some(selection.filter()),
                stages: query.stages.clone(),
                ranked: query.ranked,
            };
            let rendered = render_page(
                Compiler::new(&read.ctx, read.seq).compile(&as_events)?,
                read.offset,
                read.size,
                read.prior.is_none(),
            );
            let account = selection.account(&mut tx, read.seq).await?;
            page_of(&mut tx, rendered, table, text, query, &read, Some(account)).await?
        }
    };
    answer.read_as = read_as(query);
    tx.commit().await?;
    Ok(answer)
}

/// The validated clock and position shared by every answer shape in this read.
struct ReadPage {
    question: String,
    ctx: Context,
    seq: i64,
    offset: u64,
    size: u64,
    prior: Option<cursor::Cursor>,
}

impl ReadPage {
    fn resolved(&self) -> Resolved {
        Resolved {
            now: self.ctx.now.to_string(),
            zone: self.ctx.zone_name(),
            day_start: self.ctx.day_start.to_string(),
            commit_seq: self.seq,
        }
    }

    fn check_view(&self, view: &str) -> Result<()> {
        match &self.prior {
            Some(cursor) => cursor.check_view(view),
            None => Ok(()),
        }
    }

    fn info(&self, view: &str, total: i64, returned: usize) -> Result<PageInfo> {
        let next = self
            .offset
            .checked_add(returned as u64)
            .ok_or_else(|| Error::Invalid("query page offset is out of range".into()))?;
        let cursor = if next < total.max(0) as u64 {
            if returned == 0 {
                return Err(Error::Invalid("query page did not advance".into()));
            }
            Some(cursor::Cursor::encode(
                &self.question,
                &self.ctx,
                self.seq,
                next,
                total,
                view,
            )?)
        } else {
            None
        };
        Ok(PageInfo {
            size: self.size,
            cursor,
        })
    }
}

/// The statements of one page, rendered before anything is awaited. Each
/// reads the numbered answer only as far as this page: PostgreSQL stops
/// numbering rows once `_n` passes the bound, so a page costs the rows up to
/// its end, never the whole answer. Only the first page counts the answer.
struct Rendered {
    /// The answer's size, when asked to count it (on a first page).
    total: Option<(String, Vec<sql::Param>)>,
    /// Hashes of the rows handed out before this page and of those up to its
    /// end, and their counts.
    seen: (String, Vec<sql::Param>),
    rows: (String, Vec<sql::Param>),
    columns: Vec<String>,
}

fn render_page(compiled: Compiled, offset: u64, size: u64, count: bool) -> Rendered {
    let object = json_object(&compiled.columns);
    let end = offset.saturating_add(size).min(i64::MAX as u64) as i64;
    let total = count.then(|| compiled.count.render());
    // Ordinary change detection, never an access or integrity check. Two seeded
    // row hashes; including _n avoids XOR cancellation when projected rows
    // happen to be equal.
    let row = format!("q.\"_n\"::text || ':' || ({object})::text");
    let hash = |seed: u64| format!("hashtextextended(({row}) COLLATE \"C\", {seed})");
    let mut seen = SqlFragment::text(format!(
        "SELECT jsonb_build_array(\
         coalesce(bit_xor({h0}) FILTER (WHERE q.\"_n\" <= {offset}), 0), \
         coalesce(bit_xor({h1}) FILTER (WHERE q.\"_n\" <= {offset}), 0), \
         count(*) FILTER (WHERE q.\"_n\" <= {offset}), \
         coalesce(bit_xor({h0}), 0), coalesce(bit_xor({h1}), 0), count(*)) FROM (",
        h0 = hash(0),
        h1 = hash(982_451_653),
        offset = offset as i64,
    ));
    seen.append(compiled.answer.clone())
        .push(") q WHERE q.\"_n\" <= ")
        .param(sea_query::Value::BigInt(Some(end)));
    let mut rows = SqlFragment::text(format!("SELECT {object} FROM ("));
    rows.append(compiled.answer)
        .push(") q WHERE q.\"_n\" > ")
        .param(sea_query::Value::BigInt(Some(offset as i64)))
        .push(" AND q.\"_n\" <= ")
        .param(sea_query::Value::BigInt(Some(end)))
        .push(" ORDER BY q.\"_n\"");
    Rendered {
        total,
        seen: seen.render(),
        rows: rows.render(),
        columns: compiled.columns,
    }
}

async fn page_of(
    tx: &mut PgConnection,
    rendered: Rendered,
    table: String,
    text: &str,
    query: &Query,
    read: &ReadPage,
    recall: Option<Json>,
) -> Result<Answer> {
    let total = match (rendered.total, &read.prior) {
        (Some((sql_text, values)), _) => {
            sql::bind_scalar(sqlx::query_scalar::<_, i64>(&sql_text), values)
                .fetch_one(&mut *tx)
                .await?
        }
        (None, Some(cursor)) => cursor.total,
        (None, None) => unreachable!("a first page is rendered with its count"),
    };
    let (sql_text, values) = rendered.seen;
    let sqlx::types::Json((before_0, before_1, before, upto_0, upto_1, upto)): sqlx::types::Json<
        (i64, i64, i64, i64, i64, i64),
    > = sql::bind_scalar(sqlx::query_scalar(&sql_text), values)
        .fetch_one(&mut *tx)
        .await?;
    let view = |count: i64, first: i64, second: i64| {
        cursor::digest(&serde_json::json!([
            table,
            read_as(query),
            rendered.columns,
            count,
            first,
            second,
            recall
        ]))
    };
    if before != read.offset as i64 {
        return Err(Error::StaleCursor);
    }
    read.check_view(&view(before, before_0, before_1)?)?;
    let (sql_text, values) = rendered.rows;
    let rows: Vec<Json> = sql::bind_scalar(sqlx::query_scalar(&sql_text), values)
        .fetch_all(&mut *tx)
        .await?;
    // The answer shrank past the rows already handed out.
    if rows.is_empty() && read.prior.is_some() {
        return Err(Error::StaleCursor);
    }
    let page = read.info(&view(upto, upto_0, upto_1)?, total, rows.len())?;
    Ok(Answer {
        table,
        query: text.to_string(),
        read_as: read_as(query),
        resolved: read.resolved(),
        columns: rendered.columns,
        rows,
        total,
        page,
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
        r#"SELECT coalesce(max(commit_seq), 0) AS "seq!" FROM (
               SELECT commit_seq FROM core.events WHERE ts_persisted <= $1
               UNION ALL
               SELECT commit_seq FROM audit.archive_changes WHERE recorded_at <= $1
           ) positions"#,
        odt
    )
    .fetch_one(&mut *conn)
    .await?)
}

#[cfg(test)]
pub(crate) mod tests;
