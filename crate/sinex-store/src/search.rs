//! Indexed `commands` queries and `search` over the shell-search projection.
//! The whole matching population is ordered and paged before any event payload
//! is joined.
use crate::query::{begin_snapshot, required_nullable, time_and_tiebreak, with_deadline};
use crate::{Error, ReadPool, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::retrieval::{Bound, Expr, Text, bind, like_literal, normalize};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

const CURSOR_VERSION: u32 = 1;
const CURSOR_LIFETIME_SECONDS: i64 = 86400;
const DEFAULT_LIMIT: u32 = 10000;
/// How many rows without full text `full_text_state` inspects for a reference.
const INSPECTION_LIMIT: usize = 16;
// Collectors commit before the projection catches up; search waits briefly for
// it instead of failing every query that races a commit.
const NOT_CAUGHT_UP: &str =
    "canonical interpretation or indexed projection has not caught up; retry or inspect status";
const CATCH_UP_WAIT: std::time::Duration = std::time::Duration::from_millis(500);
const CATCH_UP_POLL: std::time::Duration = std::time::Duration::from_millis(25);

pub fn indexed(text: &str) -> bool {
    !sinex_model::retrieval::explicit_raw_events(text)
        && crate::query::validate_query(text).is_err()
}
fn expression(q: &mut QueryBuilder<'_, Postgres>, e: &Expr) {
    q.push("(");
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => {
            expression(q, a);
            q.push(if matches!(e, Expr::And(..)) {
                " AND "
            } else {
                " OR "
            });
            expression(q, b);
        }
        Expr::Not(e) => {
            q.push("NOT ");
            expression(q, e);
        }
        Expr::Text(t) => {
            for (index, pattern) in t.like_patterns().into_iter().enumerate() {
                if index > 0 {
                    q.push(" AND ");
                }
                q.push("s.tokens COLLATE \"C\" LIKE ")
                    .push_bind(pattern)
                    .push(" ESCAPE E'\\\\'");
            }
        }
        Expr::Compare { field, op, value } => {
            let column = match field.as_str() {
                "id" => "s.id",
                "when" => "(s.ts_orig,COALESCE(s.ts_orig_tiebreak,0))",
                "source" => "s.source",
                "kind" => "s.event_type",
                "project" => "p.project",
                "cwd" => "s.cwd",
                "exit_code" => "s.exit_code",
                "command" => "s.command",
                _ => unreachable!("bound field"),
            };
            if op.as_str() == "exists" {
                q.push(if field.as_str() == "when" {
                    "s.ts_orig"
                } else {
                    column
                })
                .push(" IS NOT NULL");
            } else if field.as_str() == "when" {
                let (at, tiebreak) = OffsetDateTime::parse(value, &Rfc3339)
                    .ok()
                    .and_then(time_and_tiebreak)
                    .expect("bound time");
                // Unknown time compares as NULL, so NOT keeps it excluded.
                q.push("CASE WHEN s.ts_orig IS NULL THEN NULL::boolean ELSE ")
                    .push(column)
                    .push(op.as_str())
                    .push("(")
                    .push_bind(at)
                    .push("::text::timestamptz,")
                    .push_bind(tiebreak)
                    .push(") END");
            } else if field.as_str() == "exit_code" {
                q.push(column)
                    .push(op.as_str())
                    .push_bind(value.parse::<i64>().expect("bound integer"));
            } else if field.as_str() == "id" {
                q.push(column)
                    .push(op.as_str())
                    .push_bind(Uuid::parse_str(value).expect("bound id"));
            } else if op.as_str() == "^" && field.as_str() == "cwd" {
                if value == "/" {
                    q.push("left(s.cwd,1)='/'");
                } else {
                    q.push(column)
                        .push("=")
                        .push_bind(value.clone())
                        .push(" OR left(s.cwd,")
                        .push_bind(value.chars().count() as i32 + 1)
                        .push(")=")
                        .push_bind(format!("{value}/"));
                }
            } else if op.as_str() == "~" || op.as_str() == "^" {
                // Text comparisons use the same casefold and NFC as full text,
                // against the projection's normalized columns.
                let column = match field.as_str() {
                    "command" => "s.command_normalized",
                    "cwd" => "s.cwd_normalized",
                    "source" => "s.source",
                    "kind" => "s.event_type",
                    "project" => "p.project",
                    _ => unreachable!(),
                };
                let escaped = like_literal(&normalize(value));
                q.push(column)
                    .push(" LIKE ")
                    .push_bind(format!(
                        "{}{}%",
                        if op.as_str() == "~" { "%" } else { "" },
                        escaped
                    ))
                    .push(" ESCAPE E'\\\\'");
            } else {
                q.push(column).push(op.as_str()).push_bind(value.clone());
            }
        }
    }
    q.push(")");
}
#[derive(Serialize)]
struct FullTextState {
    available: bool,
    reason: Option<&'static str>,
    first_affected_ref: Option<String>,
    inspection_limit: usize,
}
// Rows whose token text could not be stored have NULL tokens. Report the first
// one, among the first INSPECTION_LIMIT, that a reader could see now; a null
// reference does not mean later rows are unaffected.
async fn full_text_state(
    connection: &mut PgConnection,
    generation: Uuid,
    metadata_ready: bool,
) -> Result<FullTextState> {
    if !metadata_ready {
        return Ok(FullTextState {
            available: false,
            reason: Some("metadata_not_current"),
            first_affected_ref: None,
            inspection_limit: INSPECTION_LIMIT,
        });
    }
    let unavailable: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM core.search_text WHERE generation=$1 AND tokens IS NULL LIMIT 1)",
    )
    .bind(generation)
    .fetch_one(&mut *connection)
    .await?;
    let first: Option<Uuid> = if unavailable {
        sqlx::query_scalar(&format!(
            "WITH affected AS MATERIALIZED (SELECT id FROM core.search_text WHERE generation=$1 AND tokens IS NULL ORDER BY id LIMIT {INSPECTION_LIMIT}) \
             SELECT e.id FROM affected c JOIN core.live_events e ON e.id=c.id \
             WHERE e.event_type='command.canonical' AND {} ORDER BY e.id LIMIT 1",
            crate::query::CURRENT_PARENTS
        ))
        .bind(generation)
        .fetch_optional(&mut *connection)
        .await?
    } else {
        None
    };
    Ok(FullTextState {
        available: !unavailable,
        reason: unavailable.then_some("exact_full_text_representation_unavailable"),
        first_affected_ref: first.map(|id| format!("sinex:ev/{id}")),
        inspection_limit: INSPECTION_LIMIT,
    })
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    query: String,
    generation: Uuid,
    now: String,
    expires: i64,
    first: i64,
    emitted: u32,
    rank: f64,
    #[serde(deserialize_with = "required_nullable")]
    when: Option<String>,
    id: Uuid,
}
impl Cursor {
    fn decode(
        raw: &str,
        digest: &str,
        generation: Uuid,
        current: i64,
        clock: OffsetDateTime,
        limit: u32,
    ) -> Result<Self> {
        if raw.len() > 4096 {
            return Err(Error::Invalid("invalid indexed cursor"));
        }
        let c: Self =
            serde_json::from_str(raw).map_err(|_| Error::Invalid("invalid indexed cursor"))?;
        let resolved = OffsetDateTime::parse(&c.now, &Rfc3339)
            .map_err(|_| Error::Invalid("invalid indexed cursor clock"))?;
        if c.version != CURSOR_VERSION
            || c.query != digest
            || c.generation != generation
            || c.first < 0
            || c.first > current
            || c.emitted == 0
            || c.emitted >= limit
            || resolved > clock
            || c.expires != resolved.unix_timestamp() + CURSOR_LIFETIME_SECONDS
            || c.expires <= clock.unix_timestamp()
            || !c.rank.is_finite()
            || c.rank < 0.0
            || c.when.as_ref().is_some_and(|w| {
                w.len() != 30 || !w.ends_with('Z') || OffsetDateTime::parse(w, &Rfc3339).is_err()
            })
        {
            return Err(Error::Invalid(
                "indexed cursor expired or incompatible with query/generation",
            ));
        }
        Ok(c)
    }
}
fn snippet(text: &str, terms: &[Text]) -> Result<Value> {
    let fields: Vec<String> = serde_json::from_str(text)
        .map_err(|_| Error::ProjectionUnavailable("projection text backing unavailable"))?;
    for field in &fields {
        let found = snippet_field(field, terms);
        if found["spans"].as_array().is_some_and(|s| !s.is_empty()) {
            return Ok(found);
        }
    }
    Ok(snippet_field(
        fields.first().map_or("", String::as_str),
        terms,
    ))
}
fn snippet_field(text: &str, terms: &[Text]) -> Value {
    use unicode_segmentation::UnicodeSegmentation;
    let tokens: Vec<_> = text.unicode_word_indices().collect();
    let mut spans = Vec::new();
    for term in terms {
        for start in 0..tokens.len() {
            let matches = term.words.iter().enumerate().all(|(i, word)| {
                tokens.get(start + i).is_some_and(|(_, raw)| {
                    let token = normalize(raw);
                    if term.prefix && i + 1 == term.words.len() {
                        token.starts_with(word)
                    } else {
                        token == *word
                    }
                })
            });
            if matches {
                for (at, raw) in &tokens[start..start + term.words.len()] {
                    spans.push((*at, *at + raw.len()));
                }
            }
        }
    }
    spans.sort();
    spans.dedup();
    json!({"text":text,"spans":spans,"span_unit":"utf8_bytes","cut_before":false,"cut_after":false})
}

#[derive(Clone, Copy, PartialEq)]
enum Order {
    Ranked,
    Ascending,
    Descending,
}
impl Order {
    fn sql(self, table: &str) -> String {
        match self {
            Order::Ranked => format!("{table}rank DESC,{table}at ASC NULLS LAST,{table}id ASC"),
            Order::Ascending => format!("{table}at ASC NULLS LAST,{table}id ASC"),
            Order::Descending => format!("{table}at DESC NULLS LAST,{table}id ASC"),
        }
    }
    fn describe(self) -> &'static str {
        match self {
            Order::Ranked => "ranked by word_similarity, then original time and ID",
            Order::Ascending => "by original time, unknown last, then ID",
            Order::Descending => "by original time descending, unknown last, then ID",
        }
    }
}

/// Builds the page query over projection `generation`: the matching
/// population, its total, and up to `size + 1` rendered rows after the cursor.
fn search_query(
    bound: &Bound,
    generation: Uuid,
    order: Order,
    rank_text: Option<String>,
    after: Option<Cursor>,
    size: u32,
) -> QueryBuilder<'_, Postgres> {
    let mut q = QueryBuilder::<Postgres>::new("WITH matching AS MATERIALIZED (SELECT s.id,");
    match rank_text {
        Some(rank_text) if order == Order::Ranked => {
            q.push("word_similarity(")
                .push_bind(rank_text)
                .push(" COLLATE \"C\",s.tokens COLLATE \"C\")::float8");
        }
        _ => {
            q.push("0::float8");
        }
    }
    q.push(
        " AS rank,CASE WHEN s.ts_orig IS NULL THEN NULL ELSE to_char(s.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(s.ts_orig_tiebreak,0)::text,3,'0') || 'Z' END AS at \
         FROM core.search_text s JOIN core.live_events eligible ON eligible.id=s.id \
         LEFT JOIN core.event_project p ON p.generation=s.generation AND p.event_id=s.id WHERE s.generation=",
    )
    .push_bind(generation);
    q.push(" AND NOT EXISTS(SELECT 1 FROM unnest(eligible.source_event_ids) parent(id) WHERE NOT EXISTS(SELECT 1 FROM core.live_events raw WHERE raw.id=parent.id AND raw.record_state='present'))");
    if let Some(filter) = &bound.filter {
        q.push(" AND ");
        expression(&mut q, filter);
    }
    q.push("), page AS MATERIALIZED (SELECT * FROM matching");
    if let Some(c) = after {
        q.push(" WHERE (");
        if order == Order::Ranked {
            q.push("rank<")
                .push_bind(c.rank)
                .push(" OR (rank=")
                .push_bind(c.rank)
                .push(" AND (");
        }
        match c.when {
            Some(at) => {
                q.push("at IS NULL OR at")
                    .push(if order == Order::Descending { "<" } else { ">" })
                    .push_bind(at.clone())
                    .push(" OR (at=")
                    .push_bind(at)
                    .push(" AND id>")
                    .push_bind(c.id)
                    .push(")");
            }
            None => {
                q.push("at IS NULL AND id>").push_bind(c.id);
            }
        }
        if order == Order::Ranked {
            q.push("))");
        }
        q.push(")");
    }
    q.push(" ORDER BY ")
        .push(order.sql(""))
        .push(" LIMIT ")
        .push_bind((size + 1) as i64)
        .push(") SELECT (SELECT count(*) FROM matching) AS total,COALESCE((SELECT jsonb_agg(row ORDER BY ord) FROM (SELECT ");
    q.push(crate::query::EVENT_VIEW)
        .push(" || jsonb_build_object('project',p.project,'rank',page.rank,'_text',s.text) AS row,row_number() OVER(ORDER BY ")
        .push(order.sql("page."));
    q.push(") AS ord FROM page JOIN core.events e ON e.id=page.id JOIN core.search_text s ON s.id=page.id AND s.generation=")
        .push_bind(generation)
        .push(" LEFT JOIN core.event_project p ON p.event_id=page.id AND p.generation=s.generation) ordered),'[]'::jsonb) AS rows");
    q
}

impl ReadPool {
    pub async fn search_with_timeout(
        &self,
        text: &str,
        cursor: Option<&str>,
        page: u32,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let bound = bind(text).map_err(Error::Invalid)?;
        if !(1..=100).contains(&page) {
            return Err(Error::Invalid("indexed page must be 1..=100"));
        }
        with_deadline(budget, async {
            let give_up = tokio::time::Instant::now() + CATCH_UP_WAIT;
            loop {
                let result = self
                    .search_page(text, bound.clone(), cursor, page, budget)
                    .await;
                if matches!(&result, Err(Error::ProjectionUnavailable(reason)) if *reason == NOT_CAUGHT_UP)
                    && tokio::time::Instant::now() < give_up
                {
                    tokio::time::sleep(CATCH_UP_POLL).await;
                    continue;
                }
                break result;
            }
        })
        .await
    }
    async fn search_page(
        &self,
        text: &str,
        bound: Bound,
        cursor: Option<&str>,
        page: u32,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let digest = blake3::hash(serde_json::to_string(&bound).unwrap().as_bytes())
            .to_hex()
            .to_string();
        let clock = OffsetDateTime::now_utc();
        let limit = bound.limit.unwrap_or(DEFAULT_LIMIT);
        let mut lease = self.read_lease().await?;
        let (mut tx, current) = begin_snapshot(lease.connection(), budget).await?;
        let state = sqlx::query(
            "SELECT generation,position,serving,failure FROM derivation.read_model_position WHERE name='shell-search'",
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::ProjectionUnavailable("indexed retrieval is not configured"))?;
        let generation: Uuid = state.try_get("generation")?;
        let position: i64 = state.try_get("position")?;
        if state.try_get::<Option<String>, _>("failure")?.is_some() {
            return Err(Error::ProjectionUnavailable(
                "projection maintenance unavailable; inspect status",
            ));
        }
        if !state.try_get::<bool, _>("serving")? || position != current {
            return Err(Error::ProjectionUnavailable(NOT_CAUGHT_UP));
        }
        let full_text = full_text_state(&mut tx, generation, true).await?;
        if bound.filter.as_ref().is_some_and(Expr::has_text) && !full_text.available {
            return Err(Error::ProjectionUnavailable(
                "exact full-text representation unavailable; field-only metadata queries remain supported; inspect status",
            ));
        }
        let fusion: Value = sqlx::query_scalar(
            "SELECT to_jsonb(p)||jsonb_build_object('current_now_settled',NOT EXISTS(SELECT 1 FROM derivation.shell_dirty)) \
             FROM derivation.shell_progress p WHERE one",
        )
        .fetch_one(&mut *tx)
        .await?;
        let after = cursor
            .map(|raw| Cursor::decode(raw, &digest, generation, current, clock, limit))
            .transpose()?;
        let emitted = after.as_ref().map_or(0, |c| c.emitted);
        let first = after.as_ref().map_or(current, |c| c.first);
        let now = after
            .as_ref()
            .map_or_else(|| clock.format(&Rfc3339).unwrap(), |c| c.now.clone());
        let expires = after
            .as_ref()
            .map_or(clock.unix_timestamp() + CURSOR_LIFETIME_SECONDS, |c| {
                c.expires
            });
        let rank_text = bound.filter.as_ref().and_then(Expr::rank_text);
        let order = match bound.sort {
            None if rank_text.is_some() => Order::Ranked,
            Some(true) => Order::Descending,
            _ => Order::Ascending,
        };
        let size = page.min(limit.saturating_sub(emitted));
        let row = search_query(&bound, generation, order, rank_text, after, size)
            .build()
            .fetch_one(&mut *tx)
            .await?;
        let total: i64 = row.try_get("total")?;
        let mut rows: Vec<Value> = serde_json::from_value(row.try_get("rows")?)
            .map_err(|_| Error::Invalid("invalid database page"))?;
        let more = rows.len() > size as usize;
        if more {
            rows.pop();
        }
        let mut terms = Vec::new();
        if let Some(e) = &bound.filter {
            e.terms(&mut terms);
        }
        for row in &mut rows {
            let text = row.as_object_mut().unwrap().remove("_text").unwrap();
            row["snippet"] = snippet(text.as_str().unwrap(), &terms)?;
        }
        let next = match rows.last() {
            Some(last) if more && emitted + size < limit => Some(
                serde_json::to_string(&Cursor {
                    version: CURSOR_VERSION,
                    query: digest,
                    generation,
                    now: now.clone(),
                    expires,
                    first,
                    emitted: emitted + size,
                    rank: last["rank"].as_f64().unwrap(),
                    when: last["when"].as_str().map(str::to_string),
                    id: Uuid::parse_str(last["id"].as_str().unwrap()).unwrap(),
                })
                .unwrap(),
            ),
            _ => None,
        };
        let source_status = crate::source_coverage::read(&mut tx, current).await?;
        let complete = crate::source_coverage::interpreted_complete(&source_status)
            && fusion["current_now_settled"] == true
            && fusion["phase"] == "settled"
            && fusion["failure"].is_null();
        tx.commit().await?;
        lease.finish();
        Ok(json!({
            "notice": sinex_model::DATA_NOTICE,
            "unit": bound.unit,
            "query": text,
            "rows": rows,
            "total": total,
            "complete": complete,
            "fusion": fusion,
            "coverage": "unknown",
            "source_status": source_status,
            "full_text": full_text,
            "truncated": more && next.is_none(),
            "page": {
                "size": page,
                "cursor": next,
                "first_commit_seq": first,
                "changed_since_first_page": first != current,
                "change_notice": (first != current).then_some(crate::query::CHANGE_NOTICE),
            },
            "resolved": {
                "commit_seq": current,
                "projection_seq": position,
                "generation": generation,
                "now": now,
                "zone": "UTC",
                "logical_start": "04:00",
            },
            "read_as": format!("Live canonical commands, {}.", order.describe()),
        }))
    }
    pub async fn projection_status(&self) -> Result<Value> {
        let mut lease = self.read_lease().await?;
        let mut tx = lease.connection().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let row: Option<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(p) || jsonb_build_object(\
             'current_commit_seq',(SELECT seq FROM core.commit_counter WHERE one),\
             'pending_sessions',(SELECT count(*) FROM derivation.session_dirty),\
             'sessions',(SELECT to_jsonb(s)||jsonb_build_object('pending_through_day',(SELECT max(coalesce(through_day,day)) FROM derivation.session_dirty)) FROM derivation.session_progress s WHERE one),\
             'pending_canonical',(SELECT count(*) FROM derivation.shell_dirty),\
             'fusion',(SELECT to_jsonb(f) FROM derivation.shell_progress f WHERE one)) \
             FROM derivation.read_model_position p WHERE name='shell-search'",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let mut value = row.unwrap_or(json!({"state":"not_configured"}));
        if let Some(generation) = value["generation"].as_str() {
            let generation = Uuid::parse_str(generation)
                .map_err(|_| Error::Invalid("invalid projection generation"))?;
            let current = value["serving"] == true
                && value["failure"].is_null()
                && value["position"] == value["current_commit_seq"];
            value["full_text"] =
                serde_json::to_value(full_text_state(&mut tx, generation, current).await?)
                    .expect("typed full-text state serializes");
        }
        tx.commit().await?;
        lease.finish();
        Ok(value)
    }
}

#[cfg(test)]
#[path = "search_tests.rs"]
mod tests;
