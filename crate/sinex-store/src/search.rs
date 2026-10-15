//! Full-population indexed SQL lowering. No event payload is joined before the
//! matching population is ordered and its page has been selected.
use crate::{Error, ReadPool, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::retrieval::{Bound, Expr, Text, bind, normalize};
use sqlx::{Connection, Postgres, QueryBuilder, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub fn indexed(text: &str) -> bool {
    crate::query::validate_query(text).is_err()
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
            q.push("s.tsv @@ ")
                .push_bind(t.tsquery())
                .push("::text::tsquery");
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
                let time = OffsetDateTime::parse(value, &Rfc3339).expect("bound time");
                let at = time
                    .replace_nanosecond(time.nanosecond() / 1000 * 1000)
                    .unwrap()
                    .format(&Rfc3339)
                    .unwrap();
                q.push("CASE WHEN s.ts_orig IS NULL THEN NULL::boolean ELSE ")
                    .push(column)
                    .push(op.as_str())
                    .push("(")
                    .push_bind(at)
                    .push("::text::timestamptz,")
                    .push_bind((time.nanosecond() % 1000) as i32)
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
                // Registered text comparisons use the same casefold/NFC operation as
                // full text. Projection columns carry normalized companion values.
                let column = match field.as_str() {
                    "command" => "s.command_normalized",
                    "cwd" => "s.cwd_normalized",
                    "source" => "s.source",
                    "kind" => "s.event_type",
                    "project" => "p.project",
                    _ => unreachable!(),
                };
                let escaped = normalize(value)
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
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
    #[serde(deserialize_with = "required_when")]
    when: Option<String>,
    id: Uuid,
}
fn required_when<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}
fn snippet(text: &str, terms: &[Text]) -> Result<Value> {
    let fields: Vec<String> = serde_json::from_str(text)
        .map_err(|_| Error::ProjectionUnavailable("projection text backing unavailable"))?;
    for field in &fields {
        let candidate = snippet_field(field, terms);
        if candidate["spans"].as_array().is_some_and(|s| !s.is_empty()) {
            return Ok(candidate);
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
            let mut ok = true;
            for (i, word) in term.words.iter().enumerate() {
                let Some((_, raw)) = tokens.get(start + i) else {
                    ok = false;
                    break;
                };
                let actual = normalize(raw);
                if !(if term.prefix && i + 1 == term.words.len() {
                    actual.starts_with(word)
                } else {
                    actual == *word
                }) {
                    ok = false;
                    break;
                }
            }
            if ok {
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
        match tokio::time::timeout(budget, async {
            let settle=tokio::time::Instant::now()+std::time::Duration::from_millis(500);
            loop {
                let result=self.search_page(text,bound.clone(),cursor,page,budget).await;
                if matches!(&result,Err(Error::ProjectionUnavailable(reason)) if *reason=="canonical interpretation or indexed projection has not caught up; retry or inspect status") && tokio::time::Instant::now()<settle {tokio::time::sleep(std::time::Duration::from_millis(25)).await;continue;}
                break result;
            }
        }).await
        {
            Err(_) => Err(Error::Deadline),
            Ok(Err(Error::Database(sqlx::Error::Database(e))))
                if e.code().as_deref() == Some("57014") =>
            {
                Err(Error::Deadline)
            }
            Ok(result) => result,
        }
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
        let mut lease = crate::query::ReadLease(Some(self.0.acquire().await?));
        let mut tx = lease.0.as_mut().unwrap().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true),set_config('client_connection_check_interval','100ms',true)").bind(format!("{}ms",budget.as_millis().max(1))).execute(&mut *tx).await?;
        let current: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        let state=sqlx::query("SELECT generation,position,serving,failure FROM derivation.read_model_position WHERE name='shell-search'").fetch_optional(&mut *tx).await?.ok_or(Error::ProjectionUnavailable("indexed retrieval is not configured"))?;
        let generation: Uuid = state.try_get("generation")?;
        let position: i64 = state.try_get("position")?;
        let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM derivation.shell_dirty)")
            .fetch_one(&mut *tx)
            .await?;
        if state.try_get::<Option<String>, _>("failure")?.is_some() {
            return Err(Error::ProjectionUnavailable(
                "projection maintenance unavailable; inspect status",
            ));
        }
        if !state.try_get::<bool, _>("serving")?
            || state.try_get::<Option<String>, _>("failure")?.is_some()
            || position != current
            || dirty
        {
            return Err(Error::ProjectionUnavailable(
                "canonical interpretation or indexed projection has not caught up; retry or inspect status",
            ));
        }
        let after: Option<Cursor> = cursor
            .map(|raw| {
                if raw.len() > 4096 {
                    return Err(Error::Invalid("invalid indexed cursor"));
                }
                let c: Cursor = serde_json::from_str(raw)
                    .map_err(|_| Error::Invalid("invalid indexed cursor"))?;
                let resolved = OffsetDateTime::parse(&c.now, &Rfc3339)
                    .map_err(|_| Error::Invalid("invalid indexed cursor clock"))?;
                if c.first < 0
                    || c.emitted == 0
                    || c.emitted >= bound.limit.unwrap_or(10000)
                    || resolved > clock
                    || c.expires != resolved.unix_timestamp() + 86400
                    || c.rank < 0.0
                    || c.when
                        .as_ref()
                        .is_some_and(|w| w.len() != 30 || !w.ends_with('Z'))
                    || c.version != 1
                    || c.query != digest
                    || c.generation != generation
                    || c.expires <= clock.unix_timestamp()
                    || c.first > current
                    || !c.rank.is_finite()
                    || c.when
                        .as_ref()
                        .is_some_and(|w| OffsetDateTime::parse(w, &Rfc3339).is_err())
                {
                    return Err(Error::Invalid(
                        "indexed cursor expired or incompatible with query/generation",
                    ));
                }
                Ok(c)
            })
            .transpose()?;
        let emitted = after.as_ref().map_or(0, |c| c.emitted);
        let first = after.as_ref().map_or(current, |c| c.first);
        let now = after
            .as_ref()
            .map_or_else(|| clock.format(&Rfc3339).unwrap(), |c| c.now.clone());
        let expires = after
            .as_ref()
            .map_or(clock.unix_timestamp() + 86400, |c| c.expires);
        let text_query = bound.filter.as_ref().and_then(Expr::text_query);
        let ranked = bound.sort.is_none() && text_query.is_some();
        let desc = bound.sort == Some(true);
        let order = if ranked {
            "rank DESC,at ASC NULLS LAST,id ASC"
        } else if desc {
            "at DESC NULLS LAST,id ASC"
        } else {
            "at ASC NULLS LAST,id ASC"
        };
        let remaining = bound.limit.unwrap_or(10000).saturating_sub(emitted);
        let size = page.min(remaining);
        let mut q = QueryBuilder::<Postgres>::new("WITH matching AS MATERIALIZED (SELECT s.id,");
        if ranked {
            q.push("ts_rank(s.tsv,")
                .push_bind(text_query.unwrap())
                .push("::text::tsquery)::float8");
        } else {
            q.push("0::float8");
        }
        q.push(" AS rank,CASE WHEN s.ts_orig IS NULL THEN NULL ELSE to_char(s.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(s.ts_orig_tiebreak,0)::text,3,'0') || 'Z' END AS at FROM core.search_text s LEFT JOIN core.event_project p ON p.generation=s.generation AND p.event_id=s.id WHERE s.generation=").push_bind(generation);
        if let Some(filter) = &bound.filter {
            q.push(" AND ");
            expression(&mut q, filter);
        }
        q.push("), page AS MATERIALIZED (SELECT * FROM matching");
        if let Some(c) = after {
            q.push(" WHERE (");
            if ranked {
                q.push("rank<")
                    .push_bind(c.rank)
                    .push(" OR (rank=")
                    .push_bind(c.rank)
                    .push(" AND (");
            }
            match c.when {
                Some(at) => {
                    q.push("at IS NULL OR at")
                        .push(if desc { "<" } else { ">" })
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
            if ranked {
                q.push("))");
            }
            q.push(")");
        }
        q.push(" ORDER BY ").push(order).push(" LIMIT ").push_bind((size+1) as i64).push(") SELECT (SELECT count(*) FROM matching) AS total,COALESCE((SELECT jsonb_agg(row ORDER BY ord) FROM (SELECT ");
        q.push(crate::query::EVENT_VIEW).push(" || jsonb_build_object('project',p.project,'rank',page.rank,'_text',s.text) AS row,row_number() OVER(ORDER BY ");
        q.push(if ranked {
            "page.rank DESC,page.at ASC NULLS LAST,page.id ASC"
        } else if desc {
            "page.at DESC NULLS LAST,page.id ASC"
        } else {
            "page.at ASC NULLS LAST,page.id ASC"
        });
        q.push(") AS ord FROM page JOIN core.events e ON e.id=page.id JOIN core.search_text s ON s.id=page.id AND s.generation=").push_bind(generation).push(" LEFT JOIN core.event_project p ON p.event_id=page.id AND p.generation=s.generation) ordered),'[]'::jsonb) AS rows");
        let row = q.build().fetch_one(&mut *tx).await?;
        let total: i64 = row.try_get("total")?;
        let mut rows: Vec<Value> = row.try_get::<Value, _>("rows")?.as_array().unwrap().clone();
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
        let next = if more && emitted + size < bound.limit.unwrap_or(10000) && size > 0 {
            let last = rows.last().unwrap();
            Some(
                serde_json::to_string(&Cursor {
                    version: 1,
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
            )
        } else {
            None
        };
        let source_status:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('input',i.id,'source',i.binding,'status',o.status,'reason',o.reason) FROM raw.inputs i LEFT JOIN LATERAL (SELECT status,reason FROM raw.observations WHERE input_id=i.id ORDER BY seq DESC LIMIT 1) o ON true").fetch_all(&mut *tx).await?;
        let complete = source_status.iter().all(|s| s["status"] == "completed");
        tx.commit().await?;
        drop(lease.0.take());
        Ok(
            json!({"notice":sinex_model::DATA_NOTICE,"unit":bound.unit,"query":text,"rows":rows,"total":total,"complete":complete,"coverage":"unknown; retained shell observations do not prove continuous capture","source_status":source_status,"truncated":more && next.is_none(),"page":{"size":page,"cursor":next,"first_commit_seq":first,"changed_since_first_page":first!=current,"change_notice":if first!=current{Some("Current-state continuation; additions or changed rank/order keys may be absent or repeated.")}else{None}},"resolved":{"commit_seq":current,"projection_seq":position,"generation":generation,"now":now,"zone":"UTC","logical_start":"04:00"},"read_as":format!("Eligible live single-source canonical commands matching {text}; {}. Continuous source coverage is unknown.",if ranked{"ranked over every selected match, then original time and ID"}else if desc{"ordered by original time descending, unknown last, then ID"}else{"ordered by original time ascending, unknown last, then ID"}),"excluded":{}}),
        )
    }
    pub async fn projection_status(&self) -> Result<Value> {
        let mut lease = self.read_lease().await?;
        let row:Option<Value>=sqlx::query_scalar("SELECT to_jsonb(p) || jsonb_build_object('current_commit_seq',(SELECT seq FROM core.commit_counter WHERE one),'pending_canonical',(SELECT count(*) FROM derivation.shell_dirty)) FROM derivation.read_model_position p WHERE name='shell-search'").fetch_optional(lease.connection()).await?;
        lease.finish();
        Ok(row.unwrap_or(json!({"state":"not_configured"})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// SQL truth control, not a claim that the Atuin parser emits unknown times.
    #[tokio::test]
    #[ignore = "requires the owned PostgreSQL fixture"]
    async fn bound_unknown_time_preserves_three_valued_negation() -> Result<()> {
        let url = std::env::var("DATABASE_URL").expect("owned test database required");
        let mut connection = sqlx::PgConnection::connect(&url).await?;
        for (predicate, known) in [
            ("when = 2026-09-14T00:00:00Z", true),
            ("when != 2026-09-14T00:00:00Z", false),
            ("not when != 2026-09-14T00:00:00Z", true),
            ("when < 2026-09-14T00:00:00Z", false),
            ("not when > 2026-09-14T00:00:00Z", true),
        ] {
            let bound = bind(&format!("commands where {predicate}")).unwrap();
            let mut query = QueryBuilder::<Postgres>::new(
                "WITH s(n,ts_orig,ts_orig_tiebreak) AS (VALUES(0,NULL::timestamptz,789),(1,'2026-09-14T00:00:00Z'::timestamptz,0)) SELECT array_agg(matched ORDER BY n) FROM (SELECT n,",
            );
            expression(&mut query, bound.filter.as_ref().unwrap());
            query.push(" AS matched FROM s) truth");
            let actual: Vec<Option<bool>> = query
                .build_query_scalar()
                .fetch_one(&mut connection)
                .await?;
            assert_eq!(actual, vec![None, Some(known)], "{predicate}");
        }
        connection.close().await?;
        Ok(())
    }
}
