//! The `events` query language over stored raw events, plus `show` and `trace`
//! for one event. Unsupported syntax is refused before any database query.
use crate::{Error, PrivacyRule, ReadPool, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row, Transaction};
use time::{Duration, OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub(crate) const EVENT_VIEW: &str = concat!(
    "jsonb_build_object('id',e.id,'source',e.source,'host',e.host,'payload',e.payload,",
    "'parents',e.source_event_ids,'run',e.run_id,'operation',e.created_by_operation,",
    "'store','activity','time_suspect',e.time_suspect,'time_until',e.ts_orig_until,",
    "'written_by',e.written_by,'kind',e.event_type,",
    "'when',CASE WHEN e.ts_orig IS NULL THEN NULL ELSE to_char(e.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(e.ts_orig_tiebreak,0)::text,3,'0') || 'Z' END,",
    "'seen',to_char(e.ts_coided AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),",
    "'written',to_char(e.ts_persisted AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),",
    "'time_quality',e.ts_quality,'time_clock',e.ts_clock,'time_precision',e.ts_precision,",
    "'origin',CASE WHEN e.source_material_id IS NULL THEN 'derived' ELSE 'material' END,",
    "'material',e.source_material_id,'anchor',e.anchor_byte)",
);

// Derived events chain session -> canonical command -> raw event. A derived
// event is readable only while its parents and their parents are present;
// during a replacement an old canonical event can stay live briefly.
pub(crate) const CURRENT_PARENTS: &str = concat!(
    "NOT EXISTS(SELECT 1 FROM unnest(e.source_event_ids) parent(id) WHERE NOT EXISTS(",
    "SELECT 1 FROM core.live_events p WHERE p.id=parent.id AND p.record_state='present' AND NOT EXISTS(",
    "SELECT 1 FROM unnest(p.source_event_ids) grand(id) WHERE NOT EXISTS(",
    "SELECT 1 FROM core.live_events raw WHERE raw.id=grand.id AND raw.record_state='present'))))",
);

/// Maps an elapsed client budget or a PostgreSQL statement cancel (57014) to
/// `Error::Deadline`.
pub(crate) async fn with_deadline<T>(
    budget: std::time::Duration,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(budget, work).await {
        Err(_) => Err(Error::Deadline),
        Ok(Err(Error::Database(sqlx::Error::Database(error))))
            if error.code().as_deref() == Some("57014") =>
        {
            Err(Error::Deadline)
        }
        Ok(result) => result,
    }
}

/// Opens a read-only repeatable-read transaction whose statements stop after
/// `budget`, and returns it with the commit sequence it sees.
pub(crate) async fn begin_snapshot(
    connection: &mut PgConnection,
    budget: std::time::Duration,
) -> Result<(Transaction<'_, Postgres>, i64)> {
    let mut tx = connection.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "SELECT set_config('statement_timeout',$1,true),\
         set_config('client_connection_check_interval','100ms',true)",
    )
    .bind(format!("{}ms", budget.as_millis().max(1)))
    .execute(&mut *tx)
    .await?;
    let seq: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
        .fetch_one(&mut *tx)
        .await?;
    Ok((tx, seq))
}

/// Splits a time into the microsecond `ts_orig` text and the nanosecond
/// remainder stored in `ts_orig_tiebreak`.
pub(crate) fn time_and_tiebreak(time: OffsetDateTime) -> Option<(String, i32)> {
    let micros = time
        .replace_nanosecond(time.nanosecond() / 1000 * 1000)
        .ok()?
        .format(&Rfc3339)
        .ok()?;
    Some((micros, (time.nanosecond() % 1000) as i32))
}

/// Deserializes an `Option` that must be present in the input, even as null.
pub(crate) fn required_nullable<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}

#[derive(Debug, Clone)]
pub struct QueryError(pub &'static str);
#[derive(Debug)]
struct Filter {
    field: &'static str,
    op: char,
    value: Value,
}
fn keyword<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    text.get(..word.len())
        .filter(|head| head.eq_ignore_ascii_case(word))
        .map(|_| &text[word.len()..])
}
fn parse(text: &str) -> std::result::Result<Vec<Filter>, QueryError> {
    if text.len() > 16_384 {
        return Err(QueryError("query exceeds 16 KiB"));
    }
    let mut rest = keyword(text.trim(), "events")
        .ok_or(QueryError(
            "only the raw events unit is available; commands requires fusion",
        ))?
        .trim();
    if rest.is_empty() {
        return Ok(vec![]);
    }
    rest = keyword(rest, "where ")
        .ok_or(QueryError("expected events where FIELD OP VALUE"))?
        .trim();
    let mut filters = Vec::new();
    loop {
        let (filter, used) = parse_filter(rest)?;
        filters.push(filter);
        if filters.len() > 32 {
            return Err(QueryError("at most 32 filters"));
        }
        rest = rest[used..].trim_start();
        if rest.is_empty() {
            break;
        }
        rest = keyword(rest, "and ")
            .ok_or(QueryError(
                "only conjunctions are available in the raw-event subset",
            ))?
            .trim_start();
    }
    Ok(filters)
}
/// Parses one `FIELD OP VALUE` term and returns it with the bytes it used.
fn parse_filter(text: &str) -> std::result::Result<(Filter, usize), QueryError> {
    let end = text
        .find(|c: char| c.is_whitespace())
        .ok_or(QueryError("missing field operator"))?;
    let field = match &text[..end] {
        "source" => "source",
        "kind" => "kind",
        "payload.command" => "payload.command",
        "payload.exit_code" => "payload.exit_code",
        "payload.url" => "payload.url",
        "payload.title" => "payload.title",
        "payload.profile" => "payload.profile",
        "payload.database_generation" => "payload.database_generation",
        "payload.guid" => "payload.guid",
        "payload.target_path" => "payload.target_path",
        "payload.redirect" => "payload.redirect",
        "when" | "ts_orig" => "when",
        "id" => "id",
        _ => {
            return Err(QueryError(
                "supported fields: source, kind, payload.command, payload.exit_code, payload.url, payload.title, payload.profile, payload.guid, payload.target_path, payload.redirect, when, id",
            ));
        }
    };
    let rest = text[end..].trim_start();
    let op = rest.chars().next().ok_or(QueryError("missing operator"))?;
    if op != '='
        && !(op == '~'
            && matches!(
                field,
                "payload.command" | "payload.url" | "payload.title" | "payload.target_path"
            ))
    {
        return Err(QueryError(
            "supported operators: =; command, url, title and target_path also support ~",
        ));
    }
    let rest = rest[op.len_utf8()..].trim_start();
    let (value, used) = if rest.starts_with('"') {
        let mut stream = serde_json::Deserializer::from_str(rest).into_iter::<String>();
        let value = stream
            .next()
            .ok_or(QueryError("missing string"))?
            .map_err(|_| QueryError("invalid JSON string"))?;
        (Value::String(value), stream.byte_offset())
    } else {
        let n = rest.find(char::is_whitespace).unwrap_or(rest.len());
        if n == 0 {
            return Err(QueryError("missing filter value"));
        }
        let word = &rest[..n];
        let value = match field {
            "payload.redirect" => json!(
                word.parse::<bool>()
                    .map_err(|_| QueryError("redirect must be true or false"))?
            ),
            "payload.exit_code" => json!(
                word.parse::<i64>()
                    .map_err(|_| QueryError("exit_code must be an integer"))?
            ),
            _ => json!(word),
        };
        (value, n)
    };
    match field {
        "payload.exit_code" if !value.is_i64() => {
            return Err(QueryError("exit_code must be an integer"));
        }
        "payload.redirect" if !value.is_boolean() => {
            return Err(QueryError("redirect must be true or false"));
        }
        "when" => {
            OffsetDateTime::parse(
                value.as_str().ok_or(QueryError("when must be RFC3339"))?,
                &Rfc3339,
            )
            .map_err(|_| QueryError("when must be RFC3339"))?;
        }
        "id" => {
            Uuid::parse_str(value.as_str().ok_or(QueryError("invalid event ID"))?)
                .map_err(|_| QueryError("invalid event ID"))?;
        }
        _ => {}
    }
    let used = text.len() - rest.len() + used;
    Ok((Filter { field, op, value }, used))
}

/// True when no filter names a kind or source other than these, so the query
/// can match events of this derived family.
fn may_match(filters: &[Filter], kind: &str, source: &str) -> bool {
    !filters.iter().any(|f| {
        (f.field == "kind" && f.value.as_str() != Some(kind))
            || (f.field == "source" && f.value.as_str() != Some(source))
    })
}
/// True when some filter names this derived family's kind or source.
fn names(filters: &[Filter], kind: &str, source: &str) -> bool {
    filters.iter().any(|f| {
        (f.field == "kind" && f.value.as_str() == Some(kind))
            || (f.field == "source" && f.value.as_str() == Some(source))
    })
}
const CANONICAL: (&str, &str) = ("command.canonical", "fusion.shell-command");
const SESSION: (&str, &str) = ("activity.session.boundary", "session");

/// Accepts queries that cannot match canonical commands, which need schema
/// changes that are not applied yet.
pub fn validate_installed_raw_query(text: &str) -> std::result::Result<(), QueryError> {
    if may_match(&parse(text)?, CANONICAL.0, CANONICAL.1) {
        Err(QueryError(
            "canonical queries unavailable in installed-schema read mode; apply waiting changes and restart",
        ))
    } else {
        Ok(())
    }
}

pub fn validate_query(text: &str) -> std::result::Result<(), QueryError> {
    parse(text).map(|_| ())
}
// Bump this descriptor when query meaning, schema reads, defaults or key ordering change.
const CURSOR_VERSION: u32 = 2;
const CURSOR_ORDER: &str = "raw-present-activity-v2:when-ns-asc-null-last,id-asc";
const CURSOR_LIFETIME: Duration = Duration::hours(24);
const PAGE_SIZE: usize = 100;
pub(crate) const CHANGE_NOTICE: &str = "Pages read current state: rows added, removed or reordered since the first page can be missing or repeated.";

/// Digest of the query's meaning: conjunction order, repeated terms, quoting and
/// whitespace do not change it.
fn query_digest(filters: &[Filter]) -> Result<String> {
    let mut terms = Vec::new();
    for f in filters {
        let value = match f.field {
            "when" => json!(
                OffsetDateTime::parse(f.value.as_str().unwrap(), &Rfc3339)
                    .map_err(|_| Error::Invalid("invalid original time"))?
                    .to_offset(UtcOffset::UTC)
                    .format(&Rfc3339)
                    .map_err(|_| Error::Invalid("invalid original time"))?
            ),
            "id" => json!(
                Uuid::parse_str(f.value.as_str().unwrap())
                    .map_err(|_| Error::Invalid("invalid event ID"))?
                    .to_string()
            ),
            _ => f.value.clone(),
        };
        terms.push(
            serde_json::to_string(&(f.field, f.op, value))
                .map_err(|_| Error::Invalid("query encoding failed"))?,
        );
    }
    terms.sort();
    terms.dedup();
    Ok(blake3::hash(
        serde_json::to_string(&terms)
            .map_err(|_| Error::Invalid("query encoding failed"))?
            .as_bytes(),
    )
    .to_hex()
    .to_string())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    order: String,
    query: String,
    now: String,
    zone: String,
    logical_start: String,
    first_commit_seq: i64,
    expires_at: String,
    #[serde(deserialize_with = "required_nullable")]
    last_when: Option<String>,
    last_id: String,
}
impl Cursor {
    fn decode(raw: &str, digest: &str, current: i64, clock: OffsetDateTime) -> Result<Self> {
        if raw.len() > 4096 {
            return Err(Error::Invalid("invalid page cursor"));
        }
        let c: Self = serde_json::from_str(raw)
            .map_err(|_| Error::Invalid("invalid or incompatible page cursor"))?;
        if c.version != CURSOR_VERSION || c.order != CURSOR_ORDER {
            return Err(Error::Invalid("incompatible page cursor version/order"));
        }
        let now = OffsetDateTime::parse(&c.now, &Rfc3339)
            .map_err(|_| Error::Invalid("invalid cursor resolved time"))?;
        let expiry = OffsetDateTime::parse(&c.expires_at, &Rfc3339)
            .map_err(|_| Error::Invalid("invalid cursor expiry"))?;
        if expiry <= clock {
            return Err(Error::Invalid("page cursor expired; issue a fresh query"));
        }
        if c.query != digest
            || c.zone != "UTC"
            || c.logical_start != "04:00"
            || c.first_commit_seq < 0
            || c.first_commit_seq > current
            || now > clock
            || now.checked_add(CURSOR_LIFETIME) != Some(expiry)
            || Uuid::parse_str(&c.last_id)
                .ok()
                .map(|id| id.to_string())
                .as_deref()
                != Some(c.last_id.as_str())
            || c.last_when.as_ref().is_some_and(|at| {
                // SQL emits fixed-width UTC nanoseconds; any other form would sort differently.
                at.len() != 30
                    || !at.ends_with('Z')
                    || at.as_bytes().get(19) != Some(&b'.')
                    || OffsetDateTime::parse(at, &Rfc3339).is_err()
            })
        {
            return Err(Error::Invalid(
                "cursor does not match query/order/resolved context",
            ));
        }
        Ok(c)
    }
}

/// What every page of one traversal shares, taken from the cursor or from the
/// first page.
struct Traversal {
    digest: String,
    first_commit_seq: i64,
    now: String,
    expires_at: String,
}
impl Traversal {
    fn new(
        digest: String,
        after: Option<&Cursor>,
        seq: i64,
        clock: OffsetDateTime,
    ) -> Result<Self> {
        Ok(match after {
            Some(c) => Self {
                digest,
                first_commit_seq: c.first_commit_seq,
                now: c.now.clone(),
                expires_at: c.expires_at.clone(),
            },
            None => Self {
                digest,
                first_commit_seq: seq,
                now: clock
                    .format(&Rfc3339)
                    .map_err(|_| Error::Invalid("resolved clock encoding failed"))?,
                expires_at: (clock + CURSOR_LIFETIME)
                    .format(&Rfc3339)
                    .map_err(|_| Error::Invalid("cursor expiry encoding failed"))?,
            },
        })
    }
    fn next_cursor(&self, last: &Value) -> Result<String> {
        serde_json::to_string(&Cursor {
            version: CURSOR_VERSION,
            order: CURSOR_ORDER.to_string(),
            query: self.digest.clone(),
            now: self.now.clone(),
            zone: "UTC".to_string(),
            logical_start: "04:00".to_string(),
            first_commit_seq: self.first_commit_seq,
            expires_at: self.expires_at.clone(),
            last_when: last["when"].as_str().map(str::to_string),
            last_id: last["id"]
                .as_str()
                .ok_or(Error::Invalid("invalid event identity"))?
                .to_string(),
        })
        .map_err(|_| Error::Invalid("cursor encoding failed"))
    }
}

/// Builds the page query: the matching population at commit `seq`, its total,
/// and up to one row more than a page after the cursor key.
fn page_query(
    filters: Vec<Filter>,
    seq: i64,
    after: Option<Cursor>,
) -> Result<QueryBuilder<'static, Postgres>> {
    let mut q = QueryBuilder::new(
        "WITH matching AS (SELECT e.id,to_char(e.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(e.ts_orig_tiebreak,0)::text,3,'0') || 'Z' AS at \
         FROM core.events e WHERE e.record_state='present' AND e.commit_seq<=",
    );
    q.push_bind(seq)
        .push(" AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=")
        .push_bind(seq)
        .push(")");
    q.push(" AND (e.event_type NOT IN ('command.canonical','activity.session.boundary') OR ")
        .push(CURRENT_PARENTS)
        .push(")");
    if !filters.iter().any(|f| f.field == "payload.redirect") {
        q.push(" AND (e.source <> 'browser.history.chromium' OR e.event_type <> 'page.visited' OR e.payload->'redirect' IS DISTINCT FROM 'true'::jsonb)");
    }
    for f in filters {
        q.push(" AND ");
        push_filter(&mut q, f)?;
    }
    // Each page is a fresh read keyed by (original time, ID); removals and new
    // commits since the previous page apply.
    q.push("), page AS (SELECT id,at FROM matching");
    if let Some(c) = after {
        q.push(" WHERE ");
        match c.last_when {
            Some(at) => {
                q.push("(at IS NULL OR (at,id::text)>(")
                    .push_bind(at)
                    .push(",")
                    .push_bind(c.last_id)
                    .push("))");
            }
            None => {
                q.push("at IS NULL AND id::text>").push_bind(c.last_id);
            }
        }
    }
    q.push(format!(
        " ORDER BY at NULLS LAST,id LIMIT {}), \
         rendered AS (SELECT {EVENT_VIEW} AS event FROM page p JOIN core.events e ON e.id=p.id) \
         SELECT (SELECT count(*) FROM matching) AS total,\
         COALESCE((SELECT jsonb_agg(event ORDER BY event->>'when' NULLS LAST,event->>'id') FROM rendered),'[]'::jsonb) AS rows",
        PAGE_SIZE + 1
    ));
    Ok(q)
}
fn push_filter(q: &mut QueryBuilder<'static, Postgres>, f: Filter) -> Result<()> {
    let text = || f.value.as_str().unwrap().to_string();
    match f.field {
        "source" => {
            q.push("e.source=").push_bind(text());
        }
        "kind" => {
            q.push("e.event_type=").push_bind(text());
        }
        "payload.exit_code" | "payload.redirect" => {
            q.push("e.payload->")
                .push_bind(f.field["payload.".len()..].to_string())
                .push("=")
                .push_bind(f.value);
        }
        field if field.starts_with("payload.") => {
            let key = field["payload.".len()..].to_string();
            if f.op == '~' {
                q.push("strpos(e.payload->>")
                    .push_bind(key)
                    .push(",")
                    .push_bind(text())
                    .push(")>0");
            } else {
                q.push("e.payload->>")
                    .push_bind(key)
                    .push("=")
                    .push_bind(text());
            }
        }
        "when" => {
            let (at, tiebreak) = OffsetDateTime::parse(f.value.as_str().unwrap(), &Rfc3339)
                .ok()
                .and_then(time_and_tiebreak)
                .ok_or(Error::Invalid("invalid original time"))?;
            q.push("(e.ts_orig=")
                .push_bind(at)
                .push("::text::timestamptz AND COALESCE(e.ts_orig_tiebreak,0)=")
                .push_bind(tiebreak)
                .push(")");
        }
        "id" => {
            q.push("e.id=").push_bind(
                Uuid::parse_str(f.value.as_str().unwrap())
                    .map_err(|_| Error::Invalid("invalid event ID"))?,
            );
        }
        _ => return Err(Error::Invalid("invalid query field")),
    }
    Ok(())
}

fn finished(progress: &Value) -> bool {
    progress["phase"] == "settled" && progress["failure"].is_null()
}

/// Reads shell fusion and session progress. Fails when the query needs a
/// derived family that the installed schema or configuration cannot serve.
async fn derived_progress(tx: &mut PgConnection, filters: &[Filter]) -> Result<(Value, Value)> {
    let fusion_installed: bool =
        sqlx::query_scalar("SELECT to_regclass('derivation.shell_progress') IS NOT NULL")
            .fetch_one(&mut *tx)
            .await?;
    if !fusion_installed && may_match(filters, CANONICAL.0, CANONICAL.1) {
        return Err(Error::Schema(
            "canonical query unavailable until ordered schema changes are applied".into(),
        ));
    }
    let fusion = if fusion_installed {
        sqlx::query_scalar(
            "SELECT to_jsonb(p)||jsonb_build_object(\
             'current_now_settled',NOT EXISTS(SELECT 1 FROM derivation.shell_dirty),\
             'enabled',EXISTS(SELECT 1 FROM core.producer_versions WHERE name='fusion.shell-command')) \
             FROM derivation.shell_progress p WHERE one",
        )
        .fetch_one(&mut *tx)
        .await?
    } else {
        json!({"enabled":false,"state":"schema_changes_pending","reason":"installed schema supports raw events only"})
    };
    let sessions_installed: bool =
        sqlx::query_scalar("SELECT to_regclass('derivation.session_progress') IS NOT NULL")
            .fetch_one(&mut *tx)
            .await?;
    let sessions = if sessions_installed {
        sqlx::query_scalar(
            "SELECT to_jsonb(p)||jsonb_build_object(\
             'enabled',EXISTS(SELECT 1 FROM core.producer_versions v JOIN core.runs r ON r.id=v.run_id WHERE v.name='session' AND r.status='running'),\
             'pending_keys',(SELECT count(*) FROM derivation.session_dirty),\
             'pending_through_day',(SELECT max(coalesce(through_day,day)) FROM derivation.session_dirty)) \
             FROM derivation.session_progress p WHERE one",
        )
        .fetch_one(&mut *tx)
        .await?
    } else {
        json!({"enabled":false,"state":"schema_changes_pending"})
    };
    if names(filters, SESSION.0, SESSION.1) && sessions["enabled"] != true {
        return Err(Error::ProjectionUnavailable(
            "shell session reconciliation is not configured; show and trace remain available",
        ));
    }
    Ok((fusion, sessions))
}

/// A page is complete when every source finished its last poll without
/// dropping records and every enabled derived family it can match is caught up.
fn page_complete(filters: &[Filter], fusion: &Value, sessions: &Value, sources: &[Value]) -> bool {
    let fusion_current = fusion["current_now_settled"] == true && finished(fusion);
    let sessions_ok = !may_match(filters, SESSION.0, SESSION.1)
        || sessions["enabled"] != true
        || (sessions["pending_keys"] == 0 && fusion_current && finished(sessions));
    let canonical_ok = !may_match(filters, CANONICAL.0, CANONICAL.1)
        || fusion["enabled"] != true
        || fusion_current;
    sessions_ok && crate::source_coverage::admitted_complete(sources) && canonical_ok
}

// Dropping a fetch alone can leave PostgreSQL executing while SQLx waits to
// recycle the connection. Close this transport instead: PostgreSQL's
// connection checks abort the query when the socket goes away, and no
// PID-targeted cancel can reach a later, unrelated session.
pub(crate) struct ReadLease(Option<sqlx::pool::PoolConnection<Postgres>>);
impl ReadLease {
    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        &mut *self.0.as_mut().expect("owned read connection")
    }
    /// Returns the connection to the pool. Call only after the read completed.
    pub(crate) fn finish(mut self) {
        drop(self.0.take());
    }
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        if let Some(connection) = self.0.as_mut() {
            connection.close_on_drop();
        }
    }
}
impl ReadPool {
    pub(crate) async fn read_lease(&self) -> Result<ReadLease> {
        Ok(ReadLease(Some(self.0.acquire().await?)))
    }
    pub async fn query(&self, text: &str, cursor: Option<&str>) -> Result<Value> {
        self.query_with_timeout(text, cursor, std::time::Duration::from_secs(10))
            .await
    }
    pub async fn query_with_timeout(
        &self,
        text: &str,
        cursor: Option<&str>,
        budget: std::time::Duration,
    ) -> Result<Value> {
        with_deadline(budget, self.query_page(text, cursor, budget)).await
    }
    async fn query_page(
        &self,
        text: &str,
        cursor: Option<&str>,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let filters = parse(text).map_err(|e| Error::Invalid(e.0))?;
        let digest = query_digest(&filters)?;
        let clock = OffsetDateTime::now_utc();
        let mut lease = self.read_lease().await?;
        let (mut tx, seq) = begin_snapshot(lease.connection(), budget).await?;
        let after = cursor
            .map(|raw| Cursor::decode(raw, &digest, seq, clock))
            .transpose()?;
        let traversal = Traversal::new(digest, after.as_ref(), seq, clock)?;
        let (fusion, sessions) = derived_progress(&mut tx, &filters).await?;
        let source_status = crate::source_coverage::read(&mut tx, seq).await?;
        let complete = page_complete(&filters, &fusion, &sessions, &source_status);
        let row = page_query(filters, seq, after)?
            .build()
            .fetch_one(&mut *tx)
            .await?;
        let total: i64 = row.try_get("total")?;
        let mut rows: Value = row.try_get("rows")?;
        let page = rows
            .as_array_mut()
            .ok_or(Error::Invalid("invalid database page"))?;
        let next = if page.len() > PAGE_SIZE {
            page.pop();
            let last = page.last().ok_or(Error::Invalid("invalid nonempty page"))?;
            Some(traversal.next_cursor(last)?)
        } else {
            None
        };
        tx.commit().await?;
        lease.finish();
        let changed = seq != traversal.first_commit_seq;
        Ok(json!({
            "notice": sinex_model::DATA_NOTICE,
            "query": text,
            "unit": "events",
            "total": total,
            "rows": rows,
            "page": {
                "size": PAGE_SIZE,
                "cursor": next,
                "cursor_version": CURSOR_VERSION,
                "first_commit_seq": traversal.first_commit_seq,
                "expires_at": traversal.expires_at,
                "changed_since_first_page": changed,
                "change_notice": changed.then_some(CHANGE_NOTICE),
            },
            "truncated": false,
            "complete": complete,
            "sessions": sessions,
            "fusion": fusion,
            "source_status": source_status,
            "resolved": {"commit_seq": seq, "now": traversal.now, "zone": "UTC", "logical_start": "04:00"},
        }))
    }
    pub async fn show(&self, id: &str) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let mut lease = self.read_lease().await?;
        let row: Option<Value> = sqlx::query_scalar(&format!(
            "SELECT {EVENT_VIEW} || jsonb_build_object(\
             'current_eligible',e.record_state='present' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id) AND {CURRENT_PARENTS},\
             'archive',(SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id),\
             'source_state',CASE WHEN e.record_state='removed' THEN 'removed at source' ELSE 'present' END) \
             FROM core.events e WHERE e.id=$1"
        ))
        .bind(id)
        .fetch_optional(lease.connection())
        .await?;
        lease.finish();
        Ok(row)
    }
    /// Traces a derived event to its parents, recursively, and a raw event to
    /// its stored record, whose bytes are read and masked when requested.
    pub async fn trace(
        &self,
        id: &str,
        bytes_requested: bool,
        current_rules: &[PrivacyRule],
    ) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let mut lease = self.read_lease().await?;
        let parents: Option<Vec<Uuid>> =
            sqlx::query_scalar("SELECT source_event_ids FROM core.events WHERE id=$1")
                .bind(id)
                .fetch_optional(lease.connection())
                .await?
                .flatten();
        lease.finish();
        let Some(parents) = parents else {
            return self
                .trace_material(id, bytes_requested, current_rules)
                .await;
        };
        let mut traced = Vec::new();
        for parent in parents {
            traced.push(
                Box::pin(self.trace(&parent.to_string(), bytes_requested, current_rules))
                    .await?
                    .ok_or(Error::Invalid("canonical parent evidence unavailable"))?,
            );
        }
        let current = self
            .show(&id.to_string())
            .await?
            .ok_or(Error::Invalid("canonical metadata unavailable"))?;
        let representation = if current["kind"] == "activity.session.boundary" {
            "shell_session"
        } else {
            "canonical_shell_group"
        };
        Ok(Some(json!({
            "event": id,
            "representation": representation,
            "parents": traced,
            "archive": current["archive"],
            "current_eligible": current["current_eligible"],
        })))
    }
    async fn trace_material(
        &self,
        id: Uuid,
        bytes_requested: bool,
        current_rules: &[PrivacyRule],
    ) -> Result<Option<Value>> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query(
            "SELECT e.source_material_id,e.anchor_byte,e.offset_end,e.record_state,\
             (SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id) AS archive,\
             m.durable_bytes,m.parser_provenance,m.input_id,m.observation_id \
             FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE e.id=$1",
        )
        .bind(id)
        .fetch_optional(lease.connection())
        .await?;
        lease.finish();
        let Some(row) = row else { return Ok(None) };
        let material: Uuid = row.try_get("source_material_id")?;
        let anchor: i64 = row.try_get("anchor_byte")?;
        let end: Option<i64> = row.try_get("offset_end")?;
        let end = end.ok_or(Error::Invalid("event has no stored record range"))?;
        let durable: i64 = row.try_get("durable_bytes")?;
        if anchor < 0
            || end < anchor
            || end > durable
            || end - anchor > crate::MAX_STEP_BYTES as i64
        {
            return Err(Error::Invalid("stored record range unavailable"));
        }
        let representation =
            if row.try_get::<Value, _>("parser_provenance")?["source"] == crate::ZSH_SOURCE {
                "original_zsh_history"
            } else {
                "canonical_sqlite_row"
            };
        let mut traced = json!({
            "event": id,
            "material": material,
            "input": row.try_get::<Option<Uuid>, _>("input_id")?,
            "material_observation": row.try_get::<Option<Uuid>, _>("observation_id")?,
            "anchor": anchor,
            "end": end,
            "record_state": row.try_get::<String, _>("record_state")?,
            "archive": row.try_get::<Option<Value>, _>("archive")?,
            "representation": representation,
        });
        if bytes_requested {
            let (record, _) = self
                .stored_record(material, anchor, Some(end), Some(current_rules))
                .await?;
            traced["record"] = record;
        }
        Ok(Some(traced))
    }
    /// Reads one stored record starting at `anchor`; `end`, when given, must be
    /// its exact end. With `mask_with`, the record is masked with the rules it
    /// was stored under followed by these current rules; without, it is
    /// returned unmasked (replay). Returns the record and its end offset.
    pub(crate) async fn stored_record(
        &self,
        material: Uuid,
        anchor: i64,
        end: Option<i64>,
        mask_with: Option<&[PrivacyRule]>,
    ) -> Result<(Value, i64)> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query(
            "SELECT m.durable_bytes,b.blake3,m.parser_provenance FROM raw.source_material_registry m \
             LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1",
        )
        .bind(material)
        .fetch_one(lease.connection())
        .await?;
        lease.finish();
        let durable: i64 = row.try_get("durable_bytes")?;
        let expected_end = end;
        let end = end.unwrap_or_else(|| {
            anchor
                .saturating_add(crate::MAX_STEP_BYTES as i64)
                .min(durable)
        });
        if anchor < 0
            || end <= anchor
            || end > durable
            || end - anchor > crate::MAX_STEP_BYTES as i64
        {
            return Err(Error::Invalid("stored record range unavailable"));
        }
        let blob: Option<String> = row.try_get("blake3")?;
        let bytes = self.read_material(material, blob, anchor, end).await?;
        let provenance: Value = row.try_get("parser_provenance")?;
        let (record, length) = decode_record(&bytes, &provenance, mask_with)?;
        let record_end = anchor + length as i64;
        if expected_end.is_some_and(|end| end != record_end) {
            return Err(Error::Invalid("stored record range spans other data"));
        }
        Ok((record, record_end))
    }
    /// Reads `anchor..end` from a sealed blob, or from the open material file
    /// when the material has no blob yet.
    async fn read_material(
        &self,
        material: Uuid,
        blob: Option<String>,
        anchor: i64,
        end: i64,
    ) -> Result<Vec<u8>> {
        let blobs = self.1.clone();
        let root = self.2.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            use std::io::{Read, Seek, SeekFrom};
            if let Some(name) = blob {
                Ok(blobs.read_range(
                    &crate::parse_hash(&name)?,
                    anchor as u64,
                    (end - anchor) as u64,
                )?)
            } else {
                let mut file = std::fs::File::open(
                    std::path::Path::new(&root)
                        .join("open")
                        .join(material.to_string()),
                )?;
                file.seek(SeekFrom::Start(anchor as u64))?;
                let mut bytes = vec![0; (end - anchor) as usize];
                file.read_exact(&mut bytes)?;
                Ok(bytes)
            }
        })
        .await?
    }
}

/// Decodes the first record in `bytes` (a zsh history line or a JSON row
/// envelope) and masks it as `stored_record` describes. Returns the record and
/// its length in bytes.
fn decode_record(
    bytes: &[u8],
    provenance: &Value,
    mask_with: Option<&[PrivacyRule]>,
) -> Result<(Value, usize)> {
    let zsh = provenance["source"] == crate::ZSH_SOURCE;
    let (mut record, length) = if zsh {
        // A newline ends the record unless a backslash escapes it.
        let length = bytes
            .iter()
            .enumerate()
            .find(|(i, b)| **b == b'\n' && (*i == 0 || bytes[*i - 1] != b'\\'))
            .map(|(i, _)| i + 1)
            .ok_or(Error::Invalid("stored zsh record terminator unavailable"))?;
        let text =
            String::from_utf8(crate::zsh::unmetafy(&bytes[..length]).map_err(Error::Invalid)?)
                .map_err(|_| Error::Invalid("stored zsh record is not UTF-8"))?;
        (json!({ "unmetafied_record": text }), length)
    } else {
        let length = bytes
            .iter()
            .position(|b| *b == b'\n')
            .ok_or(Error::Invalid("stored row terminator unavailable"))?
            + 1;
        let record: Value = serde_json::from_slice(&bytes[..length])
            .map_err(|_| Error::Invalid("stored row envelope is invalid"))?;
        (record, length)
    };
    let rules: Vec<PrivacyRule> = serde_json::from_value(provenance["privacy_rules"].clone())
        .map_err(|_| Error::Invalid("stored privacy rules unavailable"))?;
    if let Some(current) = mask_with {
        apply_record_privacy(&mut record, &rules, current)?;
    }
    if zsh {
        // Added after masking: a rule matching hash hex or the label must not
        // rewrite or refuse them.
        record["original_blake3"] = json!(blake3::hash(&bytes[..length]).to_hex().to_string());
        record["representation"] = json!("zsh-unmetafied-utf8");
    }
    Ok((record, length))
}

// A stored record is source data, not an admitted payload. Mask it with the
// rules it was stored under, in order, then with each current rule not already
// among them. Duplicates within either list apply as often as they appear.
// Masking may change content but never identifiers, keys or hashes; a rule
// that would is refused.
fn apply_record_privacy(
    record: &mut Value,
    stored: &[PrivacyRule],
    current: &[PrivacyRule],
) -> Result<()> {
    let before = record.clone();
    let rules: Vec<_> = stored
        .iter()
        .chain(current.iter().filter(|r| !stored.contains(r)))
        .cloned()
        .collect();
    crate::atuin::mask(record, &rules)?;
    let unchanged = if matches!(before["table"].as_str(), Some("visits" | "downloads")) {
        chromium_keys_unchanged(&before, record)
    } else {
        let original = before
            .as_object()
            .ok_or(Error::Invalid("stored record is not an object"))?;
        original.iter().all(|(key, value)| {
            matches!(
                key.as_str(),
                "command" | "cwd" | "hostname" | "unmetafied_record"
            ) || record.get(key) == Some(value)
        })
    };
    if unchanged {
        Ok(())
    } else {
        Err(Error::ReplayRefused(
            "raw privacy projection changes an opaque selector",
        ))
    }
}
/// Chromium rows may be masked anywhere except provider keys, row coordinates
/// and typed BLOB or REAL envelopes, which must stay unchanged.
fn chromium_keys_unchanged(original: &Value, masked: &Value) -> bool {
    match original {
        Value::Object(fields) => fields.iter().all(|(key, value)| {
            if matches!(
                key.as_str(),
                "_from"
                    | "table"
                    | "coordinate"
                    | "id"
                    | "guid"
                    | "chain_index"
                    | "$blob"
                    | "sqlite_real_bits"
            ) {
                masked.get(key) == Some(value)
            } else {
                chromium_keys_unchanged(value, &masked[key])
            }
        }),
        Value::Array(items) => masked.as_array().is_some_and(|m| {
            items.len() == m.len()
                && items
                    .iter()
                    .zip(m)
                    .all(|(a, b)| chromium_keys_unchanged(a, b))
        }),
        Value::String(_) => true,
        _ => original == masked,
    }
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;
