//! Raw-event read subset. Unsupported syntax is refused before any database query.
use crate::{Error, ReadPool, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, Postgres, QueryBuilder, Row};
use time::{Duration, OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub(crate) const EVENT_VIEW: &str = "jsonb_build_object('id',e.id,'source',e.source,'host',e.host,'payload',e.payload,'parents',e.source_event_ids,'run',e.run_id,'operation',e.created_by_operation,'store','activity','time_suspect',e.time_suspect,'time_until',e.ts_orig_until,'written_by',e.written_by,'kind',e.event_type,'when',CASE WHEN e.ts_orig IS NULL THEN NULL ELSE to_char(e.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(e.ts_orig_tiebreak,0)::text,3,'0') || 'Z' END,'seen',to_char(e.ts_coided AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'written',to_char(e.ts_persisted AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'time_quality',e.ts_quality,'time_clock',e.ts_clock,'time_precision',e.ts_precision,'origin',CASE WHEN e.source_material_id IS NULL THEN 'derived' ELSE 'material' END,'material',e.source_material_id,'anchor',e.anchor_byte)";

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
        let end = rest
            .find(|c: char| c.is_whitespace())
            .ok_or(QueryError("missing field operator"))?;
        let field = match &rest[..end] {
            "source" => "source",
            "kind" => "kind",
            "payload.command" => "payload.command",
            "payload.exit_code" => "payload.exit_code",
            "when" | "ts_orig" => "when",
            "id" => "id",
            _ => {
                return Err(QueryError(
                    "supported fields: source, kind, payload.command, payload.exit_code, when, id",
                ));
            }
        };
        rest = rest[end..].trim_start();
        let op = rest.chars().next().ok_or(QueryError("missing operator"))?;
        if op != '=' && !(op == '~' && field == "payload.command") {
            return Err(QueryError(
                "supported operators: =; payload.command also supports ~",
            ));
        }
        rest = rest[op.len_utf8()..].trim_start();
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
            (
                if field == "payload.exit_code" {
                    json!(
                        rest[..n]
                            .parse::<i64>()
                            .map_err(|_| QueryError("exit_code must be an integer"))?
                    )
                } else {
                    json!(&rest[..n])
                },
                n,
            )
        };
        if field == "payload.exit_code" && !value.is_i64() {
            return Err(QueryError("exit_code must be an integer"));
        }
        if field == "when" {
            OffsetDateTime::parse(
                value.as_str().ok_or(QueryError("when must be RFC3339"))?,
                &Rfc3339,
            )
            .map_err(|_| QueryError("when must be RFC3339"))?;
        }
        if field == "id" {
            Uuid::parse_str(value.as_str().ok_or(QueryError("invalid event ID"))?)
                .map_err(|_| QueryError("invalid event ID"))?;
        }
        filters.push(Filter { field, op, value });
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
pub fn validate_query(text: &str) -> std::result::Result<(), QueryError> {
    parse(text).map(|_| ())
}
// Bump this descriptor when query meaning, schema reads, defaults or key ordering change.
const CURSOR_VERSION: u32 = 1;
const CURSOR_ORDER: &str = "raw-present-activity-v1:when-ns-asc-null-last,id-asc";
const CURSOR_LIFETIME: Duration = Duration::hours(24);
const CHANGE_NOTICE: &str = "The commit position changed since the first page. Each page reads current state: removals apply; additions before the last key can be absent and additions after it can appear. A row whose ordering key moves can be absent or repeated. This traversal is not one fixed snapshot. A commit change does not establish that matching rows changed.";

fn question(filters: &[Filter]) -> Result<String> {
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
    // Conjunction order, repeated terms, quoting and whitespace do not change the question.
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
    #[serde(deserialize_with = "required_last_when")]
    last_when: Option<String>,
    last_id: String,
}
fn required_last_when<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
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
            return Err(Error::Invalid(
                "page cursor expired; issue a fresh query explicitly",
            ));
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
                // SQL emits fixed-width UTC nanoseconds; a noncanonical key would sort differently.
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
// Dropping a fetch alone can leave PostgreSQL executing while SQLx waits to
// recycle the connection. Close this exact transport instead. PostgreSQL's
// transaction-local connection checks abort its query on observed socket loss.
// No PID-targeted signal can accidentally reach a later, unrelated session.
pub(crate) struct ReadLease(pub(crate) Option<sqlx::pool::PoolConnection<Postgres>>);
impl ReadLease {
    pub(crate) fn connection(&mut self) -> &mut sqlx::PgConnection {
        &mut *self.0.as_mut().expect("owned read connection")
    }
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
        match tokio::time::timeout(budget, self.query_page(text, cursor, budget)).await {
            Err(_) => Err(Error::Deadline),
            Ok(Err(Error::Database(sqlx::Error::Database(error))))
                if error.code().as_deref() == Some("57014") =>
            {
                Err(Error::Deadline)
            }
            Ok(result) => result,
        }
    }
    async fn query_page(
        &self,
        text: &str,
        cursor: Option<&str>,
        budget: std::time::Duration,
    ) -> Result<Value> {
        let filters = parse(text).map_err(|e| Error::Invalid(e.0))?;
        let digest = question(&filters)?;
        let clock = OffsetDateTime::now_utc();
        let mut lease = ReadLease(Some(self.0.acquire().await?));
        let mut tx = lease
            .0
            .as_mut()
            .expect("owned read connection")
            .begin()
            .await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT set_config('statement_timeout',$1,true), set_config('client_connection_check_interval','100ms',true)")
            .bind(format!("{}ms", budget.as_millis().max(1)))
            .execute(&mut *tx)
            .await?;
        let current: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        let page_after = cursor
            .map(|raw| Cursor::decode(raw, &digest, current, clock))
            .transpose()?;
        let first_commit_seq = page_after.as_ref().map_or(current, |c| c.first_commit_seq);
        let now = match &page_after {
            Some(c) => c.now.clone(),
            None => clock
                .format(&Rfc3339)
                .map_err(|_| Error::Invalid("resolved clock encoding failed"))?,
        };
        let expires_at = match &page_after {
            Some(c) => c.expires_at.clone(),
            None => (clock + CURSOR_LIFETIME)
                .format(&Rfc3339)
                .map_err(|_| Error::Invalid("cursor expiry encoding failed"))?,
        };
        let seq = current;
        let canonical_scope = !filters.iter().any(|f| {
            (f.field == "kind" && f.value.as_str() != Some("command.canonical"))
                || (f.field == "source" && f.value.as_str() != Some("fusion.shell-command"))
        });
        let mut q: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "WITH matching AS (SELECT {EVENT_VIEW} AS event FROM core.events e WHERE e.record_state='present' AND e.commit_seq<="
        ));
        q.push_bind(seq).push(" AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=").push_bind(seq).push(")");
        q.push(" AND (e.event_type<>'command.canonical' OR NOT EXISTS(SELECT 1 FROM unnest(e.source_event_ids) parent(id) WHERE NOT EXISTS(SELECT 1 FROM core.live_events raw WHERE raw.id=parent.id AND raw.record_state='present')))");
        for f in filters {
            q.push(" AND ");
            match f.field {
                "source" => {
                    q.push("e.source=")
                        .push_bind(f.value.as_str().unwrap().to_string());
                }
                "kind" => {
                    q.push("e.event_type=")
                        .push_bind(f.value.as_str().unwrap().to_string());
                }
                "payload.command" => {
                    if f.op == '~' {
                        q.push("strpos(e.payload->>'command',")
                            .push_bind(f.value.as_str().unwrap().to_string())
                            .push(")>0");
                    } else {
                        q.push("e.payload->>'command'=")
                            .push_bind(f.value.as_str().unwrap().to_string());
                    }
                }
                "payload.exit_code" => {
                    q.push("e.payload->'exit_code'=").push_bind(f.value);
                }
                "when" => {
                    let time = OffsetDateTime::parse(f.value.as_str().unwrap(), &Rfc3339)
                        .map_err(|_| Error::Invalid("invalid original time"))?;
                    let at = time
                        .replace_nanosecond(time.nanosecond() / 1000 * 1000)
                        .map_err(|_| Error::Invalid("invalid original time"))?
                        .format(&Rfc3339)
                        .map_err(|_| Error::Invalid("invalid original time"))?;
                    q.push("(e.ts_orig=")
                        .push_bind(at)
                        .push("::text::timestamptz AND COALESCE(e.ts_orig_tiebreak,0)=")
                        .push_bind((time.nanosecond() % 1000) as i32)
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
        }
        // Each page is a fresh read. The keyset binds query, time order and UUID tie-breaks;
        // current removals and new committed readings apply on the next request.
        q.push("), page AS (SELECT event FROM matching");
        if let Some(c) = page_after {
            q.push(" WHERE ");
            match c.last_when {
                Some(at) => {
                    q.push("((event->>'when') IS NULL OR (event->>'when',event->>'id')>(")
                        .push_bind(at)
                        .push(",")
                        .push_bind(c.last_id)
                        .push("))");
                }
                None => {
                    q.push("(event->>'when') IS NULL AND (event->>'id')>")
                        .push_bind(c.last_id);
                }
            }
        }
        q.push(" ORDER BY event->>'when' NULLS LAST,event->>'id' LIMIT 101) SELECT (SELECT count(*) FROM matching) AS total,COALESCE((SELECT jsonb_agg(event ORDER BY event->>'when' NULLS LAST,event->>'id') FROM page),'[]'::jsonb) AS rows");
        let row = q.build().fetch_one(&mut *tx).await?;
        let total: i64 = row.try_get("total")?;
        let mut rows: Value = row.try_get("rows")?;
        let page = rows
            .as_array_mut()
            .ok_or(Error::Invalid("invalid database page"))?;
        let has_more = page.len() > 100;
        if has_more {
            page.pop();
        }
        let cursor = if has_more {
            let last = page.last().ok_or(Error::Invalid("invalid nonempty page"))?;
            Some(
                serde_json::to_string(&Cursor {
                    version: CURSOR_VERSION,
                    order: CURSOR_ORDER.to_string(),
                    query: digest,
                    now: now.clone(),
                    zone: "UTC".to_string(),
                    logical_start: "04:00".to_string(),
                    first_commit_seq,
                    expires_at: expires_at.clone(),
                    last_when: last["when"].as_str().map(str::to_string),
                    last_id: last["id"]
                        .as_str()
                        .ok_or(Error::Invalid("invalid event identity"))?
                        .to_string(),
                })
                .map_err(|_| Error::Invalid("cursor encoding failed"))?,
            )
        } else {
            None
        };
        let source_status:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('input',i.id,'source',i.binding,'status',o.status,'reason',o.reason,'record_errors',o.record_errors,'retained_record_errors',CASE WHEN i.binding='terminal.zsh-history' THEN coalesce((SELECT (c.position->>'record_errors')::bigint FROM raw.source_cursors c WHERE c.binding=i.binding AND c.input=i.id::text),0) ELSE 0 END) FROM raw.inputs i LEFT JOIN LATERAL (SELECT status,reason,record_errors FROM raw.observations WHERE input_id=i.id AND commit_seq<=$1 ORDER BY seq DESC LIMIT 1) o ON true").bind(seq).fetch_all(&mut *tx).await?;
        let fusion:Value=sqlx::query_scalar("SELECT to_jsonb(p)||jsonb_build_object('current_now_settled',NOT EXISTS(SELECT 1 FROM derivation.shell_dirty),'enabled',EXISTS(SELECT 1 FROM core.producer_versions WHERE name='fusion.shell-command'),'scope','whole-input baseline; component-targeted efficiency unqualified') FROM derivation.shell_progress p WHERE one").fetch_one(&mut *tx).await?;
        let complete = source_status.iter().all(|s| {
            s["status"] == "completed"
                && (s["source"] != crate::ZSH_SOURCE
                    || s["retained_record_errors"].as_i64() == Some(0))
        }) && (!canonical_scope
            || fusion["enabled"] != true
            || (fusion["current_now_settled"] == true
                && fusion["phase"] == "settled"
                && fusion["failure"].is_null()));
        tx.commit().await?;
        // Only a fully committed read may return its connection to the pool.
        drop(lease.0.take());
        Ok(
            json!({"notice":sinex_model::DATA_NOTICE,"query":text,"unit":"events","total":total,"rows":rows,"page":{"size":100,"cursor":cursor,"cursor_version":CURSOR_VERSION,"first_commit_seq":first_commit_seq,"expires_at":expires_at,"changed_since_first_page":seq != first_commit_seq,"change_notice":if seq != first_commit_seq {Some(CHANGE_NOTICE)} else {None}},"truncated":false,"complete":complete,"fusion":fusion,"source_status":source_status,"resolved":{"commit_seq":seq,"now":now,"zone":"UTC","logical_start":"04:00"},"read_as":format!("Raw live activity events matching {text}, ordered by original time then interpretation ID; source coverage is unknown; canonical rows are policy-selected groups with their own parent evidence."),"excluded":{}}),
        )
    }
    pub async fn show(&self, id: &str) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let mut lease = self.read_lease().await?;
        let row:Option<Value>=sqlx::query_scalar(&format!("SELECT {EVENT_VIEW} || jsonb_build_object('kind',e.event_type,'archive',(SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id),'source_state',CASE WHEN e.record_state='removed' THEN 'removed at source' ELSE 'present' END) FROM core.events e WHERE e.id=$1")).bind(id).fetch_optional(lease.connection()).await?;
        lease.finish();
        Ok(row)
    }
    pub async fn trace(&self, id: &str, bytes_requested: bool) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let mut lease = self.read_lease().await?;
        let parents: Option<Vec<Uuid>> =
            sqlx::query_scalar("SELECT source_event_ids FROM core.events WHERE id=$1")
                .bind(id)
                .fetch_optional(lease.connection())
                .await?
                .flatten();
        lease.finish();
        if let Some(parents) = parents {
            let mut evidence = Vec::new();
            for parent in parents {
                evidence.push(
                    self.trace_material(parent, bytes_requested)
                        .await?
                        .ok_or(Error::Invalid("canonical parent evidence unavailable"))?,
                );
            }
            return Ok(Some(
                json!({"event":id,"representation":"canonical_shell_group","parents":evidence,"archive":self.show(&id.to_string()).await?.and_then(|e|e.get("archive").cloned())}),
            ));
        }
        self.trace_material(id, bytes_requested).await
    }
    async fn trace_material(&self, id: Uuid, bytes_requested: bool) -> Result<Option<Value>> {
        let mut lease = self.read_lease().await?;
        let row=sqlx::query("SELECT e.source_material_id,e.anchor_byte,e.offset_end,e.record_state,(SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id) AS archive,m.status,m.durable_bytes,b.blake3,m.parser_provenance FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE e.id=$1").bind(id).fetch_optional(lease.connection()).await?;
        lease.finish();
        let Some(row) = row else { return Ok(None) };
        let material: Uuid = row.try_get("source_material_id")?;
        let anchor: i64 = row.try_get("anchor_byte")?;
        let end: Option<i64> = row.try_get("offset_end")?;
        let end = end.ok_or(Error::Invalid("event has no retained record range"))?;
        let durable: i64 = row.try_get("durable_bytes")?;
        if anchor < 0
            || end < anchor
            || end > durable
            || end - anchor > crate::MAX_STEP_BYTES as i64
        {
            return Err(Error::Invalid("retained record range unavailable"));
        }
        let state: String = row.try_get("record_state")?;
        let archive: Option<Value> = row.try_get("archive")?;
        if !bytes_requested {
            return Ok(Some(
                json!({"event":id,"material":material,"anchor":anchor,"end":end,"record_state":state,"archive":archive,"representation":if row.try_get::<Value,_>("parser_provenance")?["source"]==crate::ZSH_SOURCE {"original_zsh_history"} else {"canonical_sqlite_row"}}),
            ));
        }
        let (record, _) = self.retained_record(material, anchor, Some(end)).await?;
        Ok(Some(
            json!({"event":id,"material":material,"anchor":anchor,"end":end,"representation":if row.try_get::<Value,_>("parser_provenance")?["source"]==crate::ZSH_SOURCE {"original_zsh_history"} else {"canonical_sqlite_row"},"record_state":state,"archive":archive,"record":record,"privacy":"presentation masked; original raw rows retained as plain evidence"}),
        ))
    }
    pub(crate) async fn retained_record(
        &self,
        material: Uuid,
        anchor: i64,
        end: Option<i64>,
    ) -> Result<(Value, i64)> {
        self.retained_record_raw(material, anchor, end, true).await
    }
    pub(crate) async fn retained_record_raw(
        &self,
        material: Uuid,
        anchor: i64,
        end: Option<i64>,
        presentation: bool,
    ) -> Result<(Value, i64)> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query("SELECT m.durable_bytes,b.blake3,m.parser_provenance FROM raw.source_material_registry m LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1").bind(material).fetch_one(lease.connection()).await?;
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
            return Err(Error::Invalid("retained record range unavailable"));
        }
        let blob: Option<String> = row.try_get("blake3")?;
        let blobs = self.1.clone();
        let root = self.2.clone();
        let bytes = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
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
        .await??;
        let provenance: Value = row.try_get("parser_provenance")?;
        if provenance["source"] == crate::ZSH_SOURCE {
            let length = bytes
                .iter()
                .enumerate()
                .find(|(i, b)| **b == b'\n' && (*i == 0 || bytes[*i - 1] != b'\\'))
                .map(|(i, _)| i + 1)
                .ok_or(Error::Invalid("retained zsh record terminator unavailable"))?;
            if expected_end.is_some_and(|end| end != anchor + length as i64) {
                return Err(Error::Invalid("retained zsh range spans other data"));
            }
            let text =
                String::from_utf8(crate::zsh::unmetafy(&bytes[..length]).map_err(Error::Invalid)?)
                    .map_err(|_| Error::Invalid("retained zsh UTF-8 unavailable"))?;
            let mut record = json!({"unmetafied_record":text,"original_blake3":blake3::hash(&bytes[..length]).to_hex().to_string(),"representation":"zsh-unmetafied-utf8","byte_presentation":"original bytes retained; encoded bytes are not exposed through masked output"});
            let rules: Vec<crate::PrivacyRule> =
                serde_json::from_value(provenance["privacy_rules"].clone())
                    .map_err(|_| Error::Invalid("retained privacy provenance unavailable"))?;
            if presentation {
                crate::present(&mut record, &rules)?;
            }
            return Ok((record, anchor + length as i64));
        }
        let length = bytes
            .iter()
            .position(|b| *b == b'\n')
            .ok_or(Error::Invalid("retained row terminator unavailable"))?
            + 1;
        if expected_end.is_some_and(|end| end != anchor + length as i64) {
            return Err(Error::Invalid("retained record range spans other data"));
        }
        let mut record: Value = serde_json::from_slice(&bytes[..length])
            .map_err(|_| Error::Invalid("retained row envelope is invalid"))?;
        let provenance: Value = row.try_get("parser_provenance")?;
        let rules: Vec<crate::PrivacyRule> =
            serde_json::from_value(provenance["privacy_rules"].clone())
                .map_err(|_| Error::Invalid("retained privacy provenance unavailable"))?;
        if presentation {
            crate::atuin::mask(&mut record, &rules)?;
        }
        Ok((record, anchor + length as i64))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parsing_keeps_literal_conjunctions_and_refuses_unknown_surfaces() {
        assert!(
            parse(r#"events where payload.command = "echo a and b" and payload.exit_code = 0"#)
                .is_ok()
        );
        assert!(parse("events where kind = x;DROP").is_ok()); // A literal value is bound data.
        for bad in [
            "commands",
            "events where typo = 0",
            "events where payload.exit_code = \"0\"",
            "events where when = no",
            "events where source = x or kind = x",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
    #[test]
    fn question_is_structural_and_preserves_exact_values() {
        let digest = |text| question(&parse(text).unwrap()).unwrap();
        assert_eq!(
            digest("events where source = x and kind = y"),
            digest(" events where kind = \"y\" and source = \"x\" and source = x ")
        );
        assert_eq!(
            digest("events where ts_orig = 2023-11-14T22:13:20.123456789Z"),
            digest("events where when = 2023-11-14T23:13:20.123456789+01:00")
        );
        assert_ne!(
            digest("events where when = 2023-11-14T22:13:20.123456789Z"),
            digest("events where when = 2023-11-14T22:13:20.123456788Z")
        );
        assert_ne!(
            digest("events where payload.command ~ x"),
            digest("events where payload.command = x")
        );
        assert_ne!(
            digest("events where source = x"),
            digest("events where source = X")
        );
    }

    #[test]
    fn cursor_binds_context_and_accepts_unknown_time_without_an_old_codec() {
        let clock = OffsetDateTime::parse("2026-10-15T12:00:00Z", &Rfc3339).unwrap();
        let cursor = Cursor {
            version: CURSOR_VERSION,
            order: CURSOR_ORDER.into(),
            query: "question".into(),
            now: clock.format(&Rfc3339).unwrap(),
            zone: "UTC".into(),
            logical_start: "04:00".into(),
            first_commit_seq: 5,
            expires_at: (clock + CURSOR_LIFETIME).format(&Rfc3339).unwrap(),
            last_when: None,
            last_id: "019a54a0-0000-7000-8000-000000000001".into(),
        };
        let raw = serde_json::to_string(&cursor).unwrap();
        assert!(Cursor::decode(&raw, "question", 6, clock).is_ok());
        assert!(Cursor::decode(&raw, "different", 6, clock).is_err());
        assert!(Cursor::decode(&raw, "question", 4, clock).is_err());
        assert!(Cursor::decode(&raw, "question", 6, clock + CURSOR_LIFETIME).is_err());
        assert!(Cursor::decode(&raw, "question", 6, clock - Duration::seconds(1)).is_err());
        let valid = serde_json::to_value(&cursor).unwrap();
        for (field, value) in [
            ("version", json!(2)),
            ("zone", json!("Europe/Warsaw")),
            ("logical_start", json!("00:00")),
            ("first_commit_seq", json!(-1)),
            ("order", json!("other")),
            ("last_id", json!("bad")),
            ("last_when", json!("2023-11-14T22:13:20Z")),
            ("expires_at", json!("2026-10-18T12:00:00Z")),
        ] {
            let mut changed = valid.clone();
            changed[field] = value;
            assert!(
                Cursor::decode(&changed.to_string(), "question", 6, clock).is_err(),
                "{field}"
            );
        }
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove("last_when");
        assert!(Cursor::decode(&missing.to_string(), "question", 6, clock).is_err());
        let mut known = valid.clone();
        known["last_when"] = json!("2023-11-14T22:13:20.123456789Z");
        assert!(Cursor::decode(&known.to_string(), "question", 6, clock).is_ok());
        assert!(Cursor::decode(r#"{"query":"question","last_when":null,"last_id":"019a54a0-0000-7000-8000-000000000001"}"#, "question", 6, clock).is_err());
    }
}
