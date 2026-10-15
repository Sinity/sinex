//! Raw-event read subset. Unsupported syntax is refused before any database query.
use crate::{Error, ReadPool, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, Postgres, QueryBuilder, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

const EVENT_VIEW: &str = "jsonb_build_object('id',e.id,'source',e.source,'host',e.host,'payload',e.payload,'parents',e.source_event_ids,'run',e.run_id,'store','activity','time_suspect',e.time_suspect,'time_until',e.ts_orig_until,'written_by',e.written_by,'kind',e.event_type,'when',CASE WHEN e.ts_orig IS NULL THEN NULL ELSE to_char(e.ts_orig AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || lpad(COALESCE(e.ts_orig_tiebreak,0)::text,3,'0') || 'Z' END,'seen',to_char(e.ts_coided AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'written',to_char(e.ts_persisted AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'time_quality',e.ts_quality,'time_clock',e.ts_clock,'time_precision',e.ts_precision,'origin','material','material',e.source_material_id,'anchor',e.anchor_byte)";

#[derive(Debug, Clone)]
pub struct QueryError(pub &'static str);
#[derive(Debug)]
struct Filter {
    field: &'static str,
    op: char,
    value: Value,
}
fn parse(text: &str) -> std::result::Result<Vec<Filter>, QueryError> {
    if text.len() > 16_384 {
        return Err(QueryError("query exceeds 16 KiB"));
    }
    let mut rest = text
        .trim()
        .strip_prefix("events")
        .ok_or(QueryError(
            "only the raw events unit is available; commands requires fusion",
        ))?
        .trim();
    if rest.is_empty() {
        return Ok(vec![]);
    }
    rest = rest
        .strip_prefix("where ")
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
        rest = rest
            .strip_prefix("and ")
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    query: String,
    last_when: Option<String>,
    last_id: String,
}
impl ReadPool {
    pub async fn query(&self, text: &str, cursor: Option<&str>) -> Result<Value> {
        let filters = parse(text).map_err(|e| Error::Invalid(e.0))?;
        let digest = blake3::hash(text.as_bytes()).to_hex().to_string();
        let mut conn = self.0.acquire().await?;
        let mut tx = conn.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let current: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        let page_after = match cursor {
            None => None,
            Some(raw) => {
                if raw.len() > 1024 {
                    return Err(Error::Invalid("invalid page cursor"));
                }
                let c: Cursor =
                    serde_json::from_str(raw).map_err(|_| Error::Invalid("invalid page cursor"))?;
                if c.query != digest
                    || Uuid::parse_str(&c.last_id).is_err()
                    || c.last_when
                        .as_ref()
                        .is_some_and(|at| OffsetDateTime::parse(at, &Rfc3339).is_err())
                {
                    return Err(Error::Invalid("cursor does not match query/order"));
                }
                Some(c)
            }
        };
        let seq = current;
        let mut q: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "WITH matching AS (SELECT {EVENT_VIEW} AS event FROM core.events e WHERE e.record_state='present' AND e.commit_seq<="
        ));
        q.push_bind(seq).push(" AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<=").push_bind(seq).push(")");
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
        q.push(" ORDER BY event->>'when' NULLS LAST,event->>'id' LIMIT 101) SELECT (SELECT count(*) FROM matching) AS total,COALESCE((SELECT jsonb_agg(event) FROM page),'[]'::jsonb) AS rows");
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
                    query: digest,
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
        let source_status:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('input',i.id,'source',i.binding,'status',o.status,'reason',o.reason) FROM raw.inputs i LEFT JOIN LATERAL (SELECT status,reason FROM raw.observations WHERE input_id=i.id AND commit_seq<=$1 ORDER BY seq DESC LIMIT 1) o ON true").bind(seq).fetch_all(&mut *tx).await?;
        let complete = source_status.iter().all(|s| s["status"] == "completed");
        tx.commit().await?;
        let now = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| Error::Invalid("resolved clock encoding failed"))?;
        Ok(
            json!({"notice":sinex_model::DATA_NOTICE,"query":text,"unit":"events","total":total,"rows":rows,"page":{"size":100,"cursor":cursor},"truncated":false,"complete":complete,"source_status":source_status,"resolved":{"commit_seq":seq,"now":now,"zone":"UTC","logical_start":"04:00"},"read_as":format!("Raw live activity events matching {text}, ordered by original time then interpretation ID; shell fusion and coverage claims unavailable."),"excluded":{}}),
        )
    }
    pub async fn show(&self, id: &str) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let row:Option<Value>=sqlx::query_scalar(&format!("SELECT {EVENT_VIEW} || jsonb_build_object('kind',e.event_type,'archive',(SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id),'source_state',CASE WHEN e.record_state='removed' THEN 'removed at source' ELSE 'present' END) FROM core.events e WHERE e.id=$1")).bind(id).fetch_optional(&self.0).await?;
        Ok(row)
    }
    pub async fn trace(&self, id: &str, bytes_requested: bool) -> Result<Option<Value>> {
        let id = Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid event ID"))?;
        let row=sqlx::query("SELECT e.source_material_id,e.anchor_byte,e.offset_end,e.record_state,(SELECT to_jsonb(a) FROM audit.archived a WHERE a.id=e.id) AS archive,m.status,m.durable_bytes,b.blake3,m.parser_provenance FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE e.id=$1").bind(id).fetch_optional(&self.0).await?;
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
                json!({"event":id,"material":material,"anchor":anchor,"end":end,"record_state":state,"archive":archive,"representation":"canonical_sqlite_row"}),
            ));
        }
        let (record, _) = self.retained_record(material, anchor, Some(end)).await?;
        Ok(Some(
            json!({"event":id,"material":material,"anchor":anchor,"end":end,"representation":"canonical_sqlite_row","record_state":state,"archive":archive,"record":record,"privacy":"presentation masked; original raw rows retained as plain evidence"}),
        ))
    }
    pub(crate) async fn retained_record(
        &self,
        material: Uuid,
        anchor: i64,
        end: Option<i64>,
    ) -> Result<(Value, i64)> {
        let row = sqlx::query("SELECT m.durable_bytes,b.blake3,m.parser_provenance FROM raw.source_material_registry m LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.id=$1").bind(material).fetch_one(&self.0).await?;
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
        crate::atuin::mask(&mut record, &rules)?;
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
}
