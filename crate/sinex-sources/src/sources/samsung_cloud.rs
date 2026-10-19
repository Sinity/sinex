//! Samsung account data export (the GDPR "download your data" archive,
//! extracted): one directory per service (`samsungcloud_gk…`, `SmartThings_gk…`),
//! each holding CSV tables, PDFs describing them, and backup blobs.
//!
//! Every CSV is read, the first line its header:
//!
//! - A table with a `datauuid` column is Samsung Health data synced to the
//!   cloud (`Step Count`, `Health HRV`): each row is a `health.record` of
//!   provider `samsung-health-cloud`, its category the table's directory name.
//!   Times are `YYYY-MM-DD HH:MM`, UTC as Samsung Health stores them;
//!   `time_offset` is the local offset in milliseconds.
//! - Any other table (alarms, settings, backups, store and membership
//!   records) is an `account.record` of its service and table, every column
//!   kept, a JSON column decoded. `KEY`, `_id` or `datauuid` identifies the
//!   row, else a hash of it; `CREATETIME`, `TIMESTAMP` or `create_time` dates
//!   it.
//!
//! Two tables are not read here. Samsung Pass holds saved credentials, so its
//! privacy class is the operator's call (`zpa`) before anything is stored;
//! SMS is read by `communication.samsung-sms`.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::sources::samsung_health::typed;
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::kinds::account::AccountRecord;
use sinex_model::kinds::health::HealthRecord;
use sinex_model::{Class, Timestamp};
use std::path::{Path, PathBuf};

pub const SPEC: SourceSpec = SourceSpec {
    name: "account.samsung",
    driver: Driver::ExportArchive,
    consumer: "what the operator's Samsung account kept: health sync, settings, backups",
    class: Class::Open,
    parser: "samsung-account-export",
    parser_version: 1,
    event_types: &["health.record", "account.record"],
    framer: frame,
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum SamsungEvent {
        Health(HealthRecord),
        Account(AccountRecord),
    }
}

/// The file being read and its header.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    pub member: String,
    pub header: Vec<String>,
}

pub struct SamsungCloudParser;

impl Parser for SamsungCloudParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = SamsungEvent;

    fn parse(&self, _: &&[u8], _: &Carry) -> Result<(Vec<SamsungEvent>, Carry), RecordError> {
        Err(RecordError::new(
            "a Samsung export row needs its file's path",
        ))
    }

    fn parse_record(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<SamsungEvent>, Carry), RecordError> {
        let member = context.path().unwrap_or_default().to_owned();
        let cells = fields::csv_cells(record)?;
        if carry.member != member || carry.header.is_empty() {
            return Ok((
                vec![],
                Carry {
                    member,
                    header: cells,
                },
            ));
        }
        let (service, table) = names(Path::new(&member));
        let mut values = Map::new();
        for (name, cell) in carry.header.iter().zip(&cells) {
            if cell.is_empty() {
                continue;
            }
            values.insert(name.clone(), json_or_typed(cell));
        }
        let text = |name: &str| values.get(name).and_then(Value::as_str).map(str::to_owned);
        if carry.header.iter().any(|h| h == "datauuid") {
            let native_id = text("datauuid")
                .ok_or_else(|| RecordError::new(format!("{table} row without a datauuid")))?;
            let time = |name: &str| {
                values
                    .get(name)
                    .and_then(Value::as_str)
                    .map(minute_utc)
                    .transpose()
            };
            let start = match time("start_time")? {
                Some(t) => Some(t),
                None => time("day_time")?.or(time("create_time")?),
            };
            let end = time("end_time")?;
            let offset_minutes = values
                .get("time_offset")
                .and_then(Value::as_i64)
                .and_then(|ms| i32::try_from(ms / 60_000).ok());
            return Ok((
                vec![SamsungEvent::Health(HealthRecord {
                    provider: "samsung-health-cloud".into(),
                    category: table,
                    native_id,
                    start,
                    end,
                    offset_minutes,
                    values,
                    events: vec![],
                })],
                carry.clone(),
            ));
        }
        let native_id = ["KEY", "_id", "id"]
            .iter()
            .find_map(|k| values.get(*k).map(id_text))
            .unwrap_or_else(|| format!("blake3:{}", blake3::hash(record).to_hex()));
        let at = ["CREATETIME", "TIMESTAMP", "create_time", "createTime"]
            .iter()
            .find_map(|k| values.get(*k).and_then(time_of));
        Ok((
            vec![SamsungEvent::Account(AccountRecord {
                service,
                table,
                native_id,
                at,
                values,
            })],
            carry.clone(),
        ))
    }
}

fn id_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Epoch milliseconds, or a `YYYY-MM-DD HH:MM[:SS]` text read as UTC.
fn time_of(v: &Value) -> Option<Timestamp> {
    match v {
        Value::Number(n) => n.as_i64().and_then(|ms| fields::from_unix_ms(ms).ok()),
        Value::String(s) => minute_utc(s).ok(),
        _ => None,
    }
}

/// A cell that is a JSON object or array (some tables double-encode it, as a
/// JSON string holding JSON), decoded; otherwise typed like Samsung Health.
fn json_or_typed(cell: &str) -> Value {
    let trimmed = cell.trim();
    let structured =
        trimmed.starts_with('{') || trimmed.starts_with('[') || trimmed.starts_with('"');
    if structured && let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        if let Value::String(inner) = &v
            && let Ok(inner) = serde_json::from_str::<Value>(inner)
            && (inner.is_object() || inner.is_array())
        {
            return inner;
        }
        if v.is_object() || v.is_array() {
            return v;
        }
    }
    typed(cell)
}

/// `2026-03-12 17:23` (or with seconds), UTC.
fn minute_utc(raw: &str) -> Result<Timestamp, RecordError> {
    let raw = raw.trim();
    let civil = jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S%.f", raw)
        .or_else(|_| jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", raw))
        .or_else(|_| jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M", raw))
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))?;
    let z = civil
        .to_zoned(jiff::tz::TimeZone::UTC)
        .map_err(|e| RecordError::new(e.to_string()))?;
    Timestamp::from_unix_ns(z.timestamp().as_nanosecond())
        .map_err(|_| RecordError::new("time out of range"))
}

/// `…/samsungcloud_gk000066110879_20260329_access/Step Count/Step Count(3).csv`
/// → (`samsungcloud`, `Step Count`). The service is the export directory's
/// name before its `_gk` request number; the table is the CSV's directory, or
/// its file name when it sits at the top.
fn names(path: &Path) -> (String, String) {
    let parts: Vec<&str> = path.iter().filter_map(|p| p.to_str()).collect();
    let service_at = parts.iter().rposition(|p| p.contains("_gk"));
    let service = service_at
        .map(|i| parts[i].split("_gk").next().unwrap_or(parts[i]).to_owned())
        .unwrap_or_default();
    let table = match (service_at, parts.len()) {
        (Some(i), n) if n >= i + 3 => parts[n - 2].to_owned(),
        _ => path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_owned(),
    };
    (service, table)
}

/// Every CSV of the export but Samsung Pass and SMS (see the module note).
pub fn is_table(path: &Path) -> bool {
    let csv = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("csv"));
    let s = path.to_string_lossy();
    csv && !s.contains("Samsung Pass") && !s.contains("/SMS/")
}

pub fn source() -> Declared<SamsungCloudParser> {
    Declared {
        spec: SPEC,
        parser: SamsungCloudParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.to_ascii_lowercase().ends_with(".csv"),
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(path: &str, rows: &[&[u8]]) -> Vec<SamsungEvent> {
        let manifest = json!({"path": path});
        let context = RecordContext {
            manifest: &manifest,
            member: None,
        };
        let mut carry = Carry::default();
        let mut out = Vec::new();
        for row in rows {
            let (events, next) = SamsungCloudParser
                .parse_record(row, &carry, &context)
                .unwrap();
            out.extend(events);
            carry = next;
        }
        out
    }

    #[test]
    fn health_tables_become_health_records() {
        let out = run(
            "/x/samsungcloud_gk000066110879_20260329_access/Step Count/Step Count(3).csv",
            &[
                "\u{feff}create_time,time_offset,end_time,count,start_time,datauuid\n".as_bytes(),
                b"2026-03-12 17:23,3600000,2026-03-12 17:24,21,2026-03-12 17:23,e03a\n",
            ],
        );
        let SamsungEvent::Health(h) = &out[0] else {
            panic!()
        };
        assert_eq!(
            (h.category.as_str(), h.native_id.as_str()),
            ("Step Count", "e03a")
        );
        assert_eq!(h.offset_minutes, Some(60));
        assert_eq!(h.values["count"], json!(21));
        assert_eq!(h.start, fields::rfc3339("2026-03-12T17:23:00Z").ok());
    }

    #[test]
    fn other_tables_keep_every_column_with_json_decoded() {
        let out = run(
            "/x/samsungcloud_gk000066110887_20260329_access/Alarm/Alarm_1.csv",
            &[
                b"ITEM_DATA,TIMESTAMP,VALUE,KEY\n",
                br#""",1682494230881,"""{\""_id\"":\""1\"",\""active\"":\""0\""}""",abc"#,
            ],
        );
        let SamsungEvent::Account(a) = &out[0] else {
            panic!()
        };
        assert_eq!(
            (a.service.as_str(), a.table.as_str(), a.native_id.as_str()),
            ("samsungcloud", "Alarm", "abc")
        );
        assert_eq!(a.values["VALUE"]["active"], json!("0"));
        assert_eq!(a.at, fields::from_unix_ms(1_682_494_230_881).ok());
        assert!(!is_table(Path::new(
            "/x/samsungcloud_gk1/Samsung Pass/Samsung Pass.csv"
        )));
        assert!(is_table(Path::new("/x/SmartThings_gk1/a.csv")));
    }
}
