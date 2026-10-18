//! Chromium History rows as events. IDs and times come from the browser's own fields,
//! never from when Sinex read them.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub const SOURCE: &str = "browser.history.chromium";
pub const PARSER: &str = "chromium-history-v1";
pub const WINDOWS_EPOCH_US: i64 = 11_644_473_600_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub resolved: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub version: u8,
    pub epoch: Uuid,
    pub generation: Uuid,
    pub identity: FileIdentity,
    pub schema_version: i64,
    pub visits_sequence: i64,
    pub visits_through: i64,
    pub download_max_coordinate: i64,
    pub full_completed_at: String,
    pub full_due_at: String,
    pub snapshot_at: Option<String>,
    pub snapshot: Option<Uuid>,
    pub needs_full: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BrowserReadStart {
    pub input: Option<Uuid>,
    pub snapshot_seq: i64,
    pub checkpoint: Option<Checkpoint>,
    pub reconciliation_required: bool,
}
#[derive(Clone, Debug)]
pub struct Parsed {
    pub kind: &'static str,
    pub key: String,
    pub unix_us: Option<i64>,
    pub payload: Value,
    pub unfinished: bool,
}
fn integer(row: &Value, key: &str) -> Result<i64, &'static str> {
    row.get(key)
        .and_then(Value::as_i64)
        .ok_or("missing or invalid Chromium integer field")
}
fn text<'a>(row: &'a Value, key: &str) -> Result<&'a str, &'static str> {
    let value = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or("missing or invalid Chromium text field")?;
    if value.contains('\0') {
        return Err("NUL-containing Chromium text is not representable");
    }
    Ok(value)
}
pub fn unix_us(raw: i64) -> Result<i64, &'static str> {
    let value = raw
        .checked_sub(WINDOWS_EPOCH_US)
        .ok_or("Chromium clock overflow")?;
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1000)
        .map_err(|_| "Chromium clock out of range")?;
    Ok(value)
}
/// Parses one Chromium History row. Payloads keep the browser's raw values (clock,
/// transition, state) next to the derived ones.
pub fn parse(row: &Value, profile: &str, generation: Uuid) -> Result<Parsed, &'static str> {
    match row["table"].as_str() {
        Some("visits") => parse_visit(row, profile, generation),
        Some("downloads") => parse_download(row, profile, generation),
        _ => Err("unknown Chromium History record table"),
    }
}

fn parse_visit(row: &Value, profile: &str, generation: Uuid) -> Result<Parsed, &'static str> {
    let visit = &row["visit"];
    let url = &row["url"];
    let id = integer(visit, "id")?;
    if id <= 0 {
        return Err("Chromium visit ID must be positive");
    }
    if integer(visit, "url")? != integer(url, "id")? {
        return Err("Chromium visit URL reference unavailable");
    }
    let clock = integer(visit, "visit_time")?;
    if clock <= 0 {
        return Err("Chromium visit clock is missing");
    }
    let duration = integer(visit, "visit_duration")?;
    if duration < 0 {
        return Err("negative Chromium visit duration");
    }
    let transition = integer(visit, "transition")?;
    let redirect = (transition as u64 & 0xc000_0000) != 0;
    let title = match url.get("title") {
        None | Some(Value::Null) => Value::Null,
        Some(_) => json!(text(url, "title")?),
    };
    Ok(Parsed {
        kind: "page.visited",
        key: id.to_string(),
        unix_us: Some(unix_us(clock)?),
        unfinished: duration == 0,
        payload: json!({
            "profile": profile,
            "database_generation": generation,
            "visit_id": id,
            "url": text(url, "url")?,
            "title": title,
            "visit_time_us": clock,
            "duration_us": duration,
            "transition": transition,
            "redirect": redirect,
            "from_visit": integer(visit, "from_visit")?,
            "referrer_url": row.get("referrer_url").cloned().unwrap_or(Value::Null),
            "referrer_basis": row.get("referrer_basis").cloned().unwrap_or(Value::Null),
            "duration_basis": "browser recorded elapsed duration; zero is not proof of an open tab; attention is unknown",
        }),
    })
}

fn parse_download(row: &Value, profile: &str, generation: Uuid) -> Result<Parsed, &'static str> {
    let download = &row["download"];
    let id = integer(download, "id")?;
    if id <= 0 {
        return Err("Chromium download coordinate must be positive");
    }
    let original_guid = text(download, "guid")?;
    let guid = Uuid::parse_str(original_guid)
        .map_err(|_| "Chromium download GUID is missing or invalid")?;
    if !guid
        .hyphenated()
        .to_string()
        .eq_ignore_ascii_case(original_guid)
    {
        return Err("Chromium download GUID must use provider hyphenated form");
    }
    if guid.is_nil() {
        return Err("Chromium download GUID cannot be nil");
    }
    let state = integer(download, "state")?;
    let start = integer(download, "start_time")?;
    let end = integer(download, "end_time")?;
    let received = integer(download, "received_bytes")?;
    let total = integer(download, "total_bytes")?;
    let chain = row["url_chain"]
        .as_array()
        .ok_or("Chromium download URL chain unavailable")?;
    let Some(last) = chain.last() else {
        return Err("Chromium download URL chain is empty");
    };
    for (index, item) in chain.iter().enumerate() {
        if integer(item, "id")? != id || integer(item, "chain_index")? != index as i64 {
            return Err("Chromium download URL chain is incomplete");
        }
        text(item, "url")?;
    }
    let state_label = match state {
        0 => "in_progress",
        1 => "complete",
        2 => "cancelled",
        3 => "legacy_bug_140687",
        4 => "interrupted",
        _ => "unknown_provider_state",
    };
    let optional_us = |raw: i64| {
        if raw == 0 {
            Ok(None)
        } else {
            unix_us(raw).map(Some)
        }
    };
    Ok(Parsed {
        kind: "download.observed",
        key: guid.to_string(),
        unix_us: optional_us(start)?,
        unfinished: !matches!(state, 1 | 2),
        payload: json!({
            "profile": profile,
            "database_generation": generation,
            "guid": original_guid,
            "download_id": id,
            "url_chain": chain.iter().map(|r| r["url"].clone()).collect::<Vec<_>>(),
            "url": last["url"],
            "target_path": text(download, "target_path")?,
            "current_path": text(download, "current_path")?,
            "start_time_us": start,
            "end_time_us": end,
            "end_unix_us": optional_us(end)?,
            "state": state,
            "state_label": state_label,
            "received_bytes": received,
            "total_bytes": total,
            "file_presence": "unknown; downloaded files were not read",
        }),
    })
}

pub fn payload_schema(download: bool) -> Value {
    let required = if download {
        vec![
            "profile",
            "database_generation",
            "guid",
            "download_id",
            "url_chain",
            "url",
            "state",
            "state_label",
            "file_presence",
        ]
    } else {
        vec![
            "profile",
            "database_generation",
            "visit_id",
            "url",
            "title",
            "visit_time_us",
            "duration_us",
            "transition",
            "redirect",
        ]
    };
    json!({
        "type": "object",
        "required": required,
        "properties": {
            "profile": {"type": "string"},
            "database_generation": {"type": "string"},
            "url": {"type": "string"},
        },
    })
}

#[cfg(test)]
#[path = "browser_tests.rs"]
mod tests;
