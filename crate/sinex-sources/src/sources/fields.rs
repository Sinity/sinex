//! Reading typed fields out of a JSON record, with errors that name the field.
use crate::RecordError;
use serde_json::{Map, Value};
use sinex_model::Timestamp;

pub fn text(row: &Map<String, Value>, name: &str) -> Result<String, RecordError> {
    match row.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        other => Err(RecordError::new(format!("{name} is not text: {other:?}"))),
    }
}

/// Text, or `None` for a missing field, null or an empty string.
pub fn optional_text(row: &Map<String, Value>, name: &str) -> Option<String> {
    row.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

pub fn int(row: &Map<String, Value>, name: &str) -> Result<i64, RecordError> {
    row.get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| RecordError::new(format!("{name} is not an integer: {:?}", row.get(name))))
}

pub fn optional_bool(row: &Map<String, Value>, name: &str) -> Option<bool> {
    row.get(name).and_then(Value::as_bool)
}

/// The fields not in `known`, kept by name.
pub fn extra(row: &Map<String, Value>, known: &[&str]) -> Map<String, Value> {
    row.iter()
        .filter(|(k, _)| !known.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub fn from_unix_us(us: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(us) * 1000)
        .map_err(|_| RecordError::new(format!("time {us} µs out of range")))
}

/// Microseconds since 1601-01-01 (Chromium and Windows FILETIME/10).
pub fn from_webkit_us(us: i64) -> Result<Timestamp, RecordError> {
    from_unix_us(us - WEBKIT_EPOCH_OFFSET_US)
}

/// Microseconds between 1601-01-01 and 1970-01-01.
pub const WEBKIT_EPOCH_OFFSET_US: i64 = 11_644_473_600_000_000;

pub fn rfc3339(raw: &str) -> Result<Timestamp, RecordError> {
    time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .map(Timestamp)
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))
}
