//! Reading typed fields out of a JSON record, with errors that name the field.
use serde_json::{Map, Value};
use sinex_model::Timestamp;
use sinex_source_core::RecordError;

#[cfg(feature = "full")]
pub(crate) fn text(row: &Map<String, Value>, name: &str) -> Result<String, RecordError> {
    match row.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        other => Err(RecordError::new(format!("{name} is not text: {other:?}"))),
    }
}

/// Text, or `None` for a missing field, null or an empty string.
pub(crate) fn optional_text(row: &Map<String, Value>, name: &str) -> Option<String> {
    row.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[cfg(feature = "full")]
pub(crate) fn int(row: &Map<String, Value>, name: &str) -> Result<i64, RecordError> {
    row.get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| RecordError::new(format!("{name} is not an integer: {:?}", row.get(name))))
}

#[cfg(feature = "full")]
pub(crate) fn optional_bool(row: &Map<String, Value>, name: &str) -> Option<bool> {
    row.get(name).and_then(Value::as_bool)
}

/// The fields not in `known`, kept by name.
pub(crate) fn extra(row: &Map<String, Value>, known: &[&str]) -> Map<String, Value> {
    row.iter()
        .filter(|(k, _)| !known.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(feature = "full")]
pub(crate) fn from_unix_us(us: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(us) * 1000)
        .map_err(|_| RecordError::new(format!("time {us} µs out of range")))
}

/// Microseconds since 1601-01-01 (Chromium and Windows FILETIME/10).
#[cfg(feature = "full")]
pub(crate) fn from_webkit_us(us: i64) -> Result<Timestamp, RecordError> {
    from_unix_us(us - WEBKIT_EPOCH_OFFSET_US)
}

/// Microseconds between 1601-01-01 and 1970-01-01.
#[cfg(feature = "full")]
pub(crate) const WEBKIT_EPOCH_OFFSET_US: i64 = 11_644_473_600_000_000;

pub(crate) fn rfc3339(raw: &str) -> Result<Timestamp, RecordError> {
    time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .map(Timestamp)
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))
}

#[cfg(feature = "full")]
pub(crate) fn from_unix_ms(ms: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(ms) * 1_000_000)
        .map_err(|_| RecordError::new(format!("time {ms} ms out of range")))
}

/// The cells of one framed CSV row; a byte-order mark on the first is dropped.
#[cfg(feature = "full")]
pub(crate) fn csv_cells(record: &[u8]) -> Result<Vec<String>, RecordError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(record);
    let row = reader
        .records()
        .next()
        .ok_or_else(|| RecordError::new("empty CSV record"))?
        .map_err(|e| RecordError::new(format!("CSV: {e}")))?;
    let mut cells: Vec<String> = row.iter().map(str::to_owned).collect();
    if let Some(first) = cells.first_mut() {
        *first = first.trim_start_matches('\u{feff}').to_owned();
    }
    Ok(cells)
}

/// A day (`YYYY/MM/DD` or `YYYY-MM-DD`) at midnight UTC, or `None` if empty.
#[cfg(feature = "full")]
pub(crate) fn day(raw: &str) -> Result<Option<Timestamp>, RecordError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let mut parts = raw.split(['/', '-']).map(str::parse::<u16>);
    let (Some(Ok(y)), Some(Ok(m)), Some(Ok(d)), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(RecordError::new(format!("invalid date {raw:?}")));
    };
    let month = time::Month::try_from(m as u8)
        .map_err(|_| RecordError::new(format!("invalid month in {raw:?}")))?;
    time::Date::from_calendar_date(i32::from(y), month, d as u8)
        .map(|date| Some(Timestamp(date.midnight().assume_utc())))
        .map_err(|e| RecordError::new(format!("invalid date {raw:?}: {e}")))
}
