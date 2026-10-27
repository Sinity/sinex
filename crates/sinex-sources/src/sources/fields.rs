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

/// A JSON object read field by field into a typed payload. Each read takes
/// the field out, so what is left at the end are the fields the payload does
/// not know, kept by name. A missing field, `null` and an empty string are
/// unknown (`None`); a field of the wrong type is a record failure naming it.
/// Numbers written as strings are read as numbers.
pub(crate) struct FieldReader(Map<String, Value>);

impl FieldReader {
    pub(crate) fn new(object: Map<String, Value>) -> Self {
        Self(object)
    }

    fn take(&mut self, name: &str) -> Option<Value> {
        match self.0.remove(name) {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) if s.is_empty() => None,
            Some(v) => Some(v),
        }
    }

    pub(crate) fn text(&mut self, name: &str) -> Result<Option<String>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(other) => Err(RecordError::new(format!("{name} is not text: {other}"))),
        }
    }

    pub(crate) fn required_text(&mut self, name: &str) -> Result<String, RecordError> {
        self.text(name)?
            .ok_or_else(|| RecordError::new(format!("{name} is missing")))
    }

    pub(crate) fn int(&mut self, name: &str) -> Result<Option<i64>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::Number(n)) if n.as_i64().is_some() => Ok(n.as_i64()),
            Some(Value::String(s)) if s.trim().parse::<i64>().is_ok() => Ok(s.trim().parse().ok()),
            Some(other) => Err(RecordError::new(format!(
                "{name} is not an integer: {other}"
            ))),
        }
    }

    pub(crate) fn unsigned(&mut self, name: &str) -> Result<Option<u64>, RecordError> {
        match self.int(name)? {
            None => Ok(None),
            Some(n) => u64::try_from(n)
                .map(Some)
                .map_err(|_| RecordError::new(format!("{name} is negative: {n}"))),
        }
    }

    /// Seconds since the Unix epoch, whole or fractional.
    pub(crate) fn unix_time(&mut self, name: &str) -> Result<Option<Timestamp>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::Number(n)) => n
                .as_f64()
                .map(|secs| from_unix_secs_f64(secs, name))
                .transpose(),
            Some(Value::String(s)) => match s.trim().parse::<f64>() {
                Ok(secs) => from_unix_secs_f64(secs, name).map(Some),
                Err(_) => Err(RecordError::new(format!("{name} is not a time: {s:?}"))),
            },
            Some(other) => Err(RecordError::new(format!("{name} is not a time: {other}"))),
        }
    }

    /// An RFC 3339 time; a comma before the fraction (`date -Ins`) is read as a
    /// point.
    pub(crate) fn time(&mut self, name: &str) -> Result<Option<Timestamp>, RecordError> {
        self.text(name)?
            .map(|raw| rfc3339(&raw.replacen(',', ".", 1)))
            .transpose()
    }

    pub(crate) fn boolean(&mut self, name: &str) -> Result<Option<bool>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(b)),
            Some(other) => Err(RecordError::new(format!(
                "{name} is not true or false: {other}"
            ))),
        }
    }

    /// A list of strings; a missing list is empty.
    pub(crate) fn texts(&mut self, name: &str) -> Result<Vec<String>, RecordError> {
        match self.take(name) {
            None => Ok(Vec::new()),
            Some(Value::Array(items)) => items
                .into_iter()
                .map(|item| match item {
                    Value::String(s) => Ok(s),
                    other => Err(RecordError::new(format!(
                        "{name} holds a value that is not text: {other}"
                    ))),
                })
                .collect(),
            Some(other) => Err(RecordError::new(format!("{name} is not a list: {other}"))),
        }
    }

    /// Any JSON value, kept as written.
    pub(crate) fn value(&mut self, name: &str) -> Option<Value> {
        self.take(name)
    }

    /// An object, for a nested record read the same way.
    pub(crate) fn object(&mut self, name: &str) -> Result<Option<FieldReader>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::Object(object)) => Ok(Some(FieldReader(object))),
            Some(other) => Err(RecordError::new(format!(
                "{name} is not an object: {other}"
            ))),
        }
    }

    /// The fields no read took.
    pub(crate) fn rest(self) -> Map<String, Value> {
        self.0
    }
}

pub(crate) fn from_unix_secs_f64(secs: f64, name: &str) -> Result<Timestamp, RecordError> {
    // A float of seconds holds about microseconds at today's epoch; whole
    // seconds plus rounded microseconds avoid inventing nanoseconds.
    if !secs.is_finite() {
        return Err(RecordError::new(format!("{name} is not a time: {secs}")));
    }
    let whole = secs.floor();
    let micros = ((secs - whole) * 1e6).round() as i128;
    Timestamp::from_unix_ns(whole as i128 * 1_000_000_000 + micros * 1_000)
        .map_err(|_| RecordError::new(format!("{name} {secs} is out of range")))
}
