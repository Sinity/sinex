use super::*;

pub(crate) fn text(row: &Map<String, Value>, name: &str) -> Result<String, RecordError> {
    match row.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        other => Err(RecordError::new(format!("{name} is not text: {other:?}"))),
    }
}

pub(crate) fn int(row: &Map<String, Value>, name: &str) -> Result<i64, RecordError> {
    row.get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| RecordError::new(format!("{name} is not an integer: {:?}", row.get(name))))
}

pub(crate) fn optional_bool(row: &Map<String, Value>, name: &str) -> Option<bool> {
    row.get(name).and_then(Value::as_bool)
}

pub(crate) fn from_unix_us(us: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(us) * 1000)
        .map_err(|_| RecordError::new(format!("time {us} µs out of range")))
}

/// Microseconds since 1601-01-01 (Chromium and Windows FILETIME/10).
pub(crate) fn from_webkit_us(us: i64) -> Result<Timestamp, RecordError> {
    from_unix_us(us - WEBKIT_EPOCH_OFFSET_US)
}

/// Microseconds between 1601-01-01 and 1970-01-01.
pub(crate) const WEBKIT_EPOCH_OFFSET_US: i64 = 11_644_473_600_000_000;

pub(crate) fn from_unix_ms(ms: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(i128::from(ms) * 1_000_000)
        .map_err(|_| RecordError::new(format!("time {ms} ms out of range")))
}

/// The cells of one framed CSV row; a byte-order mark on the first is dropped.
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

    /// Seconds since the Unix epoch, whole or fractional, as a JSON number or
    /// text. The written decimal is read exactly, so nanosecond digits survive.
    pub(crate) fn unix_time(&mut self, name: &str) -> Result<Option<Timestamp>, RecordError> {
        match self.take(name) {
            None => Ok(None),
            Some(Value::Number(n)) => unix_seconds(&n.to_string(), name).map(Some),
            Some(Value::String(s)) => unix_seconds(s.trim(), name).map(Some),
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

/// Decimal seconds since the Unix epoch (`1731000000.123456789`, `1.7e9`),
/// rounded to the nanosecond. A JSON number Sinex parsed as a float is read
/// from its shortest spelling, which keeps the digits the source wrote.
pub(crate) fn unix_seconds(text: &str, name: &str) -> Result<Timestamp, RecordError> {
    let nanos = query_language::number::Decimal::parse(text)
        .ok_or_else(|| RecordError::new(format!("{name} is not a time: {text:?}")))?
        .scaled_i128(9)
        .ok_or_else(|| RecordError::new(format!("{name} {text} is out of range")))?;
    Timestamp::from_unix_ns(nanos)
        .map_err(|_| RecordError::new(format!("{name} {text} is out of range")))
}

#[cfg(test)]
mod tests;
