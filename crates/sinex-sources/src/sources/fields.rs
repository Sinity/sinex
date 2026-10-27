//! Reading typed fields out of a JSON record, with errors that name the field.
use serde_json::{Map, Value};
use sinex_model::Timestamp;
use sinex_source_core::RecordError;

/// Text, or `None` for a missing field, null or an empty string.
pub(crate) fn optional_text(row: &Map<String, Value>, name: &str) -> Option<String> {
    row.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// The fields not in `known`, kept by name.
pub(crate) fn extra(row: &Map<String, Value>, known: &[&str]) -> Map<String, Value> {
    row.iter()
        .filter(|(k, _)| !known.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub(crate) fn rfc3339(raw: &str) -> Result<Timestamp, RecordError> {
    time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .map(Timestamp)
        .map_err(|e| RecordError::new(format!("invalid time {raw:?}: {e}")))
}

#[cfg(feature = "full")]
mod full;
#[cfg(feature = "full")]
pub(crate) use full::*;
