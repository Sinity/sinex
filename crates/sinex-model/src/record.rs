//! Stable record citations shared by derivations and their input readers.
use crate::{Event, Origin};
use serde_json::Value;

pub const NATIVE_ID: &str = "native_id";

/// A stable name for the record an event was read from: `<type>:<native id>`
/// when the payload carries the source's own ID; for other captured events,
/// `<source>:<type>:<equivalence key>` when the record has a key, so a re-read
/// into a new material keeps it, and its record location otherwise;
/// `<type>:<equivalence key>` for a derived event, whose ID changes with
/// every recompute. It survives a replay, unlike the event ID.
pub fn identity(event: &Event) -> String {
    let location = match &event.origin {
        Origin::Record {
            location,
            output_index,
        } => Some((
            location.material.to_string(),
            location.anchor,
            *output_index,
        )),
        Origin::Derived { .. } => None,
    };
    citation(
        &event.event_type,
        &event.source,
        &event.payload,
        location,
        event.equivalence_key.as_ref().map(|key| key.0.as_str()),
        &event.id.to_string(),
    )
}

/// [`identity`] from an event's parts, for callers that read events
/// as rows: its type, source and payload, its record location (material,
/// anchor, output) when captured, its equivalence key, and its ID.
pub fn citation(
    event_type: &str,
    source: &str,
    payload: &Value,
    location: Option<(String, u64, u64)>,
    equivalence_key: Option<&str>,
    id: &str,
) -> String {
    if let Some(native) = payload.get(NATIVE_ID).and_then(Value::as_str) {
        return format!("{event_type}:{native}");
    }
    match (location, equivalence_key) {
        (Some(_), Some(key)) => format!("{source}:{event_type}:{key}"),
        (Some((material, anchor, output_index)), None) => {
            format!("{material}:{anchor}:{output_index}")
        }
        (None, Some(key)) => format!("{event_type}:{key}"),
        (None, None) => id.to_owned(),
    }
}

/// The captured record key used for grouping observations and stable ties.
pub fn stable(event: &Event) -> String {
    let location = match &event.origin {
        Origin::Record {
            location,
            output_index,
        } => Some((
            location.material.to_string(),
            location.anchor,
            *output_index,
        )),
        Origin::Derived { .. } => None,
    };
    stable_from_parts(
        &event.event_type,
        &event.source,
        &event.payload,
        location,
        event.equivalence_key.as_ref().map(|key| key.0.as_str()),
        &event.id.to_string(),
    )
}

/// The same key for a narrow database observation, without constructing a
/// partial Event or changing the source record's citation.
pub fn stable_from_parts(
    event_type: &str,
    source: &str,
    payload: &Value,
    location: Option<(String, u64, u64)>,
    equivalence_key: Option<&str>,
    id: &str,
) -> String {
    let key = equivalence_key
        .map(str::to_owned)
        .unwrap_or_else(|| citation(event_type, source, payload, location, None, id));
    serde_json::to_string(&(source, event_type, key)).expect("record identity serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn observation_keys_keep_the_existing_captured_identity_and_tuple_encoding() {
        let location = Some(("material".into(), 7, 2));
        assert_eq!(
            stable_from_parts(
                "t",
                "s",
                &json!({"native_id":"n"}),
                location.clone(),
                Some("k"),
                "id"
            ),
            "[\"s\",\"t\",\"k\"]"
        );
        assert_eq!(
            stable_from_parts(
                "t",
                "s",
                &json!({"native_id":"n"}),
                location.clone(),
                None,
                "id"
            ),
            "[\"s\",\"t\",\"t:n\"]"
        );
        assert_eq!(
            stable_from_parts("t", "s", &json!({}), location, None, "id"),
            "[\"s\",\"t\",\"material:7:2\"]"
        );
        assert_eq!(
            stable_from_parts("t", "s", &json!({}), None, None, "id"),
            "[\"s\",\"t\",\"id\"]"
        );
        assert_eq!(
            citation("t", "s", &json!({"native_id":"n"}), None, Some("k"), "id"),
            "t:n"
        );
    }
}
