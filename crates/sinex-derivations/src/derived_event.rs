//! Building derived events. A derived event names every parent and takes the
//! strictest privacy class among them, so a derivation never moves data to a
//! more visible class. Its record hash covers the payload and the parents, so
//! a recompute with the same result is a repeat and one with new parents
//! replaces the old reading.
use serde_json::Value;
use sinex_model::{BuildError, Class, Event, EventId, EventPayload, Origin, ReadingOrder};

/// A stable name for the record an event was read from: `<type>:<native id>`
/// when the payload carries the source's own ID, the record location for
/// other captured events, and `<type>:<equivalence key>` for a derived event,
/// whose ID changes with every recompute. It survives a replay, unlike the
/// event ID.
pub fn record_identity(event: &Event) -> String {
    if let Some(id) = event
        .payload
        .get("native_id")
        .and_then(serde_json::Value::as_str)
    {
        return format!("{}:{id}", event.event_type);
    }
    match &event.origin {
        Origin::Record {
            location,
            output_index,
        } => {
            format!("{}:{}:{output_index}", location.material, location.anchor)
        }
        Origin::Derived { .. } => match &event.equivalence_key {
            Some(key) => format!("{}:{}", event.event_type, key.0),
            None => event.id.to_string(),
        },
    }
}

/// A derived event from `payload` with `inputs` as parents. The reading order
/// is set by the driver when it submits the key's outputs.
pub fn derived<'a, P: EventPayload>(
    payload: &P,
    derivation: &str,
    inputs: impl IntoIterator<Item = &'a Event>,
) -> Event {
    let value = serde_json::to_value(payload).expect("payloads serialize");
    build(&value, inputs, |origin, hash, class| {
        Event::build(payload, derivation, origin, ReadingOrder(0), hash, class)
    })
    .expect("derived payloads are keyed objects with at least one parent")
}

/// A derived event of the registered `event_type` from a JSON payload, which
/// is decoded through that type's Rust payload first. `None` when the type is
/// not registered or the payload does not decode.
pub fn derived_json<'a>(
    event_type: &str,
    payload: &Value,
    derivation: &str,
    inputs: impl IntoIterator<Item = &'a Event>,
) -> Option<Event> {
    let registered = sinex_model::event_types::find(event_type)?;
    build(payload, inputs, |origin, hash, class| {
        (registered.build)(payload, derivation, origin, ReadingOrder(0), hash, class)
    })
    .ok()
}

fn build<'a>(
    payload: &Value,
    inputs: impl IntoIterator<Item = &'a Event>,
    make: impl FnOnce(Origin, [u8; 32], Class) -> Result<Event, BuildError>,
) -> Result<Event, BuildError> {
    let inputs: Vec<&Event> = inputs.into_iter().collect();
    let parents = crate::parents_of(inputs.iter().copied());
    let class = inputs.iter().map(|e| e.class).max().unwrap_or(Class::Open);
    let mut sorted: Vec<EventId> = parents.clone();
    sorted.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    hasher.update(payload.to_string().as_bytes());
    for p in &sorted {
        hasher.update(p.0.as_bytes());
    }
    make(
        Origin::Derived { parents },
        *hasher.finalize().as_bytes(),
        class,
    )
}
