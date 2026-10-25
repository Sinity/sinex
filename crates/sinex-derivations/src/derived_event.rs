//! Building derived events. A derived event names every parent and takes the
//! strictest privacy class among them, so a derivation never moves data to a
//! more visible class. Its record hash covers the payload and the parents, so
//! a recompute with the same result is a repeat and one with new parents
//! replaces the old reading.
use sinex_model::{Class, Event, EventId, EventPayload, Origin, ReadingOrder};

/// A stable name for the record an event was read from: `<type>:<native id>`
/// when the payload carries the source's own ID, otherwise the record
/// location. It survives a replay, unlike the event ID.
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
        Origin::Derived { .. } => event.id.to_string(),
    }
}

/// A derived event from `payload` with `inputs` as parents. The reading order
/// is set by the driver when it submits the key's outputs.
pub fn derived<'a, P: EventPayload>(
    payload: &P,
    derivation: &str,
    inputs: impl IntoIterator<Item = &'a Event>,
) -> Event {
    let inputs: Vec<&Event> = inputs.into_iter().collect();
    let parents = crate::parents_of(inputs.iter().copied());
    let class = inputs.iter().map(|e| e.class).max().unwrap_or(Class::Open);
    let mut sorted: Vec<EventId> = parents.clone();
    sorted.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    hasher.update(
        serde_json::to_string(payload)
            .expect("payloads serialize")
            .as_bytes(),
    );
    for p in &sorted {
        hasher.update(p.0.as_bytes());
    }
    Event::build(
        payload,
        derivation,
        Origin::Derived { parents },
        ReadingOrder(0),
        *hasher.finalize().as_bytes(),
        class,
    )
    .expect("derived payloads are keyed objects with at least one parent")
}
