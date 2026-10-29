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

/// A declared JSON output; bundled declarations also supply their exact Rust
/// validator. Invalid and unavailable definitions are explicit errors.
pub fn derived_json<'a>(
    declaration: &sinex_model::KindDeclaration,
    validator: Option<sinex_model::event_types::Build>,
    payload: &Value,
    derivation: &str,
    inputs: impl IntoIterator<Item = &'a Event>,
) -> Result<Event, BuildError> {
    build(payload, inputs, |origin, hash, class| {
        if declaration.normalization == sinex_model::kinds::Normalization::Rust {
            let validator =
                validator.ok_or_else(|| BuildError::Declaration("validator_unavailable".into()))?;
            let event = validator(payload, derivation, origin, ReadingOrder(0), hash, class)?;
            if event.kind_ref() != declaration.kind {
                return Err(BuildError::Declaration("validator_unavailable".into()));
            }
            return Ok(event);
        }
        let evaluated = declaration
            .evaluate(payload, class)
            .map_err(BuildError::Declaration)?;
        if evaluated.equivalence_key.is_none() {
            return Err(BuildError::DerivedWithoutKey(declaration.kind.name.clone()));
        }
        if let Origin::Derived { parents } = &origin
            && parents.is_empty()
        {
            return Err(BuildError::NoParents);
        }
        let mut event = Event::submitted(
            declaration.kind.clone(),
            payload.clone(),
            derivation.into(),
            origin,
            ReadingOrder(0),
            hash,
            class,
        );
        event.equivalence_key = evaluated.equivalence_key;
        event.ts_orig = evaluated.ts_orig;
        event.record_state = evaluated.record_state;
        event.owned_blobs = evaluated.owned_blobs;
        event.class = evaluated.class;
        event.replace = declaration.replace_rule;
        Ok(event)
    })
}

fn build<'a>(
    payload: &Value,
    inputs: impl IntoIterator<Item = &'a Event>,
    make: impl FnOnce(Origin, [u8; 32], Class) -> Result<Event, BuildError>,
) -> Result<Event, BuildError> {
    let inputs: Vec<&Event> = inputs.into_iter().collect();
    let parents = crate::parents_of(inputs.iter().copied());
    let class = inputs.iter().map(|e| e.class).max().unwrap_or(Class::Open);
    let hash = payload_hash(payload,&parents);
    make(Origin::Derived { parents },hash,class)
}


/// The complete stored payload and exact parents determine a derived reading.
pub fn payload_hash(payload: &Value,parents: &[EventId]) -> [u8;32] {
    let mut sorted = parents.to_vec();
    sorted.sort_unstable();
    let mut hasher = blake3::Hasher::new();
    hasher.update(payload.to_string().as_bytes());
    for parent in sorted {hasher.update(parent.0.as_bytes());}
    *hasher.finalize().as_bytes()
}
