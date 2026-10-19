//! The registries as data for the NixOS module (architecture §5.4, §10): every
//! source and derivation with what an option needs to know about it. The
//! module reads the generated `nix/catalog.json`, so adding a source to the
//! registry adds its option; a test keeps the file current.
use serde_json::{Value, json};

pub fn catalog() -> Value {
    let sources: Vec<Value> = sinex_sources::sources::specs()
        .into_iter()
        .map(|s| {
            json!({
                "name": s.name,
                "kind": sinex_sources::sources::kind_of(s.name).map(|k| k.name),
                "driver": format!("{:?}", s.driver),
                "consumer": s.consumer,
                "class": s.class,
                "eventTypes": s.event_types,
            })
        })
        .collect();
    let keyed = sinex_automata::keyed().into_iter().map(|d| {
        json!({
            "name": d.name(),
            "kind": "keyed",
            "inputs": d.inputs_types(),
            "outputs": d.output_types(),
        })
    });
    let per_event = sinex_automata::per_event().into_iter().map(|d| {
        json!({
            "name": d.name(),
            "kind": "per-event",
            "inputs": d.inputs_types(),
            "outputs": d.output_types(),
        })
    });
    json!({
        "sources": sources,
        "derivations": keyed.chain(per_event).collect::<Vec<_>>(),
    })
}

/// The catalog as committed: pretty JSON with a trailing newline.
pub fn render() -> String {
    let mut text = serde_json::to_string_pretty(&catalog()).expect("catalog serializes");
    text.push('\n');
    text
}

#[cfg(test)]
mod tests;
