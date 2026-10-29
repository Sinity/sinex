//! The registries as data for the NixOS module: every source and derivation
//! with what an option needs to know about it. The module reads the generated
//! `nix/catalog.json`, so adding a source to the registry adds its option; a
//! test keeps the file current.
use serde_json::{Value, json};
use sinex_derivations::AnyPerEvent;

pub(crate) fn catalog() -> Value {
    let sources: Vec<Value> = sources().into_iter().map(|s| s.to_json()).collect();
    let keyed = crate::analysis::keyed().into_iter().map(|d| {
        json!({
            "name": d.name(),
            "enabledByDefault": sinex_derivations::enabled_by_default(d.name()),
            "kind": "keyed",
            "history": d.history_policy(),
            "inputs": d.inputs_types(),
            "outputs": d.output_types(),
        })
    });
    let mut per_event = sinex_derivations::per_event();
    per_event.push(
        Box::new(crate::document_text::DocumentText { pdf_reader: None }) as Box<dyn AnyPerEvent>,
    );
    per_event
        .push(Box::new(crate::screen_text::ScreenText { engine: None }) as Box<dyn AnyPerEvent>);
    let per_event = per_event.into_iter().map(|d| {
        json!({
            "name": d.name(),
            "enabledByDefault": sinex_derivations::enabled_by_default(d.name()),
            "kind": "per-event",
            "history": d.history_policy(),
            "inputs": d.inputs_types(),
            "outputs": d.output_types(),
        })
    });
    json!({
        "eventKinds": sinex_model::event_types::declarations(),
        "sources": sources,
        "derivations": keyed.chain(per_event).collect::<Vec<_>>(),
    })
}

/// One catalog entry: every declaration that shares a source name (one per
/// table it reads, or one per format that can read it), as one option.
struct CatalogSource {
    name: &'static str,
    formats: Vec<&'static str>,
    drivers: Vec<&'static str>,
    purpose: &'static str,
    class: sinex_model::Class,
    /// The newest parser version among its declarations.
    version: u32,
    event_types: Vec<&'static str>,
    discovery: Vec<Value>,
}

impl CatalogSource {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "formats": self.formats,
            "drivers": self.drivers,
            "purpose": self.purpose,
            "class": self.class,
            "eventTypes": self.event_types,
            "discovery": self.discovery,
        })
    }
}

fn sources() -> Vec<CatalogSource> {
    let mut out: Vec<CatalogSource> = Vec::new();
    for format in sinex_sources::sources::formats() {
        for spec in (format.specs)() {
            let at = match out.iter().position(|s| s.name == spec.name) {
                Some(at) => at,
                None => {
                    out.push(CatalogSource {
                        name: spec.name,
                        formats: Vec::new(),
                        drivers: Vec::new(),
                        purpose: spec.purpose,
                        class: spec.class,
                        version: spec.parser_version,
                        event_types: Vec::new(),
                        discovery: Vec::new(),
                    });
                    out.len() - 1
                }
            };
            let entry = &mut out[at];
            assert!(
                format.discovery.request_only
                    || !format.discovery.locations.is_empty()
                    || !format.discovery.streams.is_empty(),
                "{}: no discovery or request-only role",
                format.name
            );
            let description = json!({"format": format.name, "capture": format.capture.is_some(), "description": format.discovery});
            if !entry.discovery.contains(&description) {
                entry.discovery.push(description);
            }
            entry.version = entry.version.max(spec.parser_version);
            // A source read several ways is as private as its most private
            // declaration.
            entry.class = entry.class.max(spec.class);
            for (list, value) in [
                (&mut entry.formats, format.name),
                (&mut entry.drivers, spec.driver.name()),
            ] {
                if !list.contains(&value) {
                    list.push(value);
                }
            }
            for event_type in spec.event_types {
                if !entry.event_types.contains(event_type) {
                    entry.event_types.push(event_type);
                }
            }
        }
    }
    out
}

/// The sources as `registry.sources` rows.
pub(crate) fn registry_sources() -> Vec<sinex_store::registry::Source> {
    sources()
        .into_iter()
        .map(|s| sinex_store::registry::Source {
            name: s.name.to_owned(),
            driver: s.drivers.join(", "),
            version: i32::try_from(s.version).unwrap_or(i32::MAX),
            event_types: s.event_types.iter().map(|t| (*t).to_owned()).collect(),
            purpose: s.purpose.to_owned(),
        })
        .collect()
}

/// The catalog as committed: pretty JSON with a trailing newline.
pub(crate) fn render() -> String {
    let mut catalog = catalog();
    catalog.sort_all_objects();
    let mut text = serde_json::to_string_pretty(&catalog).expect("catalog serializes");
    text.push('\n');
    text
}

#[cfg(test)]
mod tests;
