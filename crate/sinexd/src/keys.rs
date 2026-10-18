//! Admission's dirty-key marking, backed by the registered keyed derivations.
use serde_json::Value;
use sinex_automata::{AnyKeyed, KeyView};
use sinex_model::Timestamp;
use sinex_store::{DirtyKeys, KeyInput};

pub struct Registered(pub Vec<Box<dyn AnyKeyed>>);

impl DirtyKeys for Registered {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        let view = KeyView {
            event_type: event.event_type,
            ts_orig: event.ts_orig.map(Timestamp),
            payload: event.payload,
        };
        self.0
            .iter()
            .filter(|d| d.inputs_types().contains(&event.event_type))
            .flat_map(|d| d.keys_for(&view).into_iter().map(move |k| (d.name(), k)))
            .collect()
    }
}
