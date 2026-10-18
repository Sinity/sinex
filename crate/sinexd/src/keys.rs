//! Admission's dirty-key marking, backed by the registered keyed derivations.
use serde_json::Value;
use sinex_automata::{AnyKeyed, Env, KeyView};
use sinex_model::Timestamp;
use sinex_model::civil::Civil;
use sinex_store::{DirtyKeys, KeyInput};

pub struct Registered {
    derivations: Vec<Box<dyn AnyKeyed>>,
    civil: Civil,
}

impl Registered {
    pub fn new(derivations: Vec<Box<dyn AnyKeyed>>) -> Self {
        Self {
            derivations,
            civil: Civil::operator_default(),
        }
    }
}

impl DirtyKeys for Registered {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        let view = KeyView {
            event_type: event.event_type,
            ts_orig: event.ts_orig.map(Timestamp),
            payload: event.payload,
        };
        let env = Env {
            now: Timestamp::now(),
            civil: self.civil.clone(),
            settings: Value::Null,
        };
        self.derivations
            .iter()
            .filter(|d| d.inputs_types().contains(&event.event_type))
            .flat_map(|d| {
                d.keys_for(&view, &env)
                    .into_iter()
                    .map(move |k| (d.name(), k))
            })
            .collect()
    }
}
