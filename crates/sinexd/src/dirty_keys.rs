//! Admission's dirty-key marking, backed by the registered keyed derivations:
//! a changed event dirties the keys of every derivation that reads its type.
use serde_json::Value;
use sinex_derivations::{AnyKeyed, Env, KeyView};
use sinex_model::Timestamp;
use sinex_model::time_settings::TimeSettings;
use sinex_store::{DirtyKeys, KeyInput};

pub(crate) struct Registered {
    derivations: Vec<Box<dyn AnyKeyed>>,
    time_settings: TimeSettings,
}

impl Registered {
    pub(crate) fn new(derivations: Vec<Box<dyn AnyKeyed>>) -> Self {
        Self {
            derivations,
            time_settings: TimeSettings::operator_default(),
        }
    }
}

impl DirtyKeys for Registered {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(String, Value)> {
        let view = KeyView {
            event_type: event.event_type,
            ts_orig: event.ts_orig.map(Timestamp),
            payload: event.payload,
        };
        let env = Env {
            now: Timestamp::now(),
            time_settings: self.time_settings.clone(),
            settings: Value::Null,
        };
        self.derivations
            .iter()
            .filter(|d| d.inputs_types().contains(&event.event_type))
            .flat_map(|d| {
                d.keys_for(&view, &env)
                    .into_iter()
                    .map(move |k| (d.name().to_string(), k))
            })
            .collect()
    }
}
