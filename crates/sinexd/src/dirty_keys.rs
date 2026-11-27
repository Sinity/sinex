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
    pub(crate) fn new(derivations: Vec<Box<dyn AnyKeyed>>, time_settings: TimeSettings) -> Self {
        Self {
            derivations,
            time_settings,
        }
    }
}

impl DirtyKeys for Registered {
    fn reads_type(&self, kind: &str) -> bool {
        self.derivations.iter().any(|d| d.reads_type(kind))
    }
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
            .filter(|d| d.reads_type(event.event_type))
            .flat_map(|d| {
                d.keys_for(&view, &env)
                    .into_iter()
                    .map(move |k| (d.name().to_string(), k))
            })
            .collect()
    }
    fn stores_inputs(&self, derivation: &str, key: &Value) -> bool {
        self.derivations
            .iter()
            .any(|d| d.name() == derivation && d.stores_inputs(key))
    }
    fn output_ranges(&self) -> Vec<sinex_store::OutputRange> {
        self.derivations
            .iter()
            .flat_map(|d| {
                d.output_ranges()
                    .iter()
                    .map(move |range| sinex_store::OutputRange {
                        derivation: d.name(),
                        output_type: range.output_type,
                        start_field: range.start_field,
                        end_field: range.end_field,
                        open_field: range.open_field,
                        fence_field: range.fence_field,
                    })
            })
            .collect()
    }
    fn affected_ranges(&self, event: &KeyInput<'_>) -> Vec<sinex_store::AffectedRange> {
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
            .filter(|d| d.reads_type(event.event_type))
            .flat_map(|d| {
                d.affected_ranges(&view, &env)
                    .into_iter()
                    .map(move |range| sinex_store::AffectedRange {
                        derivation: d.name(),
                        output_type: range.output_type,
                        start: range.start.0,
                        end: range.end.0,
                    })
            })
            .collect()
    }

    fn nearby_inputs(&self, event: &KeyInput<'_>) -> Vec<sinex_store::derivations::Window> {
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
            .filter(|d| d.reads_type(event.event_type))
            .filter_map(|d| d.nearby_inputs(&view, &env))
            .map(|query| crate::derivation_runner::to_window(&query, env.now))
            .collect()
    }
}

#[cfg(test)]
mod tests;
