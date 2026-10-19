//! Admission's dirty-key marking, backed by the registered keyed derivations
//! and the shipped metrics: a changed event dirties its derivations' keys and
//! the logical day of every metric over its type (`analysis.md` §4.4).
use serde_json::{Value, json};
use sinex_automata::{AnyKeyed, Env, KeyView};
use sinex_model::Timestamp;
use sinex_model::civil::Civil;
use sinex_store::{DirtyKeys, KeyInput};

pub struct Registered {
    derivations: Vec<Box<dyn AnyKeyed>>,
    /// `(relation, dirty-key name)` of every shipped metric.
    metrics: Vec<(&'static str, &'static str)>,
    civil: Civil,
}

impl Registered {
    pub fn new(derivations: Vec<Box<dyn AnyKeyed>>) -> Self {
        let metrics = sinex_analysis::catalog::METRICS
            .iter()
            .map(|m| {
                // Built once per process for a fixed catalog.
                let name: &'static str =
                    Box::leak(crate::analysis::dirty_name(m.id).into_boxed_str());
                (m.relation, name)
            })
            .collect();
        Self {
            derivations,
            metrics,
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
        let mut keys: Vec<(&'static str, Value)> = self
            .derivations
            .iter()
            .filter(|d| d.inputs_types().contains(&event.event_type))
            .flat_map(|d| {
                d.keys_for(&view, &env)
                    .into_iter()
                    .map(move |k| (d.name(), k))
            })
            .collect();
        // An event with an unknown time lands in no day, so dirties none.
        if let Some(day) = event
            .ts_orig
            .and_then(|t| self.civil.day(Timestamp(t)).ok())
        {
            let key = json!({ "day": day.to_string() });
            keys.extend(
                self.metrics
                    .iter()
                    .filter(|(relation, _)| *relation == event.event_type)
                    .map(|(_, name)| (*name, key.clone())),
            );
        }
        keys
    }
}
