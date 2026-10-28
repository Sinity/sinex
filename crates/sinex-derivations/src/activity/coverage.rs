//! A closed physical recorder lifetime bounds where silence can mean absence.
use super::*;
use sinex_model::event_types::activity::InputCoverageInterval;

pub struct InputCoverage;
impl Keyed for InputCoverage {
    const NAME: &'static str = "input.coverage";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["input.logger"];
    const OUTPUTS: &'static [&'static str] = &["input.coverage"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if !matches!(input.payload["event"].as_str(), Some("start" | "stop")) {
            return vec![];
        }
        let Some(session) = input.payload["session"].as_str().filter(|session| !session.is_empty()) else {
            return vec![];
        };
        let scope = scope(input.payload, input.event_type);
        if serde_json::from_str::<Value>(&scope).is_ok_and(|scope| scope[0] == "unknown") {
            return vec![];
        }
        vec![json!({"session": session, "scope": scope})]
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        Some(InputQuery::all(vec![
            crate::Kind::only("input.logger", &[("event", "start")]),
            crate::Kind::only("input.logger", &[("event", "stop")]),
        ]).only("session", key.get("session")?.clone()))
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState { KeyState::Closed }
    fn compute(&self, key: &Value, inputs: &[Event], _: &Env) -> Computed {
        let Some(captured_scope) = key["scope"].as_str() else { return Computed::default() };
        let mut events: Vec<_> = inputs.iter().filter(|event| event.ts_orig.at.is_some()
            && scope(&event.payload, event.event_type) == captured_scope).collect();
        events.sort_by_cached_key(|event| (event.ts_orig.at, stable(event)));
        let mut start: Option<&Event> = None;
        let mut outputs = Vec::new();
        for event in events {
            match event.payload["event"].as_str() {
                Some("start") => { start = Some(event); }
                Some("stop") => {
                    let Some(first) = start.take() else { continue };
                    if first.payload.pointer("/fields/layer").and_then(Value::as_str) != Some("evdev")
                        || first.payload.pointer("/fields/virtual").and_then(Value::as_bool) != Some(false) {
                        continue;
                    }
                    outputs.push(derived(&InputCoverageInterval {
                        coverage_id: stable(first), scope: captured_scope.into(),
                        started_at: first.ts_orig.at.unwrap(), ended_at: event.ts_orig.at.unwrap(),
                    }, Self::NAME, [first, event]));
                }
                _ => {}
            }
        }
        Computed { outputs, waiting: false }
    }
}
