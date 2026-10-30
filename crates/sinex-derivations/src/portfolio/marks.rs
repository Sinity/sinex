//! Delayed mark links use native record identities or exact session containment.
use crate::activity::{evidence, stable};
use crate::derived_event::{derived, record_identity};
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::event_types::portfolio::MarkLink;
use sinex_model::{Event, Timestamp};

pub struct MarkLinks;
fn time(payload: &Value, field: &str) -> Option<Timestamp> {
    serde_json::from_value(payload.get(field)?.clone()).ok()
}
fn host(payload: &Value) -> Option<String> {
    if let Some(host) = payload["host"].as_str() {
        return Some(host.into());
    }
    let scope: Value = serde_json::from_str(payload["scope"].as_str()?).ok()?;
    (scope[0] == "host")
        .then(|| scope[1].as_str().map(str::to_owned))
        .flatten()
}
impl Keyed for MarkLinks {
    const NAME: &'static str = "reference.marks";
    const VERSION: u32 = 1;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &[
        "operator.mark",
        "command.fused",
        "focus.fused",
        "shell.session",
        "activity.session",
    ];
    const OUTPUTS: &'static [&'static str] = &["reference.about"];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let Ok(mut day) = env.time_settings.day(start) else {
            return vec![];
        };
        let end = time(input.payload, "ended_at").unwrap_or(start);
        let Ok(last) = env.time_settings.day(end) else {
            return vec![];
        };
        let mut keys = vec![];
        loop {
            keys.push(json!({"day":day.to_string()}));
            if day >= last {
                break;
            }
            let Ok(next) = day.tomorrow() else { break };
            day = next;
        }
        keys
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = key["day"].as_str()?.parse().ok()?;
        let mut query = InputQuery::window(
            Self::INPUTS,
            env.time_settings.day_start(day).ok()?,
            env.time_settings.day_end(day).ok()?,
        );
        query.intervals = vec![
            (
                "shell.session",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
            (
                "activity.session",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
            (
                "focus.fused",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
        ];
        Some(query)
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let mut outputs = vec![];
        for mark in inputs
            .iter()
            .filter(|event| event.event_type == "operator.mark")
        {
            let Some(at) = mark.ts_orig.at else { continue };
            if env
                .time_settings
                .day(at)
                .ok()
                .map(|day| day.to_string())
                .as_deref()
                != key["day"].as_str()
            {
                continue;
            }
            for target in inputs
                .iter()
                .filter(|event| event.event_type != "operator.mark")
            {
                let method = match target.event_type.as_str() {
                    "command.fused" | "focus.fused" => {
                        let field = if target.event_type == "command.fused" {
                            "command_record"
                        } else {
                            "focus_record"
                        };
                        let Some(record) = mark.payload[field].as_str() else {
                            continue;
                        };
                        if record != record_identity(target)
                            && target.payload["record"].as_str() != Some(record)
                        {
                            continue;
                        }
                        "native_record_key"
                    }
                    "shell.session" | "activity.session" => {
                        if host(&target.payload).as_deref() != mark.payload["host"].as_str() {
                            continue;
                        }
                        let (Some(start), Some(end)) =
                            (target.ts_orig.at, time(&target.payload, "ended_at"))
                        else {
                            continue;
                        };
                        if at < start || at > end {
                            continue;
                        }
                        "containing_session"
                    }
                    _ => continue,
                };
                outputs.push(derived(
                    &MarkLink {
                        mark_record: stable(mark),
                        target_record: stable(target),
                        at,
                        mark: evidence(mark),
                        target: evidence(target),
                        method: method.into(),
                    },
                    Self::NAME,
                    [mark, target],
                ));
            }
        }
        Computed {
            error: None,
            outputs,
            waiting: false,
        }
    }
}
