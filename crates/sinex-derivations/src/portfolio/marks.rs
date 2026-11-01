//! Delayed mark links use native record identities or exact session containment.
//! A command record a fused command joins is linked through the fused one.
use crate::activity::{evidence, stable};
use crate::derived_event::{derived, record_identity};
use crate::fusion::rules::COMMANDS;
use crate::observed::FOCUS;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind};
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
    const VERSION: u32 = 2;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &[
        "operator.mark",
        "command.fused",
        "command.executed",
        "command.imported",
        "state.interval",
        "window.span",
        "shell.session",
        "activity.session",
    ];
    const OUTPUTS: &'static [&'static str] = &["reference.about"];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(at) = input.ts_orig else {
            return vec![];
        };
        // A command's fused record may lie in the neighbouring day.
        let margin = if COMMANDS.reads(input.event_type, input.payload) {
            COMMANDS.margin()
        } else {
            0
        };
        let start = Timestamp::from_unix_ns(at.unix_ns() - margin).unwrap_or(at);
        let Ok(mut day) = env.time_settings.day(start) else {
            return vec![];
        };
        let end = time(input.payload, "ended_at").unwrap_or(at);
        let end = Timestamp::from_unix_ns(end.unix_ns() + margin).unwrap_or(end);
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
        let margin = COMMANDS.margin();
        let mut kinds = vec![
            Kind::all("operator.mark"),
            Kind::all("shell.session"),
            Kind::all("activity.session"),
        ];
        kinds.extend(COMMANDS.kinds());
        kinds.extend(FOCUS.kinds());
        let mut query = InputQuery::of(
            kinds,
            Timestamp::from_unix_ns(env.time_settings.day_start(day).ok()?.unix_ns() - margin)
                .ok()?,
            Timestamp::from_unix_ns(env.time_settings.day_end(day).ok()?.unix_ns() + margin)
                .ok()?,
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
        ];
        query.intervals.extend(FOCUS.intervals());
        Some(query)
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let commands: Vec<_> = COMMANDS
            .occurrences(inputs)
            .into_iter()
            .map(|o| (o.event, "command_record"))
            .collect();
        let focus = inputs
            .iter()
            .filter(|e| FOCUS.admits(&e.event_type, &e.payload))
            .map(|e| (e, "focus_record"));
        let records: Vec<_> = commands.into_iter().chain(focus).collect();
        let sessions = inputs
            .iter()
            .filter(|e| matches!(e.event_type.as_str(), "shell.session" | "activity.session"));
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
            let by_record = records.iter().filter_map(|(target, field)| {
                let record = mark.payload[*field].as_str()?;
                (record == record_identity(target)
                    || target.payload["record"].as_str() == Some(record))
                .then_some((*target, "native_record_key"))
            });
            let by_session = sessions.clone().filter_map(|target| {
                if host(&target.payload).as_deref() != mark.payload["host"].as_str() {
                    return None;
                }
                let start = target.ts_orig.at?;
                let end = time(&target.payload, "ended_at")?;
                (start <= at && at <= end).then_some((target, "containing_session"))
            });
            for (target, method) in by_record.chain(by_session) {
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
