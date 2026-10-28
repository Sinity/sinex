//! Presence uses the same elementary segmentation as activity, so the two
//! cannot disagree about physical input or exact absence boundaries.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::Event;
use sinex_model::event_types::{ActivitySpan, desktop::PresenceInterval};

pub struct Presence;

impl Keyed for Presence {
    const NAME: &'static str = "presence.intervals";
    const VERSION: u32 = 2;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &["activity.span"];
    const OUTPUTS: &'static [&'static str] = &["presence.interval"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        input
            .payload
            .get("partition_start")
            .map(|hour| vec![json!({"hour": hour})])
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let start = serde_json::from_value(key.get("hour")?.clone()).ok()?;
        if env.time_settings.hour_start(start).ok()? != start { return None }
        Some(InputQuery::window(
            Self::INPUTS,
            start,
            env.time_settings.hour_end(start).ok()?,
        ))
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        match self.inputs(key, env).and_then(|query| query.window) {
            Some((_, end)) if env.now < end => KeyState::Open,
            _ => KeyState::Closed,
        }
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        Computed {
            outputs: inputs
                .iter()
                .filter_map(|event| {
                    let span = event.payload_as::<ActivitySpan>()?;
                    let contradictions = span
                        .presence_claims
                        .iter()
                        .filter(|claim| {
                            (span.presence == "present" && claim.as_str() == "afk")
                                || (span.presence == "absent" && claim.as_str() == "not-afk")
                        })
                        .cloned()
                        .collect();
                    Some(derived(
                        &PresenceInterval {
                            scope: span.scope,
                            day: span.day,
                            started_at: span.started_at,
                            ended_at: span.ended_at,
                            state: span.presence,
                            method: span.presence_method,
                            support: span.agency_evidence,
                            claims: span.presence_claims,
                            contradictions,
                            profile: span.profile,
                        },
                        Self::NAME,
                        [event],
                    ))
                })
                .collect(),
            waiting: false,
        }
    }
}

#[cfg(test)]
mod tests;
