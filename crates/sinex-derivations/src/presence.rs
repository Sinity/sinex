//! Presence uses the same elementary segmentation as activity, so the two
//! cannot disagree about physical input or recorded absence boundaries.
use crate::derived_event::derived;
use crate::keys::HourKey;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};

use sinex_model::Event;
use sinex_model::event_types::{ActivitySpan, desktop::PresenceInterval};

pub struct Presence;

impl Keyed for Presence {
    type Key = HourKey;
    const NAME: &'static str = "presence.intervals";
    const VERSION: u32 = 2;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &["activity.span"];
    const OUTPUTS: &'static [&'static str] = &["presence.interval"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Self::Key> {
        input
            .payload
            .get("partition_start")
            .and_then(|hour| serde_json::from_value(hour.clone()).ok())
            .map(|hour| vec![HourKey { hour }])
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Self::Key, env: &Env) -> Option<InputQuery> {
        let start = key.hour;
        if env.time_settings.hour_start(start).ok()? != start {
            return None;
        }
        Some(InputQuery::window(
            Self::INPUTS,
            start,
            env.time_settings.hour_end(start).ok()?,
        ))
    }
    fn key_state(&self, key: &Self::Key, env: &Env) -> KeyState {
        match self.inputs(key, env).and_then(|query| query.window) {
            Some((_, end)) if env.now < end => KeyState::Open,
            _ => KeyState::Closed,
        }
    }
    fn compute(&self, _: &Self::Key, inputs: &[Event], _: &Env) -> Computed {
        Computed {
            error: None,
            outputs: inputs
                .iter()
                .filter_map(|event| {
                    let span = event.payload_as::<ActivitySpan>()?;
                    let contradictions = span
                        .presence_claims
                        .iter()
                        .filter(|report| {
                            (span.presence == "present" && report.as_str() == "afk")
                                || (span.presence == "absent" && report.as_str() == "not-afk")
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
