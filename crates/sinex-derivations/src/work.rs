//! Work is keyed by recorded repository and branch. Session lifetimes do not
//! create operator time; every act, episode and resume pack is a stored event.
use crate::activity::{SECOND, evidence, stable, union_seconds};
use crate::derived_event::derived;
use crate::{AffectedRange, Computed, Env, InputQuery, InputRange, KeyState, KeyView, Keyed, Kind};
use serde_json::{Value, json};
use sinex_model::event_types::{activity::Agency, polylogue, work::*};
use sinex_model::{Event, Timestamp};
use std::collections::{BTreeMap, BTreeSet};

pub struct WorkActs;
impl Keyed for WorkActs {
    const NAME: &'static str = "work.acts";
    const VERSION: u32 = 2;
    const INPUTS: &'static [&'static str] = &[
        "polylogue.session",
        "polylogue.message",
        "polylogue.tool_call",
        "polylogue.turn",
    ];
    const OUTPUTS: &'static [&'static str] = &["work.act"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        input
            .payload
            .get("session")
            .filter(|id| id.as_str().is_some_and(|id| !id.is_empty()))
            .map(|id| vec![json!({"session": id})])
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let mut query = InputQuery::all(Self::INPUTS.iter().map(|kind| Kind::all(kind)).collect())
            .only("session", key.get("session")?.clone());
        query.unknown_times = true;
        Some(query)
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let mut sessions: Vec<_> = inputs
            .iter()
            .filter(|event| event.event_type == "polylogue.session")
            .collect();
        sessions.sort_by_cached_key(|event| stable(event));
        let Some(session) = sessions.first().copied() else {
            return Computed::default();
        };
        let Some(metadata) = session.payload_as::<polylogue::Session>() else {
            return Computed::default();
        };
        if sessions.iter().any(|event| {
            event.payload["repo"] != session.payload["repo"]
                || event.payload["branch"] != session.payload["branch"]
                || event.payload["machine"] != session.payload["machine"]
                || event.payload["kind"] != session.payload["kind"]
                || event.payload["mode"] != session.payload["mode"]
        }) {
            return Computed::default();
        }
        let label = metadata.branch.clone().filter(|branch| !branch.is_empty());
        let objective = metadata
            .repo
            .clone()
            .filter(|repo| !repo.is_empty())
            .zip(label.clone())
            .map(|(repository, branch)| Objective {
                kind: "branch".into(),
                reference: json!([repository, branch]).to_string(),
                repository,
            });
        let episode = objective
            .as_ref()
            .map(|objective| json!([objective.kind, objective.reference]).to_string());
        let mut ordered: Vec<_> = inputs
            .iter()
            .filter(|event| event.ts_orig.at.is_some())
            .collect();
        ordered.sort_by_cached_key(|event| (event.ts_orig.at, stable(event)));
        let outputs = ordered
            .into_iter()
            .filter_map(|event| {
                let operator = event.event_type == "polylogue.message"
                    && event.payload["written_by"] == "human"
                    && event.payload["role"] == "user"
                    && event.payload["prompt_kind"] == "prose"
                    && metadata.kind == "main"
                    && metadata.mode.is_none();
                let delegated =
                    matches!(event.event_type, "polylogue.tool_call" | "polylogue.turn");
                if !operator && !delegated {
                    return None;
                }
                Some(derived(
                    &WorkAct {
                        act: stable(event),
                        session: metadata.session.clone(),
                        episode: episode.clone(),
                        objective: objective.clone(),
                        label: label.clone(),
                        label_source: evidence(session),
                        machine: metadata.machine.clone(),
                        at: event.ts_orig.at.unwrap(),
                        ended_at: event
                            .payload_as::<polylogue::Turn>()
                            .and_then(|turn| turn.end)
                            .filter(|end| Some(*end) >= event.ts_orig.at),
                        evidence: evidence(event),
                        agency: if operator {
                            Agency::Operator
                        } else {
                            Agency::Delegated
                        },
                        action: if operator {
                            "prompt".into()
                        } else {
                            event.payload["action"].as_str().unwrap_or("turn").into()
                        },
                        path: event.payload["path"].as_str().map(str::to_owned),
                        failed: event.payload["outcome"] == "error",
                    },
                    Self::NAME,
                    [event].into_iter().chain(sessions.iter().copied()),
                ))
            })
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }
}

fn episode_kind(episode: &Value) -> Kind {
    let mut kind = Kind::all("work.act");
    kind.equal.push(("episode", episode.clone()));
    kind
}

pub struct WorkEpisodes;
impl Keyed for WorkEpisodes {
    const NAME: &'static str = "work.episodes";
    const VERSION: u32 = 4;
    const INPUTS: &'static [&'static str] = &["work.act", "state.interval", "rest.fused"];
    const OUTPUTS: &'static [&'static str] =
        &["work.episode", "work.suspension", "work.resumption"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if input.event_type != "work.act" {
            return vec![];
        }
        input
            .payload
            .get("episode")
            .filter(|id| id.as_str().is_some())
            .map(|id| vec![json!({"episode":id})])
            .unwrap_or_default()
    }
    fn affected_ranges(&self, input: &KeyView<'_>, _: &Env) -> Vec<AffectedRange> {
        input
            .ts_orig
            .map(|at| {
                vec![AffectedRange {
                    open_field: None,
                    output_type: "work.episode",
                    start_field: "started_at",
                    end_field: "last_activity",
                    start: Timestamp::from_unix_ns(at.unix_ns() - 14 * 86_400 * SECOND)
                        .unwrap_or(Timestamp(time::PrimitiveDateTime::MIN.assume_utc())),
                    end: input
                        .payload
                        .get("ended_at")
                        .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
                        .unwrap_or(at),
                }]
            })
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let mut query = InputQuery::all(Self::INPUTS.iter().map(|kind| Kind::all(kind)).collect());
        query.range = Some(InputRange::Extent {
            kinds: vec![episode_kind(key.get("episode")?)],
        });
        query.after.push(crate::Neighbour {
            kinds: vec![Kind::only("work.act", &[("agency", "operator")])],
            per: None,
            count: 1,
        });
        query.after.push(crate::Neighbour {
            kinds: vec![Kind::all("rest.fused")],
            per: None,
            count: 1,
        });
        query.after.push(crate::Neighbour {
            kinds: ["desktop.lock", "desktop.display", "machine.suspend"]
                .into_iter()
                .flat_map(|state| {
                    ["locked", "off", "suspended"]
                        .into_iter()
                        .map(move |label| {
                            Kind::only("state.interval", &[("state_kind", state), ("label", label)])
                        })
                })
                .collect(),
            per: Some("subject_id"),
            count: 1,
        });
        query.intervals = vec![
            (
                "state.interval",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
            (
                "rest.fused",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
        ];
        Some(query)
    }
    fn refresh_at(&self, outputs: &[Event], _: &Env) -> Option<Timestamp> {
        outputs
            .iter()
            .filter_map(|event| event.payload_as::<WorkEpisode>())
            .filter(|episode| episode.state != "dormant")
            .filter_map(|episode| {
                Timestamp::from_unix_ns(episode.last_activity.unix_ns() + 14 * 86_400 * SECOND).ok()
            })
            .min()
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Open
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let mut acts: Vec<_> = inputs
            .iter()
            .filter_map(|event| Some((event, event.payload_as::<WorkAct>()?)))
            .collect();
        acts.sort_by_cached_key(|(event, act)| (act.at, stable(event)));
        let group: Vec<_> = acts
            .iter()
            .filter(|(_, act)| {
                key["episode"].as_str() == act.episode.as_deref() && act.episode.is_some()
            })
            .collect();
        let (Some((_, first)), Some(_)) = (group.first().copied(), group.last().copied()) else {
            return Computed::default();
        };
        let episode = first.episode.clone().expect("objective key");
        let last_activity = group
            .iter()
            .map(|(_, act)| act.ended_at.unwrap_or(act.at))
            .max()
            .unwrap();
        let mut parents: Vec<_> = group.iter().map(|(event, _)| *event).collect();
        let mut outputs = Vec::new();
        let mut suspensions = Vec::new();
        let mut resumptions = Vec::new();
        let mut absorbed_excursions = 0;
        let operator: Vec<_> = group
            .iter()
            .filter(|(_, act)| act.agency == Agency::Operator)
            .copied()
            .collect();
        let mut pending = false;
        for (index, (before_event, before)) in operator.iter().enumerate() {
            let after = operator.get(index + 1).copied();
            let until = after.map_or_else(
                || {
                    Timestamp::from_unix_ns(last_activity.unix_ns() + 14 * 86_400 * SECOND)
                        .unwrap_or(env.now)
                        .min(env.now)
                },
                |(_, act)| act.at,
            );
            let between: Vec<_> = acts
                .iter()
                .filter(|(_, act)| {
                    act.agency == Agency::Operator
                        && act.at > before.at
                        && act.at <= until
                        && act.episode.as_deref().is_some_and(|other| other != episode)
                        && after.is_none_or(|(_, next)| act.at < next.at)
                })
                .collect();
            let absence: Vec<_> = inputs
                .iter()
                .filter(|event| {
                    let Some(start) = event.ts_orig.at else {
                        return false;
                    };
                    start > before.at
                        && start <= until
                        && after.is_none_or(|(_, next)| start < next.at)
                        && (event.event_type == "rest.fused"
                            || (event.event_type == "state.interval"
                                && before.machine.as_ref().is_some_and(|host| {
                                    event
                                        .payload
                                        .pointer("/attributes/host")
                                        .and_then(Value::as_str)
                                        == Some(host.as_str())
                                })
                                && matches!(
                                    event.payload["label"].as_str(),
                                    Some("locked" | "suspended" | "off")
                                )))
                })
                .collect();
            let first_switch = between.first().map(|(event, act)| (act.at, *event, "self"));
            let first_absence = absence
                .iter()
                .filter_map(|event| Some((event.ts_orig.at?, *event, "absence")))
                .min_by_key(|(at, event, _)| (*at, stable(event)));
            let Some((suspended_at, trigger, source)) = first_switch
                .into_iter()
                .chain(first_absence)
                .min_by_key(|(at, event, _)| (*at, stable(event)))
            else {
                continue;
            };
            if absence.is_empty()
                && after.is_some_and(|(_, next)| {
                    next.at.unix_ns() - suspended_at.unix_ns() < 30 * SECOND
                })
            {
                absorbed_excursions += 1;
                continue;
            }
            let suspension = json!([episode, before.act, stable(trigger)]).to_string();
            let boundary: Vec<_> = [*before_event]
                .into_iter()
                .chain(after.map(|(event, _)| *event))
                .chain(between.iter().map(|(event, _)| *event))
                .chain(absence.iter().copied())
                .collect();
            outputs.push(derived(
                &WorkSuspension {
                    suspension: suspension.clone(),
                    episode: episode.clone(),
                    suspended_at,
                    returned_at: after.map(|(_, act)| act.at),
                    interruption_source: source.into(),
                    away_s: after.map(|(_, act)| {
                        (act.at.unix_ns() - suspended_at.unix_ns()) as f64 / SECOND as f64
                    }),
                    intervening_episodes: between
                        .iter()
                        .filter_map(|(_, act)| act.episode.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    absorbed_excursions: 0,
                },
                Self::NAME,
                boundary.iter().copied(),
            ));
            if let Some((event, act)) = after {
                outputs.push(derived(
                    &WorkResumption {
                        resumption: suspension.clone(),
                        episode: episode.clone(),
                        suspension: suspension.clone(),
                        resumed_at: act.at,
                        lag_s: None,
                        act: evidence(event),
                    },
                    Self::NAME,
                    boundary.iter().copied(),
                ));
                resumptions.push(suspension.clone());
            } else {
                pending = true;
            }
            parents.extend(boundary);
            suspensions.push(suspension);
        }
        let durations: Vec<_> = group
            .iter()
            .filter_map(|(_, act)| act.ended_at.map(|end| (act.at.unix_ns(), end.unix_ns())))
            .collect();
        let unknown = group
            .iter()
            .filter(|(_, act)| act.agency == Agency::Delegated && act.ended_at.is_none())
            .count() as u64;
        outputs.push(derived(
            &WorkEpisode {
                episode,
                objective: first.objective.clone().expect("objective key"),
                label: first.label.clone().expect("objective label"),
                label_source: first.label_source.clone(),
                state: if env.now.unix_ns() - last_activity.unix_ns() >= 14 * 86_400 * SECOND {
                    "dormant"
                } else if pending {
                    "suspended"
                } else {
                    "open"
                }
                .into(),
                started_at: first.at,
                last_activity,
                operator_s: None,
                agent_s: if durations.is_empty() {
                    None
                } else {
                    Some(union_seconds(durations))
                },
                untimed_agent_acts: unknown,
                trail: group.iter().map(|(event, _)| evidence(event)).collect(),
                suspensions,
                resumptions,
                absorbed_excursions,
                profile: "work/v1".into(),
            },
            Self::NAME,
            parents,
        ));
        Computed {
            outputs,
            waiting: env.now.unix_ns() - last_activity.unix_ns() < 14 * 86_400 * SECOND,
        }
    }
}

pub struct ContextPacks;
impl Keyed for ContextPacks {
    const NAME: &'static str = "context.packs";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["work.episode", "work.act"];
    const OUTPUTS: &'static [&'static str] = &["context.pack"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        input
            .payload
            .get("episode")
            .filter(|id| id.as_str().is_some())
            .map(|episode| vec![json!({"episode":episode})])
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        Some(
            InputQuery::all(Self::INPUTS.iter().map(|kind| Kind::all(kind)).collect())
                .only("episode", key.get("episode")?.clone()),
        )
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let Some((episode_event, episode)) = inputs
            .iter()
            .find_map(|event| Some((event, event.payload_as::<WorkEpisode>()?)))
        else {
            return Computed::default();
        };
        let mut acts: Vec<_> = inputs
            .iter()
            .filter_map(|event| Some((event, event.payload_as::<WorkAct>()?)))
            .collect();
        acts.sort_by_cached_key(|(event, act)| (act.at, stable(event)));
        let last_edit = acts
            .iter()
            .rev()
            .find(|(_, act)| matches!(act.action.as_str(), "file_edit" | "file_write"));
        Computed {
            outputs: vec![derived(
                &ContextPack {
                    pack: format!("resume:{}", episode.episode),
                    episode: episode.episode,
                    at: episode.last_activity,
                    stated_goal: acts
                        .iter()
                        .find(|(_, act)| act.agency == Agency::Operator)
                        .map(|(event, _)| evidence(event)),
                    trail: acts.iter().map(|(event, _)| evidence(event)).collect(),
                    last_edit: last_edit.map(|(event, _)| evidence(event)),
                    last_edit_path: last_edit.and_then(|(_, act)| act.path.clone()),
                    open_failures: acts
                        .iter()
                        .filter(|(_, act)| act.failed)
                        .map(|(event, _)| evidence(event))
                        .collect(),
                    coverage: vec![
                        "repository and branch anchors only; task and job links unavailable".into(),
                        "operator acts have no recorded duration".into(),
                        "failure resolution needs checks with a recorded revision".into(),
                    ],
                    profile: "resume/v1".into(),
                },
                Self::NAME,
                [episode_event]
                    .into_iter()
                    .chain(acts.iter().map(|(event, _)| *event)),
            )],
            waiting: false,
        }
    }
}

pub struct ActivityDays;
impl Keyed for ActivityDays {
    const NAME: &'static str = "activity.days";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["rest.fused", "activity.span"];
    const OUTPUTS: &'static [&'static str] = &["activity.day"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if input.event_type != "rest.fused" || input.payload["kind"] != "sleep" {
            return vec![];
        }
        input
            .payload
            .get("ended_at")
            .filter(|end| !end.is_null())
            .map(|end| vec![json!({"start":end})])
            .unwrap_or_default()
    }
    fn affected_ranges(&self, input: &KeyView<'_>, _: &Env) -> Vec<AffectedRange> {
        let end = input
            .payload
            .get("ended_at")
            .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok());
        input
            .ts_orig
            .map(|start| {
                vec![AffectedRange {
                    open_field: Some("provisional"),
                    output_type: "activity.day",
                    start_field: "started_at",
                    end_field: "ended_at",
                    start,
                    end: end.unwrap_or(start),
                }]
            })
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let start = serde_json::from_value::<Timestamp>(key.get("start")?.clone()).ok()?;
        let mut query = InputQuery::all(Self::INPUTS.iter().map(|kind| Kind::all(kind)).collect());
        query.range = Some(InputRange::UntilNext {
            start,
            kinds: vec![Kind::only("rest.fused", &[("kind", "sleep")])],
            field: "ended_at",
        });
        query.intervals = Self::INPUTS
            .iter()
            .map(|kind| (*kind, sinex_model::clock::IntervalEnd::At("ended_at")))
            .collect();
        Some(query)
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Open
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(start_key) = key
            .get("start")
            .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
        else {
            return Computed::default();
        };
        let mut wakes: Vec<_> = inputs
            .iter()
            .filter(|event| event.event_type == "rest.fused" && event.payload["kind"] == "sleep")
            .filter_map(|event| {
                Some((
                    serde_json::from_value::<sinex_model::Timestamp>(
                        event.payload.get("ended_at")?.clone(),
                    )
                    .ok()?,
                    event,
                ))
            })
            .filter(|(at, _)| *at <= env.now)
            .collect();
        wakes.sort_by_cached_key(|(at, event)| (*at, stable(event)));
        wakes.dedup_by_key(|(at, _)| *at);
        let mut outputs = Vec::new();
        for (index, (start, wake)) in wakes
            .iter()
            .enumerate()
            .filter(|(_, (start, _))| *start == start_key)
        {
            let end = wakes.get(index + 1).map(|(at, _)| *at).unwrap_or(env.now);
            let spans: Vec<_> = inputs
                .iter()
                .filter(|event| event.event_type == "activity.span")
                .filter_map(|event| {
                    Some((
                        event,
                        event.payload_as::<sinex_model::event_types::ActivitySpan>()?,
                    ))
                })
                .filter(|(_, span)| span.started_at < end && span.ended_at > *start)
                .collect();
            let mut boundaries: Vec<_> =
                [*start, end]
                    .into_iter()
                    .chain(spans.iter().flat_map(|(_, span)| {
                        [span.started_at.max(*start), span.ended_at.min(end)]
                    }))
                    .collect();
            boundaries.sort_unstable();
            boundaries.dedup();
            let mut agency_seconds = BTreeMap::<String, f64>::new();
            for pair in boundaries.windows(2) {
                let active: BTreeSet<String> = spans
                    .iter()
                    .filter(|(_, span)| span.started_at <= pair[0] && span.ended_at >= pair[1])
                    .map(|(_, span)| {
                        serde_json::to_value(&span.agency)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_owned()
                    })
                    .collect();
                // Other devices' passive or unknown signals do not duplicate
                // the operator's channel. Agent time is also reported below.
                let agency = [
                    "mixed",
                    "operator",
                    "supervising",
                    "passive",
                    "automated",
                    "delegated",
                    "unknown",
                ]
                .into_iter()
                .find(|agency| active.contains(*agency))
                .or(Some("unknown"));
                if let Some(agency) = agency {
                    *agency_seconds.entry(agency.into()).or_default() +=
                        (pair[1].unix_ns() - pair[0].unix_ns()) as f64 / SECOND as f64;
                }
            }
            let agent_intervals: Vec<_> = spans
                .iter()
                .filter(|(_, span)| span.delegated)
                .map(|(_, span)| {
                    (
                        span.started_at.max(*start).unix_ns(),
                        span.ended_at.min(end).unix_ns(),
                    )
                })
                .collect();
            let agent_s = (!agent_intervals.is_empty()).then(|| union_seconds(agent_intervals));
            outputs.push(derived(
                &ActivityDay {
                    day_id: start.unix_ns().to_string(),
                    started_at: *start,
                    ended_at: end,
                    boundary_method: "wake".into(),
                    provisional: index + 1 == wakes.len(),
                    agency_seconds,
                    agent_s,
                    profile: "wake-day/v1".into(),
                },
                Self::NAME,
                [*wake]
                    .into_iter()
                    .chain(wakes.get(index + 1).map(|(_, event)| *event))
                    .chain(spans.iter().map(|(event, _)| *event)),
            ));
        }
        let waiting = !outputs.is_empty() && !wakes.iter().any(|(at, _)| *at > start_key);
        Computed { outputs, waiting }
    }
}

#[cfg(test)]
mod tests;
