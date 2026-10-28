//! Exact project ownership from recorded acts and deployment-owned repository keys.
//! Ambiguous owners conserve time as unallocated; titles never establish ownership.
use crate::activity::{evidence, stable};
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::event_types::activity::{ActivitySpan, Agency};
use sinex_model::event_types::work::{ProjectShare, ProjectSpan, WorkAct};
use sinex_model::{Event, Timestamp};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AttributionProfile {
    #[serde(default)]
    pub projects: Vec<Value>,
}

pub struct ProjectSpans(pub AttributionProfile);

fn values<'a>(value: &'a Value, field: &str) -> impl Iterator<Item = &'a str> {
    value[field]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
}

fn within(path: &str, root: &str) -> bool {
    let path = std::path::Path::new(path);
    let root = std::path::Path::new(root);
    path.is_absolute()
        && root.is_absolute()
        && path.starts_with(root)
        && !path
            .components()
            .any(|part| part == std::path::Component::ParentDir)
        && !root
            .components()
            .any(|part| part == std::path::Component::ParentDir)
}

fn effective(root: &Value, at: Timestamp) -> bool {
    let time = |field| {
        root.get(field)
            .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
    };
    let valid_until = root.get("until").is_none_or(|until| until.is_null() || time("until").is_some());
    valid_until && time("from").is_some_and(|from| from <= at)
        && time("until").is_none_or(|until| at < until)
}

/// The highest recorded rung wins. Multiple projects at that rung stay conflicting.
fn owners(act: &WorkAct, registry: &[Value]) -> (Option<u8>, BTreeSet<String>) {
    for rung in 1..=3 {
        let projects: BTreeSet<_> = registry
            .iter()
            .filter(|project| match rung {
                1 => act.objective.as_ref().is_some_and(|objective| {
                    matches!(objective.kind.as_str(), "task" | "pull_request")
                        && values(project, "repositories").any(|repo| repo == objective.repository)
                }),
                2 => act.objective.as_ref().is_some_and(|objective| {
                    values(project, "repositories").any(|repo| repo == objective.repository)
                }),
                3 => {
                    act.git_identity.as_ref().is_some_and(|identity| {
                        values(project, "common_dirs").any(|key| key == identity.common_dir)
                            || values(project, "root_commits")
                                .any(|key| identity.root_commits.iter().any(|root| root == key))
                    }) || act.cwd.as_ref().is_some_and(|cwd| {
                        project["roots"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|root| {
                                root["path"].as_str().is_some_and(|path| within(cwd, path))
                                    && effective(root, act.at)
                            })
                    })
                }
                _ => false,
            })
            .filter_map(|project| project["project"].as_str().filter(|name| !name.is_empty()))
            .map(str::to_owned)
            .collect();
        if !projects.is_empty() {
            return (Some(rung), projects);
        }
    }
    (None, BTreeSet::new())
}

impl Keyed for ProjectSpans {
    const NAME: &'static str = "activity.projects";
    const VERSION: u32 = 1;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &["activity.span", "work.act"];
    const OUTPUTS: &'static [&'static str] = &["activity.project_span"];
    fn version(&self) -> u32 {
        let bytes = serde_json::to_vec(&(Self::VERSION, &self.0)).expect("profile serializes");
        let hash = blake3::hash(&bytes);
        1 + (u32::from_le_bytes(hash.as_bytes()[..4].try_into().unwrap()) >> 2)
    }
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        input
            .ts_orig
            .and_then(|at| env.time_settings.hour_start(at).ok())
            .map(|hour| vec![json!({"hour":hour})])
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let hour = serde_json::from_value(key.get("hour")?.clone()).ok()?;
        let mut query =
            InputQuery::window(Self::INPUTS, hour, env.time_settings.hour_end(hour).ok()?);
        query.intervals = vec![
            (
                "activity.span",
                sinex_model::clock::IntervalEnd::At("ended_at"),
            ),
            (
                "work.act",
                sinex_model::clock::IntervalEnd::EndOrInstant("ended_at"),
            ),
        ];
        Some(query)
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let registry = &self.0.projects;
        let mut spans: Vec<_> = inputs
            .iter()
            .filter_map(|event| Some((event, event.payload_as::<ActivitySpan>()?)))
            .filter(|(_, span)| matches!(span.agency, Agency::Operator | Agency::Supervising))
            .collect();
        spans.sort_by_cached_key(|(event, span)| (span.started_at, stable(event)));
        let mut outputs = Vec::new();
        // Form one operator-time union across devices; supervising is a separate
        // channel. Independent observers cannot multiply supported seconds.
        for agency in [Agency::Operator, Agency::Supervising] {
            let channel: Vec<_> = spans
                .iter()
                .filter(|(_, span)| span.agency == agency)
                .collect();
            let boundaries: BTreeSet<_> = channel
                .iter()
                .flat_map(|(_, span)| [span.started_at, span.ended_at])
                .collect();
            let boundaries: Vec<_> = boundaries.into_iter().collect();
            for pair in boundaries.windows(2) {
                let active: Vec<_> = channel
                    .iter()
                    .filter(|(_, span)| span.started_at <= pair[0] && span.ended_at >= pair[1])
                    .copied()
                    .collect();
                if active.is_empty() {
                    continue;
                }
                // Only acts explicitly cited by these spans may establish ownership.
                let acts: Vec<_> = inputs
                    .iter()
                    .filter(|act| {
                        active.iter().any(|(_, span)| {
                            span.agency_evidence.iter().any(|link| link.event == act.id)
                        })
                    })
                    .filter_map(|event| Some((event, event.payload_as::<WorkAct>()?)))
                    .filter(|(_, act)| match agency {
                        Agency::Operator => act.agency == Agency::Operator,
                        Agency::Supervising => act.agency == Agency::Delegated,
                        _ => false,
                    })
                    .collect();
                let resolutions: Vec<_> = acts
                    .iter()
                    .map(|(event, act)| (*event, owners(act, registry)))
                    .filter(|(_, (rung, _))| rung.is_some())
                    .collect();
                let rung = resolutions.iter().filter_map(|(_, (rung, _))| *rung).min();
                let candidates: BTreeSet<_> = resolutions
                    .iter()
                    .filter(|(_, (candidate, _))| *candidate == rung)
                    .flat_map(|(_, (_, projects))| projects.iter().cloned())
                    .collect();
                let owner = (candidates.len() == 1).then(|| candidates.first().unwrap().clone());
                let scopes: BTreeSet<_> = active.iter().map(|(_, span)| &span.scope).collect();
                outputs.push(derived(
                    &ProjectSpan {
                        project_span: json!([agency, pair[0], pair[1]]).to_string(),
                        scope: json!(scopes).to_string(),
                        started_at: pair[0],
                        ended_at: pair[1],
                        activity_spans: active.iter().map(|(event, _)| evidence(event)).collect(),
                        agency: agency.clone(),
                        shares: owner
                            .clone()
                            .map(|project| {
                                vec![ProjectShare {
                                    project,
                                    numerator: 1,
                                    denominator: 1,
                                }]
                            })
                            .unwrap_or_default(),
                        unallocated_numerator: u64::from(owner.is_none()),
                        denominator: 1,
                        ladder_rung: rung,
                        reason: if candidates.len() > 1 {
                            "conflicting"
                        } else if owner.is_some() {
                            "exact"
                        } else {
                            "unknown"
                        }
                        .into(),
                        conflicting_projects: if candidates.len() > 1 {
                            candidates.into_iter().collect()
                        } else {
                            vec![]
                        },
                        supported_seconds: (pair[1].unix_ns() - pair[0].unix_ns()) as f64 / 1e9,
                        profile: "project/exact-v1".into(),
                    },
                    Self::NAME,
                    active
                        .iter()
                        .map(|(event, _)| *event)
                        .chain(acts.iter().map(|(event, _)| *event)),
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

#[cfg(test)]
mod tests;
