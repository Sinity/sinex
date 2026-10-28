use super::signals::{Signal, absence_scope, pieces};
use super::*;
use sinex_model::event_types::activity::{ActivitySpan, Agency};

pub struct ActivitySpans;

impl Keyed for ActivitySpans {
    const NAME: &'static str = "activity.spans";
    const VERSION: u32 = 5;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = INPUTS;
    const OUTPUTS: &'static [&'static str] = &["activity.span"];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        hours(input, env)
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        inputs(key, env)
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        key_state(key, env)
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some((from, until)) = partition(key, env) else {
            return Computed::default();
        };
        let pieces = pieces(inputs);
        let mut scopes = BTreeMap::<&str, Vec<_>>::new();
        for piece in &pieces {
            scopes.entry(&piece.scope).or_default().push(piece);
        }
        let absence: Vec<_> = pieces
            .iter()
            .filter(|piece| piece.signal == Signal::Absent)
            .collect();
        let mut outputs = Vec::new();
        for (scope, mut scoped) in scopes {
            for piece in &absence {
                if piece.scope != scope && absence_scope(piece, scope) {
                    scoped.push(*piece);
                }
            }
            let pieces = scoped;
            let mut changes = BTreeMap::<i128, (Vec<usize>, Vec<usize>)>::new();
            let mut point_events = BTreeMap::<i128, Vec<&Event>>::new();
            for (index, piece) in pieces.iter().enumerate() {
                if piece.signal == Signal::Physical {
                    point_events
                        .entry(piece.start)
                        .or_default()
                        .push(piece.event);
                    if from <= piece.start && piece.start < until {
                        changes.entry(piece.start).or_default();
                    }
                }
                let start = piece.start.max(from);
                let end = piece.end.min(until);
                if end > start {
                    changes.entry(start).or_default().0.push(index);
                    changes.entry(end).or_default().1.push(index);
                }
            }
            let boundaries: Vec<_> = changes.keys().copied().collect();
            let mut live = std::collections::BTreeSet::new();
            for pair in boundaries.windows(2) {
                let (start, end) = (pair[0], pair[1]);
                let (enter, leave) = &changes[&start];
                for index in leave {
                    live.remove(index);
                }
                for index in enter {
                    live.insert(*index);
                }
                let active: Vec<_> = live.iter().map(|index| pieces[*index]).collect();
                if active.is_empty() {
                    continue;
                }
                let has = |signal| active.iter().any(|piece| piece.signal == signal);
                let operator = has(Signal::Operator);
                let exact_absence = has(Signal::Absent);
                let physical_coverage = has(Signal::Coverage);
                let no_input = has(Signal::NoInput) && !operator;
                let absent_claim = active.iter().any(|piece| {
                    piece.signal == Signal::Claim && piece.event.payload["status"] == "afk"
                });
                let absent =
                    exact_absence || no_input || (!physical_coverage && !operator && absent_claim);
                let agency = if operator && exact_absence {
                    Agency::Mixed
                } else if operator {
                    Agency::Operator
                } else if has(Signal::Delegated) {
                    Agency::Delegated
                } else if has(Signal::Playback) {
                    Agency::Passive
                } else {
                    Agency::Unknown
                };
                let mut agency_evidence: Vec<_> = active
                    .iter()
                    .filter(|piece| {
                        !matches!(
                            piece.signal,
                            Signal::Focus | Signal::Claim | Signal::Coverage
                        )
                    })
                    .map(|piece| evidence(piece.event))
                    .collect();
                agency_evidence.sort_by_key(|evidence| evidence.record.clone());
                agency_evidence.dedup_by(|first, next| first.record == next.record);
                // Both sides of a boundary are dependencies, including an
                // absence which ended here and input points with no duration.
                let boundary = changes[&start]
                    .0
                    .iter()
                    .chain(&changes[&start].1)
                    .chain(&changes[&end].0)
                    .chain(&changes[&end].1)
                    .map(|index| pieces[*index].event);
                let points = point_events
                    .range(start..end)
                    .flat_map(|(_, events)| events.iter().copied());
                let mut parents: Vec<_> = active
                    .iter()
                    .map(|piece| piece.event)
                    .chain(boundary)
                    .chain(points)
                    .collect();
                parents.sort_by_key(|event| stable(event));
                parents.dedup_by_key(|event| event.id);
                let strings = |field: &str, signal| {
                    let mut values: Vec<_> = active
                        .iter()
                        .filter(|piece| piece.signal == signal)
                        .filter_map(|piece| {
                            piece
                                .event
                                .payload
                                .get(field)
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        })
                        .collect();
                    values.sort();
                    values.dedup();
                    values
                };
                let mut claims = strings("status", Signal::Claim);
                claims.extend(strings("state", Signal::Claim));
                claims.sort();
                claims.dedup();
                let span = ActivitySpan {
                    span_id: json!([
                        scope,
                        timestamp(from),
                        timestamp(start),
                        parents
                            .iter()
                            .filter(|event| event.ts_orig.at == Some(timestamp(start)))
                            .map(|event| stable(event))
                            .collect::<Vec<_>>()
                    ])
                    .to_string(),
                    scope: scope.into(),
                    day: env.time_settings.day(timestamp(start)).unwrap().to_string(),
                    partition_start: timestamp(from),
                    partition_end: timestamp(until),
                    started_at: timestamp(start),
                    ended_at: timestamp(end),
                    window: None,
                    agency,
                    delegated: has(Signal::Delegated),
                    agency_evidence,
                    presence: if operator && exact_absence {
                        "mixed"
                    } else if operator {
                        "present"
                    } else if absent {
                        "absent"
                    } else {
                        "unknown"
                    }
                    .into(),
                    presence_method: if exact_absence {
                        "exact"
                    } else if operator
                        && active.iter().any(|piece| {
                            piece.signal == Signal::Operator && piece.reason == Some("recorded_act")
                        })
                    {
                        "recorded_act"
                    } else if operator
                        && active
                            .iter()
                            .any(|piece| piece.reason == Some("reading_gap"))
                    {
                        "bracketed_quiet"
                    } else if operator || no_input {
                        "physical"
                    } else if !claims.is_empty() {
                        "claim_reconciled"
                    } else {
                        "unknown"
                    }
                    .into(),
                    presence_claims: claims,
                    focus: strings("record", Signal::Focus),
                    apps: strings("app", Signal::Focus),
                    titles: strings("title", Signal::Focus),
                    display: if active
                        .iter()
                        .any(|piece| piece.reason == Some("display_off"))
                    {
                        "off"
                    } else {
                        "unknown"
                    }
                    .into(),
                    playback: has(Signal::Playback),
                    input_records: u64::try_from(
                        point_events
                            .range(start..end)
                            .map(|(_, events)| events.len())
                            .sum::<usize>(),
                    )
                    .expect("input count fits u64"),
                    operator_present_s: if operator && !absent {
                        (end - start) as f64 / SECOND as f64
                    } else {
                        0.0
                    },
                    profile: "presence/v1".into(),
                    continuation_before: active.iter().any(|piece| piece.start < from),
                    continuation_after: active.iter().any(|piece| piece.end > until),
                    provisional: active
                        .iter()
                        .any(|piece| piece.event.payload.get("ended_at") == Some(&Value::Null)),
                };
                outputs.push(derived(&span, Self::NAME, parents));
            }
        }
        Computed {
            outputs,
            waiting: false,
        }
    }
}
