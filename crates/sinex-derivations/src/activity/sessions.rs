use super::*;
use sinex_model::event_types::activity::{ActivityMember, ActivitySession, ActivityWindow};

/// The break threshold groups presence envelopes, without adding focus time.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityProfile {
    #[serde(default = "default_break", deserialize_with = "positive_break")]
    pub break_seconds: u64,
}
fn default_break() -> u64 {
    900
}
fn positive_break<'de, D: serde::Deserializer<'de>>(de: D) -> Result<u64, D::Error> {
    let seconds = <u64 as serde::Deserialize>::deserialize(de)?;
    if seconds == 0 {
        return Err(serde::de::Error::custom(
            "the activity break threshold must be positive",
        ));
    }
    Ok(seconds)
}
impl Default for ActivityProfile {
    fn default() -> Self {
        Self { break_seconds: 900 }
    }
}
fn selection_time(ns: i128) -> Timestamp {
    Timestamp::from_unix_ns(ns).unwrap_or_else(|_| {
        Timestamp(if ns < 0 {
            time::PrimitiveDateTime::MIN.assume_utc()
        } else {
            time::PrimitiveDateTime::MAX.assume_utc()
        })
    })
}

pub struct ActivitySessions(pub ActivityProfile);

impl Keyed for ActivitySessions {
    const NAME: &'static str = "activity.sessions";
    const VERSION: u32 = 3;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &[
        "activity.window",
        "activity.span",
        "state.interval",
        "rest.fused",
        "journal.entry",
    ];
    const OUTPUTS: &'static [&'static str] = &["activity.session"];
    fn version(&self) -> u32 {
        let mut hash = blake3::Hasher::new();
        hash.update(&Self::VERSION.to_le_bytes());
        hash.update(&self.0.break_seconds.to_le_bytes());
        let bytes: [u8; 4] = hash.finalize().as_bytes()[..4].try_into().unwrap();
        1 + (u32::from_le_bytes(bytes) >> 2)
    }
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        if input.event_type != "activity.window" {
            return vec![];
        }
        let (Some(at), Some(scope)) = (input.payload.get("started_at"), input.payload.get("scope"))
        else {
            return vec![];
        };
        vec![json!({"at":at,"scope":scope})]
    }
    fn nearby_inputs(&self, input: &KeyView<'_>, _: &Env) -> Option<InputQuery> {
        if input.event_type != "activity.window" {
            return None;
        }
        let start = input.ts_orig?;
        let end = input
            .payload
            .get("ended_at")
            .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
            .unwrap_or(start);
        let gap = i128::from(self.0.break_seconds) * SECOND;
        Some(
            InputQuery::window(
                &["activity.window"],
                selection_time(start.unix_ns() - gap),
                selection_time(end.unix_ns() + gap + 1),
            )
            .only("scope", input.payload.get("scope")?.clone()),
        )
    }
    fn affected_ranges(&self, input: &KeyView<'_>, _: &Env) -> Vec<crate::AffectedRange> {
        if !relevant(input.event_type, input.payload) {
            return vec![];
        }
        input
            .ts_orig
            .map(|start| {
                let gap = i128::from(self.0.break_seconds) * SECOND;
                let end = input
                    .payload
                    .get("ended_at")
                    .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
                    .unwrap_or(start);
                vec![crate::AffectedRange {
                    open_field: None,
                    output_type: "activity.session",
                    start_field: "started_at",
                    end_field: "ended_at",
                    start: selection_time(start.unix_ns() - gap),
                    end: selection_time(end.unix_ns() + gap),
                }]
            })
            .unwrap_or_default()
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let at = serde_json::from_value::<Timestamp>(key.get("at")?.clone()).ok()?;
        let scope = key.get("scope")?;
        let mut selected = crate::Kind::all("activity.window");
        selected.equal.push(("scope", scope.clone()));
        let mut query = InputQuery::all(kinds(Self::INPUTS));
        for kind in &mut query.kinds {
            if kind.event_type == "activity.window" {
                kind.equal.push(("scope", scope.clone()));
            }
        }
        query.range = Some(crate::InputRange::Connected {
            kinds: vec![selected],
            at,
            end_field: "ended_at",
            gap_ns: i128::from(self.0.break_seconds) * SECOND,
        });
        query.intervals = Self::INPUTS
            .iter()
            .filter_map(|kind| interval_end(kind).map(|end| (*kind, end)))
            .collect();
        query.intervals.extend([
            ("activity.window", IntervalEnd::At("ended_at")),
            ("activity.span", IntervalEnd::At("ended_at")),
        ]);
        Some(query)
    }

    fn refresh_at(&self, outputs: &[Event], _: &Env) -> Option<Timestamp> {
        outputs
            .iter()
            .filter(|event| event.payload["provisional"] == true)
            .filter_map(|event| {
                let end: Timestamp =
                    serde_json::from_value(event.payload.get("ended_at")?.clone()).ok()?;
                Timestamp::from_unix_ns(end.unix_ns() + i128::from(self.0.break_seconds) * SECOND)
                    .ok()
            })
            .min()
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Open
    }
    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let threshold = i128::from(self.0.break_seconds) * SECOND;
        let mut by_scope = BTreeMap::<String, Vec<_>>::new();
        for (event, window) in inputs
            .iter()
            .filter_map(|event| Some((event, event.payload_as::<ActivityWindow>()?)))
        {
            if window.ended_at >= window.started_at {
                by_scope
                    .entry(window.scope.clone())
                    .or_default()
                    .push((event, window));
            }
        }
        let barriers = super::signals::pieces(inputs);
        let mut outputs = Vec::new();
        let mut waiting = false;
        for (scope, mut windows) in by_scope {
            windows.sort_by_key(|(event, window)| (window.started_at, stable(event)));
            let mut groups: Vec<Vec<(&Event, ActivityWindow)>> = Vec::new();
            let mut end = i128::MIN;
            for (event, window) in windows {
                let boundary = barriers.iter().any(|piece| {
                    super::signals::absence_scope(piece, &scope)
                        && super::signals::barrier(piece, end, window.started_at.unix_ns())
                });
                let closed = boundary
                    || groups
                        .last()
                        .and_then(|group| group.last())
                        .and_then(|(_, previous)| previous.close_reason.as_deref())
                        .is_some_and(|reason| reason != "gap");
                if groups.is_empty() || window.started_at.unix_ns() - end >= threshold || closed {
                    end = window.ended_at.unix_ns();
                    groups.push(vec![(event, window)]);
                } else {
                    end = end.max(window.ended_at.unix_ns());
                    groups.last_mut().unwrap().push((event, window));
                }
            }
            for group in &groups {
                let first = &group[0].1;
                if key.get("at").is_some()
                    && (key["at"] != json!(first.started_at) || key["scope"] != first.scope)
                {
                    continue;
                }
                let end = group
                    .iter()
                    .map(|(_, window)| window.ended_at)
                    .max()
                    .unwrap();
                let last = &group.last().unwrap().1;
                let boundary = barriers
                    .iter()
                    .filter(|piece| {
                        super::signals::absence_scope(piece, &scope)
                            && piece.signal == super::signals::Signal::Absent
                            && piece.start >= end.unix_ns()
                            && piece.start - end.unix_ns() < threshold
                    })
                    .min_by_key(|piece| piece.start);
                let close = boundary
                    .and_then(|piece| piece.reason)
                    .or(last
                        .close_reason
                        .as_deref()
                        .filter(|reason| *reason != "gap"))
                    .map(str::to_owned)
                    .or_else(|| {
                        (env.now.unix_ns() - end.unix_ns() >= threshold).then(|| "gap".into())
                    });
                let provisional = close.is_none();
                waiting |= provisional;
                let mut members = Vec::new();
                let mut counts = ActivityCounts::default();
                for (_, window) in group {
                    members.push(ActivityMember {
                        kind: "window".into(),
                        window: Some(window.window_id.clone()),
                        started_at: window.started_at,
                        ended_at: window.ended_at,
                    });
                    for (total, part) in [
                        (&mut counts.records, &window.counts.records),
                        (&mut counts.sources, &window.counts.sources),
                        (&mut counts.observers, &window.counts.observers),
                    ] {
                        for (name, count) in part {
                            *total.entry(name.clone()).or_default() += count;
                        }
                    }
                }
                let support = union_ranges(
                    group
                        .iter()
                        .flat_map(|(_, window)| {
                            window
                                .operator_intervals
                                .iter()
                                .map(|(start, end)| (start.unix_ns(), end.unix_ns()))
                        })
                        .collect(),
                );
                let operator_present_s = union_seconds(support.clone());
                for pair in support.windows(2) {
                    if pair[1].0 - pair[0].1 >= 120 * SECOND {
                        members.push(ActivityMember {
                            kind: "short_break".into(),
                            window: None,
                            started_at: timestamp(pair[0].1),
                            ended_at: timestamp(pair[1].0),
                        });
                    }
                }
                members.sort_by_key(|member| (member.started_at, member.kind.clone()));
                let spans: Vec<_> = inputs
                    .iter()
                    .filter_map(|event| {
                        let span = event.payload_as::<sinex_model::event_types::ActivitySpan>()?;
                        (span.scope == scope
                            && span.started_at < end
                            && span.ended_at > first.started_at)
                            .then_some((event, span))
                    })
                    .collect();
                let mut agency_seconds = BTreeMap::<String, f64>::new();
                for (_, span) in &spans {
                    let name = serde_json::to_value(&span.agency)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_owned();
                    *agency_seconds.entry(name).or_default() += (span.ended_at.min(end).unix_ns()
                        - span.started_at.max(first.started_at).unix_ns())
                        as f64
                        / SECOND as f64;
                }
                // Delegated work can run on another host while the operator
                // uses this seat. It is concurrent, never subtracted from it.
                let delegated: Vec<_> = inputs
                    .iter()
                    .filter_map(|event| {
                        let span = event.payload_as::<sinex_model::event_types::ActivitySpan>()?;
                        (span.delegated
                            && span.started_at < end
                            && span.ended_at > first.started_at)
                            .then_some((event, span))
                    })
                    .collect();
                let delegated_overlap_s = (!delegated.is_empty()).then(|| {
                    union_seconds(
                        delegated
                            .iter()
                            .map(|(_, span)| {
                                (
                                    span.started_at.max(first.started_at).unix_ns(),
                                    span.ended_at.min(end).unix_ns(),
                                )
                            })
                            .collect(),
                    )
                });
                let session = ActivitySession {
                    session_id: json!([scope, first.window_id]).to_string(),
                    scope: scope.clone(),
                    day: first.day.clone(),
                    started_at: first.started_at,
                    ended_at: end,
                    windows: u64::try_from(group.len()).expect("window count fits u64"),
                    short_breaks: u64::try_from(
                        members
                            .iter()
                            .filter(|member| member.kind == "short_break")
                            .count(),
                    )
                    .unwrap(),
                    members,
                    counts,
                    close_reason: close,
                    provisional,
                    operator_present_s,
                    agency_seconds,
                    delegated_overlap_s,
                    profile: "activity/v1".into(),
                    break_seconds: self.0.break_seconds,
                };
                outputs.push(derived(
                    &session,
                    Self::NAME,
                    group
                        .iter()
                        .map(|(event, _)| *event)
                        .chain(spans.iter().map(|(event, _)| *event))
                        .chain(delegated.iter().map(|(event, _)| *event))
                        .chain(boundary.map(|piece| piece.event))
                        .chain(
                            barriers
                                .iter()
                                .filter(|piece| {
                                    super::signals::absence_scope(piece, &scope)
                                        && piece.signal == super::signals::Signal::Absent
                                        && piece.start <= first.started_at.unix_ns()
                                        && piece.end >= first.started_at.unix_ns() - threshold
                                })
                                .map(|piece| piece.event),
                        ),
                ));
            }
        }
        Computed { outputs, waiting }
    }
}
