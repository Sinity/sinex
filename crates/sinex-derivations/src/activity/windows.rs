use super::signals::{Piece, Signal, absence_scope, barrier, pieces};
use super::*;
use sinex_model::event_types::ActivityWindow;

pub struct ActivityWindows;

impl Keyed for ActivityWindows {
    const NAME: &'static str = "activity.windows";
    const VERSION: u32 = 2;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = INPUTS;
    const OUTPUTS: &'static [&'static str] = &["activity.window"];
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        hours(input, env)
    }
    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        inputs(key, env)
    }
    fn refresh_at(&self, outputs: &[Event], _: &Env) -> Option<Timestamp> {
        outputs
            .iter()
            .filter(|event| event.payload["provisional"] == true)
            .filter_map(|event| {
                let end: Timestamp =
                    serde_json::from_value(event.payload.get("ended_at")?.clone()).ok()?;
                Timestamp::from_unix_ns(end.unix_ns() + i128::from(300) * SECOND).ok()
            })
            .min()
    }
    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        key_state(key, env)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some((from, until)) = partition(key, env) else {
            return Computed::default();
        };
        let pieces = pieces(inputs);
        let absent: Vec<_> = pieces
            .iter()
            .filter(|piece| piece.signal == Signal::Absent)
            .collect();
        let mut by_scope: BTreeMap<&str, Vec<Piece<'_>>> = BTreeMap::new();
        for piece in pieces
            .iter()
            .filter(|piece| matches!(piece.signal, Signal::Operator | Signal::Physical))
        {
            // Exact absence boundaries divide supporting input. Conflicts stay
            // visible in elementary spans rather than extending an envelope.
            let mut remaining = vec![(piece.start, piece.end)];
            for absent in absent
                .iter()
                .filter(|absent| absence_scope(absent, &piece.scope))
            {
                remaining = remaining
                    .into_iter()
                    .flat_map(|(start, end)| {
                        if absent.end < start || absent.start > end {
                            return vec![(start, end)];
                        }
                        let mut out = Vec::new();
                        if start < absent.start {
                            out.push((start, end.min(absent.start)));
                        }
                        if end > absent.end {
                            out.push((start.max(absent.end), end));
                        }
                        out
                    })
                    .collect();
            }
            for (start, end) in remaining {
                by_scope.entry(&piece.scope).or_default().push(Piece {
                    start,
                    end,
                    ..piece.clone()
                });
            }
        }
        let mut outputs = Vec::new();
        let mut waiting = false;
        for (scope, mut active) in by_scope {
            active.sort_by_cached_key(|piece| (piece.start, piece.end, stable(piece.event)));
            let mut groups: Vec<Vec<Piece<'_>>> = Vec::new();
            let mut latest = i128::MIN;
            for piece in active {
                let cut = absent
                    .iter()
                    .any(|p| absence_scope(p, scope) && barrier(p, latest, piece.start));
                if groups.is_empty() || piece.start - latest >= GAP_NS || cut {
                    latest = piece.end;
                    groups.push(vec![piece]);
                } else {
                    latest = latest.max(piece.end);
                    groups.last_mut().unwrap().push(piece);
                }
            }
            for group in &groups {
                let start = group[0].start;
                let end = group.iter().map(|p| p.end).max().unwrap();
                if start >= until || end < from {
                    continue;
                }
                let clipped_start = start.max(from);
                let clipped_end = end.min(until);
                let next_cut = absent
                    .iter()
                    .filter(|p| {
                        absence_scope(p, scope) && p.signal == Signal::Absent && p.start >= end
                    })
                    .min_by_key(|p| p.start);
                let reason = if let Some(cut) = next_cut.filter(|p| p.start - end < GAP_NS) {
                    cut.reason
                } else if env.now.unix_ns() - end >= GAP_NS {
                    Some("gap")
                } else {
                    None
                };
                let continues_after =
                    end > until || (end == until && group.iter().any(|piece| piece.start == until));
                let provisional = reason.is_none() && !continues_after;
                waiting |= provisional;
                let context: Vec<_> = inputs
                    .iter()
                    .filter(|event| {
                        super::scope(&event.payload, &event.event_type) == scope
                            && event.ts_orig.at.is_some_and(|at| {
                                clipped_start <= at.unix_ns() && at.unix_ns() <= clipped_end
                            })
                    })
                    .collect();
                let mut apps: Vec<_> = context
                    .iter()
                    .filter_map(|event| {
                        event
                            .payload
                            .get("app")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect();
                apps.sort();
                apps.dedup();
                let mut parents: Vec<&Event> = group
                    .iter()
                    .map(|piece| piece.event)
                    .chain(context.iter().copied())
                    .collect();
                // Absence also determines which first record owns a group.
                parents.extend(
                    absent
                        .iter()
                        .filter(|piece| absence_scope(piece, scope))
                        .map(|piece| piece.event),
                );
                if let Some(cut) = next_cut.filter(|p| p.start - end < GAP_NS) {
                    parents.push(cut.event);
                }
                let window = ActivityWindow {
                    window_id: json!([
                        scope,
                        stable(group[0].event),
                        timestamp(from),
                        timestamp(clipped_start)
                    ])
                    .to_string(),
                    scope: scope.into(),
                    day: env
                        .time_settings
                        .day(timestamp(clipped_start))
                        .unwrap()
                        .to_string(),
                    partition_start: timestamp(from),
                    partition_end: timestamp(until),
                    started_at: timestamp(clipped_start),
                    ended_at: timestamp(clipped_end),
                    counts: counts(context.iter().copied(), from, until),
                    apps,
                    close_reason: if end > until
                        || (end == until && group.iter().any(|piece| piece.start == until))
                    {
                        None
                    } else {
                        reason.map(str::to_owned)
                    },
                    continuation_before: start < from,
                    continuation_after: end > until
                        || (end == until && group.iter().any(|piece| piece.start == until)),
                    provisional,
                    operator_present_s: union_seconds(
                        group
                            .iter()
                            .map(|p| (p.start.max(from), p.end.min(until)))
                            .collect(),
                    ),
                    operator_intervals: union_ranges(
                        group
                            .iter()
                            .map(|p| (p.start.max(from), p.end.min(until)))
                            .filter(|(start, end)| {
                                start < end || (*start < until && *start >= from)
                            })
                            .collect(),
                    )
                    .into_iter()
                    .map(|(start, end)| (timestamp(start), timestamp(end)))
                    .collect(),
                };
                outputs.push(derived(&window, Self::NAME, parents));
            }
        }
        Computed {
            error: None,
            outputs,
            waiting,
        }
    }
}
