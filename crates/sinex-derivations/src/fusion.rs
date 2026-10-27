//! Fusion: one event for each thing several sources observed. A redundant
//! source is the point, not noise: shell histories, the compositor and
//! ActivityWatch, a browser's history and its Takeout export, live capture and
//! the Sinnix capture lake each see some of the same things, and each sees
//! some the others missed. Every fused kind is declared as a [`FusionRule`]
//! in [`rules`]; the matching below is the same for all of them.
//!
//! A [`FusionReader`] reads one source's events as [`Sighting`]s: what must be
//! equal for two sightings to be the same thing (`identity`), when it began,
//! when it ended if the source knows, and the output fields it can supply.
//! Fusion readers are listed in preference order. Sightings of one identity from
//! different readers match:
//!
//! 1. strong pass: the globally closest pairs within `window`, closest first
//!    (ties by preference, then commit order);
//! 2. fallback pass: among sightings still alone, pairs within `fallback`
//!    where each side has exactly one possible partner;
//! 3. with `absorb` set, a group whose first start falls inside a group of
//!    the same identity led by a more preferred reader joins it, as does a
//!    lone sighting inside one led by the same reader: a window title change
//!    inside a focus the compositor reported, a player's reports of a track
//!    the streaming export holds, a player's repeated report of one track. A
//!    group's extent runs from `window` before its first start to its latest
//!    end (a span still open reaches `absorb`), and no later than `absorb`
//!    after its first start.
//! 4. every other sighting is fused alone.
//!
//! Pairing puts at most one sighting per reader in a group. The fused
//! event takes its start and end from the group's most preferred member,
//! every other field from the most preferred member that has it, and names
//! every member as a parent. Sightings with an unknown time never match and
//! never fuse.
//!
//! The key is the UTC hour a group's first sighting starts in. Each hour reads
//! a margin on both sides wide enough for any group that reaches into it, and
//! emits only groups whose first sighting starts inside it, so a group
//! straddling an hour edge is emitted exactly once.
use crate::derived_event::{derived_json, record_identity};
use crate::{AnyKeyed, Computed, Env, InputQuery, KeyState, KeyView, Kind};
use serde_json::{Map, Value, json};
use sinex_model::{Event, RecordState, Timestamp};

pub mod rules;

/// How one source's events are read for a fusion rule.
#[derive(Clone, Copy)]
pub struct FusionReader {
    pub event_type: &'static str,
    /// Payload fields an event must have these values in to be read.
    pub only: &'static [(&'static str, &'static str)],
    /// The one source whose events it reads, when several sources write the
    /// same type (mail from IMAP and from an mbox); `None` reads any.
    pub source: Option<&'static str>,
    /// Whether sightings are spans, so one without an end is still open.
    pub spans: bool,
    pub read: fn(&Event) -> Option<Sighting>,
}

impl FusionReader {
    fn admits(&self, event_type: &str, payload: &Value) -> bool {
        self.event_type == event_type
            && self
                .only
                .iter()
                .all(|(f, v)| payload.get(*f).and_then(Value::as_str) == Some(*v))
    }
}

/// One source's sighting of something, ready to match.
#[derive(Clone, Debug, PartialEq)]
pub struct Sighting {
    /// What must be equal for two sightings to be the same thing.
    pub identity: String,
    pub start: Timestamp,
    pub end: Option<Timestamp>,
    /// Output payload fields this sighting supplies.
    pub fields: Map<String, Value>,
}

/// One fused kind: its readers in preference order and how they match.
#[derive(Clone, Copy)]
pub struct FusionRule {
    /// The derivation's registered name.
    pub derivation: &'static str,
    pub version: u32,
    /// The fused event type.
    pub output: &'static str,
    pub readers: &'static [FusionReader],
    /// The readers' event types, once each (a test checks the list).
    pub event_types: &'static [&'static str],
    pub window_ns: i128,
    pub fallback_ns: i128,
    /// How long after a group's first start a later group of the same
    /// identity can join it; `None` when every sighting is a thing of its own
    /// (two runs of `ls` are two commands).
    pub absorb_ns: Option<i128>,
    /// The output fields the group's start and end are written to.
    pub start_field: &'static str,
    pub end_field: Option<&'static str>,
}

const HOUR_NS: i128 = 3_600_000_000_000;
/// Sources fill a record in after it starts (Atuin's exit code, Chromium's
/// visit duration); a key is settled this long after its margin has passed.
const SETTLE_NS: i128 = 900_000_000_000;

impl FusionRule {
    /// How far outside its hour a key reads, and how far from a sighting the
    /// hours it affects reach.
    fn margin(&self) -> i128 {
        self.window_ns + self.fallback_ns + self.absorb_ns.unwrap_or(0)
    }

    fn reader(&self, event_type: &str, payload: &Value) -> Option<usize> {
        self.readers
            .iter()
            .position(|o| o.admits(event_type, payload))
    }
}

/// One sighting during matching.
struct Seen<'a> {
    event: &'a Event,
    rank: usize,
    spans: bool,
    sighting: Sighting,
    at: i128,
}

impl Seen<'_> {
    fn end(&self, absorb_ns: i128) -> i128 {
        match self.sighting.end {
            Some(end) => end.unix_ns().max(self.at),
            None if self.spans => self.at + absorb_ns,
            None => self.at,
        }
    }
}

fn read<'a>(rule: &FusionRule, event: &'a Event) -> Option<Seen<'a>> {
    if event.record_state == RecordState::Removed {
        return None;
    }
    let rank = rule.readers.iter().position(|o| {
        o.admits(event.event_type, &event.payload) && o.source.is_none_or(|s| s == event.source)
    })?;
    let reader = &rule.readers[rank];
    let sighting = (reader.read)(event)?;
    Some(Seen {
        event,
        rank,
        spans: reader.spans,
        at: sighting.start.unix_ns(),
        sighting,
    })
}

/// Group sightings, sorted by time, by the rule. Returns groups of indices
/// into `seen`, each sorted by preference, then time.
fn group(seen: &[Seen<'_>], rule: &FusionRule) -> Vec<Vec<usize>> {
    let n = seen.len();
    let mut group_of: Vec<usize> = (0..n).collect();
    let mut members: Vec<Vec<usize>> = (0..n).map(|i| vec![i]).collect();
    let compatible =
        |a: &Seen<'_>, b: &Seen<'_>| a.rank != b.rank && a.sighting.identity == b.sighting.identity;

    // Possible pairs, closest first. Sightings are sorted by time, so the
    // inner loop stops once the gap exceeds the fallback window.
    let mut pairs: Vec<(i128, usize, usize)> = Vec::new();
    for i in 0..n {
        for j in i + 1..n {
            let dt = seen[j].at - seen[i].at;
            if dt > rule.fallback_ns {
                break;
            }
            if compatible(&seen[i], &seen[j]) {
                pairs.push((dt, i, j));
            }
        }
    }
    pairs.sort_by_key(|&(dt, i, j)| (dt, seen[i].rank.min(seen[j].rank), i, j));

    let merge = |i: usize, j: usize, members: &mut Vec<Vec<usize>>, group_of: &mut Vec<usize>| {
        let (gi, gj) = (group_of[i], group_of[j]);
        if gi == gj {
            return;
        }
        let ranks: Vec<usize> = members[gi].iter().map(|&m| seen[m].rank).collect();
        if members[gj].iter().any(|&m| ranks.contains(&seen[m].rank)) {
            return;
        }
        let all: Vec<usize> = members[gi].iter().chain(&members[gj]).copied().collect();
        let lo = all.iter().map(|&m| seen[m].at).min().unwrap_or(0);
        let hi = all.iter().map(|&m| seen[m].at).max().unwrap_or(0);
        if hi - lo > rule.window_ns && all.len() > 2 {
            return;
        }
        let moved = std::mem::take(&mut members[gj]);
        for m in &moved {
            group_of[*m] = gi;
        }
        members[gi].extend(moved);
    };

    // Strong pass.
    for &(dt, i, j) in &pairs {
        if dt <= rule.window_ns {
            merge(i, j, &mut members, &mut group_of);
        }
    }

    // Fallback pass: only between sightings still alone, and only when each
    // side has exactly one such partner.
    let alone = |i: usize, members: &Vec<Vec<usize>>, group_of: &Vec<usize>| {
        members[group_of[i]].len() == 1
    };
    let mut partners: Vec<usize> = vec![0; n];
    let fallback: Vec<(usize, usize)> = pairs
        .iter()
        .filter(|&&(dt, i, j)| {
            dt > rule.window_ns && alone(i, &members, &group_of) && alone(j, &members, &group_of)
        })
        .map(|&(_, i, j)| (i, j))
        .collect();
    for &(i, j) in &fallback {
        partners[i] += 1;
        partners[j] += 1;
    }
    for &(i, j) in &fallback {
        if partners[i] == 1 && partners[j] == 1 {
            merge(i, j, &mut members, &mut group_of);
        }
    }

    if let Some(absorb_ns) = rule.absorb_ns {
        absorb(seen, rule.window_ns, absorb_ns, &mut members, &mut group_of);
    }

    let mut groups: Vec<Vec<usize>> = members.into_iter().filter(|g| !g.is_empty()).collect();
    for g in &mut groups {
        g.sort_by_key(|&m| (seen[m].rank, seen[m].at));
    }
    groups
}

/// Join groups, in order of their first start, to the group of their
/// identity whose extent holds that start and whose lead is more preferred
/// (or equally preferred, for a lone sighting): the most preferred such
/// group, then the one starting nearest to it.
fn absorb(
    seen: &[Seen<'_>],
    window_ns: i128,
    absorb_ns: i128,
    members: &mut [Vec<usize>],
    group_of: &mut [usize],
) {
    let first_of = |group: &[usize]| group.iter().map(|&m| seen[m].at).min().unwrap_or(0);
    let lead_of = |group: &[usize]| {
        group
            .iter()
            .copied()
            .min_by_key(|&m| (seen[m].rank, seen[m].at))
            .expect("groups are not empty")
    };
    // Groups by first start. A group's first start only moves earlier, by
    // less than `window`, as it absorbs; the search widens by that much.
    let mut by_first: Vec<(i128, usize)> = members
        .iter()
        .enumerate()
        .filter(|(_, g)| !g.is_empty())
        .map(|(g, group)| (first_of(group), g))
        .collect();
    by_first.sort_unstable();
    for &(_, joiner) in &by_first.clone() {
        if members[joiner].is_empty() {
            continue;
        }
        let at = first_of(&members[joiner]);
        let lead = lead_of(&members[joiner]);
        let alone = members[joiner].len() == 1;
        let from = by_first.partition_point(|&(first, _)| first < at - absorb_ns - window_ns);
        let mut best: Option<((usize, i128), usize)> = None;
        for &(_, g) in by_first[from..]
            .iter()
            .take_while(|&&(first, _)| first <= at + 2 * window_ns)
        {
            let group = &members[g];
            if g == joiner || group.is_empty() {
                continue;
            }
            let target = lead_of(group);
            let preferred = seen[target].rank < seen[lead].rank
                || (alone && seen[target].rank == seen[lead].rank);
            if !preferred || seen[target].sighting.identity != seen[lead].sighting.identity {
                continue;
            }
            let first = first_of(group);
            let last = group
                .iter()
                .map(|&m| seen[m].end(absorb_ns))
                .max()
                .unwrap_or(first);
            if at < first - window_ns || at >= last.max(first + window_ns) || at - first > absorb_ns
            {
                continue;
            }
            let rank = (seen[target].rank, (at - first).abs());
            if best.is_none_or(|(r, _)| rank < r) {
                best = Some((rank, g));
            }
        }
        if let Some((_, g)) = best {
            let moved = std::mem::take(&mut members[joiner]);
            for &m in &moved {
                group_of[m] = g;
            }
            members[g].extend(moved);
        }
    }
}

fn fused(rule: &FusionRule, group: &[usize], seen: &[Seen<'_>]) -> Option<Event> {
    // Members are in preference order; the first with a value wins.
    let lead = &seen[group[0]];
    let mut payload = Map::new();
    for &m in group {
        for (field, value) in &seen[m].sighting.fields {
            if !value.is_null() && payload.get(field).is_none_or(Value::is_null) {
                payload.insert(field.clone(), value.clone());
            }
        }
    }
    payload.insert("record".into(), json!(record_identity(lead.event)));
    payload.insert(rule.start_field.into(), json!(lead.sighting.start));
    if let Some(end_field) = rule.end_field {
        payload.insert(end_field.into(), json!(lead.sighting.end));
    }
    let mut observed_by: Vec<&str> = Vec::new();
    for &m in group {
        let source = seen[m].event.source.as_str();
        if !observed_by.contains(&source) {
            observed_by.push(source);
        }
    }
    payload.insert("observed_by".into(), json!(observed_by));
    derived_json(
        rule.output,
        &Value::Object(payload),
        rule.derivation,
        group.iter().map(|&m| seen[m].event),
    )
}

fn hour_of(key: &Value) -> Option<i128> {
    key.get("hour")?.as_i64().map(i128::from)
}

fn hour_index(ns: i128) -> i64 {
    i64::try_from(ns.div_euclid(HOUR_NS)).unwrap_or(i64::MAX)
}

/// The derivation for one fusion rule.
pub struct Fusion(pub &'static FusionRule);

impl AnyKeyed for Fusion {
    fn name(&self) -> &'static str {
        self.0.derivation
    }

    fn version(&self) -> u32 {
        self.0.version
    }

    fn inputs_types(&self) -> &'static [&'static str] {
        self.0.event_types
    }

    fn output_types(&self) -> &'static [&'static str] {
        std::slice::from_ref(&self.0.output)
    }

    fn keys_for(&self, input: &KeyView<'_>, _env: &Env) -> Vec<Value> {
        let rule = self.0;
        let (Some(_), Some(t)) = (rule.reader(input.event_type, input.payload), input.ts_orig)
        else {
            // No time, or nothing this rule reads: nothing to fuse with.
            return vec![];
        };
        let ns = t.unix_ns();
        (hour_index(ns - rule.margin())..=hour_index(ns + rule.margin()))
            .map(|h| json!({ "hour": h }))
            .collect()
    }

    fn inputs(&self, key: &Value, _env: &Env) -> Option<InputQuery> {
        let rule = self.0;
        let start = hour_of(key)? * HOUR_NS;
        let kinds = rule
            .readers
            .iter()
            .map(|o| Kind::only(o.event_type, o.only))
            .collect();
        Some(InputQuery::of(
            kinds,
            Timestamp::from_unix_ns(start - rule.margin()).ok()?,
            Timestamp::from_unix_ns(start + HOUR_NS + rule.margin()).ok()?,
        ))
    }

    fn compute(&self, key: &Value, inputs: &[Event], _env: &Env) -> Computed {
        let rule = self.0;
        let Some(start) = hour_of(key).map(|h| h * HOUR_NS) else {
            return Computed::default();
        };
        let mut seen: Vec<Seen<'_>> = inputs.iter().filter_map(|e| read(rule, e)).collect();
        seen.sort_by_key(|s| (s.at, s.rank));
        let outputs = group(&seen, rule)
            .into_iter()
            .filter(|g| {
                let first = g.iter().map(|&m| seen[m].at).min().unwrap_or(i128::MIN);
                (start..start + HOUR_NS).contains(&first)
            })
            .filter_map(|g| fused(rule, &g, &seen))
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        let Some(hour) = hour_of(key) else {
            return KeyState::Closed;
        };
        let settled = (hour + 1) * HOUR_NS + self.0.margin() + SETTLE_NS;
        if env.now.unix_ns() >= settled {
            KeyState::Closed
        } else {
            KeyState::Open
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
