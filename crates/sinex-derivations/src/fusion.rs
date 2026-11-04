//! Fusion: one event for each occurrence that several sources recorded, for
//! the families whose definition is written (docs/derivations.md#fusion).
//! Every other family stays separate source records. The families are data
//! ([`declaration`]); the matching below is the same for all of them.
//!
//! A member reads one source's events as observations: their scope (which
//! must be equal for any association), their match fields (which must be
//! equal for a time match), their exact identifiers, their start, and the
//! output fields they supply. Members are listed in preference order, and a
//! fused group holds at most one observation per member. Observations match:
//!
//! 1. by the operator's `same` decisions (and agents', where the family lets
//!    them govern);
//! 2. by a shared exact identifier in one namespace, whatever their time
//!    distance, closest pairs first;
//! 3. by time: the closest pairs within `window`, closest first;
//! 4. among observations still alone, pairs within `fallback` where each
//!    side has exactly one possible partner;
//! 5. with `absorb` set, a group whose first start falls inside a group led
//!    by a more preferred member joins it, as does a lone observation inside
//!    one led by the same member.
//!
//! A pair decided `distinct` never shares a group. Matches do not chain past
//! these rules: a time match never grows a group of three or more beyond
//! `window`. Only groups of two or more become fused events; an unmatched
//! observation stays a source record only. Every choice among several
//! candidates records `ambiguous` on the fused event and a `fusion.considered_pair`
//! row for each candidate pair; so does every decided pair.
//!
//! A consumer of a family reads its fused records plus the member records no
//! fused record joins, through [`Family::occurrences`], so each occurrence
//! counts once.
//!
//! A family whose identity alone decides (mail: Message-ID within its
//! account) is keyed by the identity's values: its key reads every copy of
//! that identity, whatever their times, and emits all its groups. Any other
//! family is keyed by the UTC hour a group's first observation starts in.
//! Each hour reads a margin on both sides wide enough for any group that
//! reaches into it, and emits only groups whose first observation starts
//! inside it (and duplicate rows whose earlier side does), so each is emitted
//! exactly once. Identifier matches are exact among the observations a key
//! reads.
use crate::derived_event::{derived_json, record_identity};
use crate::{AnyKeyed, Computed, Env, InputQuery, KeyState, KeyView, Kind, Neighbour};
use serde_json::{Map, Value, json};
use sinex_model::event_types::duplicate::{Decision, Duplicate};
use sinex_model::event_types::fused::{
    Basis, ConsideredPair, Joined, Member as FusedMember, PairState, StandingDecision, Strategy,
};
use sinex_model::{Event, EventId, EventPayload, Origin, RecordState, Timestamp};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

pub mod declaration;

pub use declaration::{AgentDecisions, Family, Member, OPERATOR, families, family_named};

/// How a rule decides that records of one identity are one occurrence.
#[derive(Clone, Copy, Debug)]
pub enum Matching {
    /// Records of one identity match by time, as the module documentation
    /// describes; a key is the hour a group starts in.
    Time {
        window_ns: i128,
        fallback_ns: i128,
        /// How long after a group's first start a later group of the same
        /// identity can join it; `None` when every fusion record is a thing
        /// of its own (two runs of `ls` are two commands).
        absorb_ns: Option<i128>,
    },
    /// The identity alone decides, whatever the records' times: the first
    /// record of each input with that identity joins one group. A key is the
    /// values of `fields`, which together are the identity.
    Identity {
        fields: &'static [&'static str],
        /// How far apart in time copies of one occurrence are expected to be.
        /// Consumers that read a time period read this far beyond it; copies
        /// further apart than this can count once in each of two periods.
        spread_ns: i128,
    },
}

const HOUR_NS: i128 = 3_600_000_000_000;
/// Sources fill a record in after it starts (Atuin's exit code); a key is
/// settled this long after its margin has passed.
const SETTLE_NS: i128 = 900_000_000_000;

impl Family {
    /// How far outside its hour a key reads, and how far from an observation
    /// the hours it affects reach. A consumer reads this far beyond its own
    /// window, so the set it reads holds the fused record of every member it
    /// holds.
    pub fn margin(&self) -> i128 {
        match self.spread_ns {
            Some(spread) => spread,
            None => {
                self.window_ns.unwrap_or(0)
                    + self.fallback_ns.unwrap_or(0)
                    + self.absorb_ns.unwrap_or(0)
            }
        }
    }

    /// How far outside its hour an hour key reads: beyond the margin, the
    /// other candidates a fallback partner may have, so whether a fallback
    /// match is unique never depends on where the hour boundary falls.
    pub fn reach(&self) -> i128 {
        self.margin() + self.fallback_ns.unwrap_or(0)
    }

    /// For a family keyed by identity, the key of an event of a member type
    /// (or of the fused kind): the key fields' values; none when one is
    /// missing.
    pub fn identity_key(&self, event_type: &str, payload: &Value) -> Option<Value> {
        let mut key = Map::new();
        for name in &self.keyed_by {
            let value = self.field_of(event_type, payload, name)?;
            value.as_str()?;
            key.insert((*name).to_owned(), value);
        }
        Some(Value::Object(key))
    }

    /// The member that reads this event, from its type, source and payload.
    pub fn member(&self, event_type: &str, source: &str, payload: &Value) -> Option<usize> {
        self.members
            .iter()
            .position(|m| m.admits(event_type, source, payload))
    }

    fn reads_type(&self, event_type: &str) -> bool {
        self.members.iter().any(|m| m.event_type == event_type)
    }

    /// Whether some member could read this event, its source aside.
    fn reads_view(&self, event_type: &str, payload: &Value) -> bool {
        self.members.iter().any(|m| {
            m.event_type == event_type
                && m.only
                    .iter()
                    .all(|(field, text)| payload.get(*field).and_then(Value::as_str) == Some(*text))
        })
    }

    /// The kinds every member reads, once each.
    fn member_kinds(&self) -> Vec<Kind> {
        let mut kinds: Vec<Kind> = Vec::new();
        for member in &self.members {
            let kind = member.kind();
            if !kinds.contains(&kind) {
                kinds.push(kind);
            }
        }
        kinds
    }

    /// The members carrying a token namespace, as the kinds a neighbour read
    /// takes.
    fn token_kinds(&self, token: &str) -> Vec<Kind> {
        self.members
            .iter()
            .filter(|m| {
                m.identifiers
                    .iter()
                    .any(|(namespace, _)| *namespace == token)
            })
            .map(Member::kind)
            .collect()
    }

    /// The kinds a consumer of this family reads: the fused kind and every
    /// member's type.
    pub fn kinds(&self) -> Vec<Kind> {
        let mut kinds = vec![Kind::all(self.output)];
        kinds.extend(self.member_kinds());
        kinds
    }

    /// Whether an event of this type is a fused record of this family or of
    /// a member type. Members are told apart by source; a consumer filters
    /// those through [`Family::occurrences`].
    pub fn reads(&self, event_type: &str) -> bool {
        event_type == self.output || self.reads_type(event_type)
    }

    /// The source records in `inputs` that a live fused record of this
    /// family in `inputs` names as a parent.
    pub fn joined(&self, inputs: &[Event]) -> HashSet<EventId> {
        inputs
            .iter()
            .filter(|e| e.event_type == self.output && e.record_state != RecordState::Removed)
            .flat_map(|e| match &e.origin {
                Origin::Derived { parents } => parents.as_slice(),
                Origin::Record { .. } => &[],
            })
            .copied()
            .collect()
    }

    /// The family field `name` of an event of this type: a fused record's
    /// own field, or what the first member of that type reads.
    pub fn field_of(&self, event_type: &str, payload: &Value, name: &str) -> Option<Value> {
        if event_type == self.output {
            return payload.get(name).filter(|v| !v.is_null()).cloned();
        }
        self.members
            .iter()
            .find(|m| m.event_type == event_type)?
            .field(name, payload)
    }

    /// Each occurrence of this family in `inputs` once: every fused record,
    /// and every member record that no fused record in `inputs` joins, read
    /// into the family's fields (`record`, the start field and what the
    /// member supplies). In input order.
    ///
    /// A member that joins only by identifier (kitty's command end) never
    /// stands for an occurrence of its own. It is timed at the command's end
    /// while its fused record is timed at the start, so a window can hold the
    /// member without the fused record; and when the shell's report is lost,
    /// the Atuin row stands for the command.
    pub fn occurrences<'a>(&self, inputs: &'a [Event]) -> Vec<Occurrence<'a>> {
        let joined = self.joined(inputs);
        inputs
            .iter()
            .filter_map(|event| {
                if event.event_type == self.output {
                    return (event.record_state != RecordState::Removed).then_some(Occurrence {
                        event,
                        payload: Cow::Borrowed(&event.payload),
                    });
                }
                if joined.contains(&event.id) {
                    return None;
                }
                let seen = observe(self, event)?;
                if seen.identifier_only {
                    return None;
                }
                let mut payload = seen.fields.clone();
                payload.insert("record".into(), json!(seen.citation));
                payload.insert(self.start_field.into(), json!(seen.start));
                Some(Occurrence {
                    event,
                    payload: Cow::Owned(Value::Object(payload)),
                })
            })
            .collect()
    }
}

/// One occurrence of a fused family, as a consumer counts it.
#[derive(Clone, Debug)]
pub struct Occurrence<'a> {
    /// The fused record, or the member record no fused record joins.
    pub event: &'a Event,
    /// The family's fields: the fused record's payload, or the member's
    /// fields read into the same names.
    pub payload: Cow<'a, Value>,
}

/// A command occurrence as consumers read it: the fields every command
/// member supplies, from a fused command or an unmatched member record.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct Command {
    pub record: String,
    pub command: String,
    pub host: String,
    pub started_at: Timestamp,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub duration_ns: Option<i64>,
    #[serde(default)]
    pub exit_code: Option<i64>,
}

/// A mail occurrence as consumers read it.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct Mail {
    pub record: String,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub from: Vec<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub sent_at: Timestamp,
}

/// The command family.
pub fn commands() -> &'static Family {
    family_named("command")
}

/// The mail family.
pub fn mail() -> &'static Family {
    family_named("mail")
}

/// One observation during matching.
struct Seen<'a> {
    event: &'a Event,
    rank: usize,
    /// Joins only by identifier, never by time.
    identifier_only: bool,
    spans: bool,
    citation: String,
    start: Timestamp,
    at: i128,
    /// `None` when a scope field is missing: such an observation never joins
    /// another.
    scope: Option<Vec<Value>>,
    /// `None` when a match field is missing: no time match.
    matching: Option<Vec<Value>>,
    identifiers: Vec<(&'static str, String)>,
    fields: Map<String, Value>,
}

impl Seen<'_> {
    fn end(&self, absorb_ns: i128) -> i128 {
        if self.spans {
            self.at + absorb_ns
        } else {
            self.at
        }
    }
}

fn observe<'a>(family: &Family, event: &'a Event) -> Option<Seen<'a>> {
    if event.record_state == RecordState::Removed {
        return None;
    }
    let rank = family.member(&event.event_type, &event.source, &event.payload)?;
    let member = &family.members[rank];
    let start = event.ts_orig.at?;
    let mut fields = Map::new();
    for (name, field) in &member.fields {
        if let Some(value) = field.read(&event.payload) {
            fields.insert((*name).to_owned(), value);
        }
    }
    let values = |names: &[&str]| -> Option<Vec<Value>> {
        names.iter().map(|n| fields.get(*n).cloned()).collect()
    };
    let scope = values(&family.scope);
    let matching = values(&family.match_fields);
    let identifiers = member
        .identifiers
        .iter()
        .filter_map(|(namespace, field)| Some((*namespace, field.read_text(&event.payload)?)))
        .collect::<Vec<_>>();
    // A member that joins only by identifier is not read without one.
    if member.identifier_only && identifiers.is_empty() {
        return None;
    }
    Some(Seen {
        event,
        rank,
        identifier_only: member.identifier_only,
        spans: member.spans,
        citation: record_identity(event),
        start,
        at: start.unix_ns(),
        scope,
        matching,
        identifiers,
        fields,
    })
}

/// The decisions on pairs, read from the family's current duplicate
/// decisions.
#[derive(Default)]
struct Decisions {
    /// Per pair (lesser citation first): the governing decision and its
    /// author, and the other authors' standing decisions.
    pairs: BTreeMap<(String, String), PairDecisions>,
}

#[derive(Default)]
struct PairDecisions {
    governing: Option<(Decision, String)>,
    others: Vec<StandingDecision>,
}

fn pair_of(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

impl Decisions {
    /// The current decisions by the authored-record rules: a revision
    /// supersedes what it revises, a withdrawal takes back what it names,
    /// and nothing is chosen by write order or time. An author whose current
    /// decisions on one pair disagree (a conflict) has no standing decision.
    fn read(family: &Family, inputs: &[Event]) -> Self {
        let mut history = authored::History::new();
        let mut records: BTreeMap<authored::Revision, Duplicate> = BTreeMap::new();
        for event in inputs {
            if event.event_type != Duplicate::EVENT_TYPE
                || event.record_state == RecordState::Removed
            {
                continue;
            }
            let Ok(record) = serde_json::from_value::<Duplicate>(event.payload.clone()) else {
                continue;
            };
            if record.family.as_deref() != Some(family.name) || record.pair.len() != 2 {
                continue;
            }
            let Ok(header) = record.header() else {
                continue;
            };
            records.insert(header.revision(), record.clone());
            history.push(header, record.slots());
        }
        let Ok(current) = history.current() else {
            return Self::default();
        };
        let mut by_author: BTreeMap<((String, String), String), BTreeSet<Decision>> =
            BTreeMap::new();
        for revision in current {
            let Some(record) = records.get(&revision) else {
                continue;
            };
            let Some(decision) = record.decision else {
                continue;
            };
            by_author
                .entry((
                    pair_of(&record.pair[0], &record.pair[1]),
                    record.author.clone(),
                ))
                .or_default()
                .insert(decision);
        }
        let mut pairs: BTreeMap<(String, String), PairDecisions> = BTreeMap::new();
        let mut standing: BTreeMap<(String, String), Vec<(String, Decision)>> = BTreeMap::new();
        for ((pair, author), decisions) in by_author {
            if decisions.len() == 1 {
                let decision = *decisions.iter().next().expect("one decision");
                standing.entry(pair).or_default().push((author, decision));
            } else {
                pairs.entry(pair).or_default();
            }
        }
        for (pair, decisions) in standing {
            let governing = decisions
                .iter()
                .find(|(author, _)| author == OPERATOR)
                .or_else(|| {
                    // Agents govern only where the family admits them, and
                    // only when they agree.
                    let agree = decisions.iter().all(|(_, d)| *d == decisions[0].1);
                    (family.agent_decisions == AgentDecisions::Govern && agree)
                        .then(|| decisions.first())
                        .flatten()
                })
                .map(|(author, decision)| (*decision, author.clone()));
            let others = decisions
                .iter()
                .filter(|(author, _)| governing.as_ref().is_none_or(|(_, by)| by != author))
                .map(|(author, decision)| StandingDecision {
                    author: author.clone(),
                    decision: *decision,
                })
                .collect();
            pairs.insert(pair, PairDecisions { governing, others });
        }
        Self { pairs }
    }

    fn governing(&self, a: &str, b: &str) -> Option<&(Decision, String)> {
        self.pairs.get(&pair_of(a, b))?.governing.as_ref()
    }

    fn distinct(&self, a: &str, b: &str) -> bool {
        matches!(self.governing(a, b), Some((Decision::Distinct, _)))
    }
}

/// How two observations were linked when their groups merged.
struct Edge {
    a: usize,
    b: usize,
    joined: Joined,
    candidates: u32,
    competitors: Vec<usize>,
}

/// A pair considered among several candidates, or a decided pair.
struct Considered {
    a: usize,
    b: usize,
    strategy: Strategy,
    candidates: u32,
}

/// A possible link, before choosing.
#[derive(Clone)]
struct Candidate {
    distance: i128,
    i: usize,
    j: usize,
}

struct Grouping<'s, 'e> {
    seen: &'s [Seen<'e>],
    decisions: &'s Decisions,
    group_of: Vec<usize>,
    members: Vec<Vec<usize>>,
    edges: Vec<Edge>,
    considered: Vec<Considered>,
}

impl Grouping<'_, '_> {
    fn same_scope(&self, i: usize, j: usize) -> bool {
        self.seen[i].scope.is_some() && self.seen[i].scope == self.seen[j].scope
    }

    fn alone(&self, i: usize) -> bool {
        self.members[self.group_of[i]].len() == 1
    }

    /// Merge the groups of `i` and `j`, unless that puts two observations of
    /// one member in a group, joins a pair decided distinct, or (for a time
    /// match, with `extent` set) stretches a group of three or more past it.
    fn merge(&mut self, i: usize, j: usize, extent: Option<i128>) -> bool {
        let (gi, gj) = (self.group_of[i], self.group_of[j]);
        if gi == gj || !self.same_scope(i, j) {
            return false;
        }
        let seen = self.seen;
        let ranks: HashSet<usize> = self.members[gi].iter().map(|&m| seen[m].rank).collect();
        if self.members[gj]
            .iter()
            .any(|&m| ranks.contains(&seen[m].rank))
        {
            return false;
        }
        for &a in &self.members[gi] {
            for &b in &self.members[gj] {
                if self
                    .decisions
                    .distinct(&seen[a].citation, &seen[b].citation)
                {
                    return false;
                }
            }
        }
        if let Some(extent) = extent {
            let all = self.members[gi].iter().chain(&self.members[gj]);
            let lo = all.clone().map(|&m| seen[m].at).min().unwrap_or(0);
            let hi = all.clone().map(|&m| seen[m].at).max().unwrap_or(0);
            if hi - lo > extent && self.members[gi].len() + self.members[gj].len() > 2 {
                return false;
            }
        }
        let moved = std::mem::take(&mut self.members[gj]);
        for &m in &moved {
            self.group_of[m] = gi;
        }
        self.members[gi].extend(moved);
        true
    }

    /// Link the closest candidates first. A side's candidates are the other
    /// side's member observations it could pair with; more than one is a
    /// choice, recorded on the link and as considered pairs.
    fn closest_first(
        &mut self,
        mut candidates: Vec<Candidate>,
        strategy: Strategy,
        joined: impl Fn(&Candidate) -> Joined,
        extent: Option<i128>,
    ) {
        let seen = self.seen;
        let order = |c: &Candidate| {
            let (a, b) = (&seen[c.i], &seen[c.j]);
            (
                c.distance,
                a.rank.min(b.rank),
                a.citation.clone().min(b.citation.clone()),
                a.citation.clone().max(b.citation.clone()),
            )
        };
        candidates.sort_by_cached_key(order);
        // For each observation, its candidates per member of the other side.
        let mut options: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
        for c in &candidates {
            options.entry((c.i, seen[c.j].rank)).or_default().push(c.j);
            options.entry((c.j, seen[c.i].rank)).or_default().push(c.i);
        }
        let choices =
            |x: usize, rank: usize| options.get(&(x, rank)).map_or(&[][..], Vec::as_slice);
        for c in &candidates {
            let from_i = choices(c.i, seen[c.j].rank);
            let from_j = choices(c.j, seen[c.i].rank);
            let n = from_i.len().max(from_j.len());
            if !self.merge(c.i, c.j, extent) {
                continue;
            }
            let competitors: Vec<usize> = from_i
                .iter()
                .filter(|&&x| x != c.j)
                .chain(from_j.iter().filter(|&&x| x != c.i))
                .copied()
                .collect();
            if n > 1 {
                for &x in from_i {
                    self.consider(c.i, x, strategy, n);
                }
                for &x in from_j {
                    self.consider(c.j, x, strategy, n);
                }
            }
            self.edges.push(Edge {
                a: c.i,
                b: c.j,
                joined: joined(c),
                candidates: u32::try_from(n).unwrap_or(u32::MAX),
                competitors,
            });
        }
    }

    fn consider(&mut self, a: usize, b: usize, strategy: Strategy, n: usize) {
        if self
            .considered
            .iter()
            .any(|c| (c.a, c.b) == (a, b) || (c.a, c.b) == (b, a))
        {
            return;
        }
        self.considered.push(Considered {
            a,
            b,
            strategy,
            candidates: u32::try_from(n).unwrap_or(u32::MAX),
        });
    }
}

/// Group observations, sorted by time, by the family's rules.
fn group<'s, 'e>(
    seen: &'s [Seen<'e>],
    family: &Family,
    decisions: &'s Decisions,
) -> Grouping<'s, 'e> {
    let n = seen.len();
    let mut g = Grouping {
        seen,
        decisions,
        group_of: (0..n).collect(),
        members: (0..n).map(|i| vec![i]).collect(),
        edges: Vec::new(),
        considered: Vec::new(),
    };
    let index: HashMap<&str, usize> = seen
        .iter()
        .enumerate()
        .map(|(i, s)| (s.citation.as_str(), i))
        .collect();

    let decided: Vec<(usize, usize)> = decisions
        .pairs
        .keys()
        .filter_map(|(a, b)| Some((*index.get(a.as_str())?, *index.get(b.as_str())?)))
        .collect();
    // Governing `same` decisions join first.
    for (pair, decided) in &decisions.pairs {
        let (Some(&a), Some(&b)) = (index.get(pair.0.as_str()), index.get(pair.1.as_str())) else {
            continue;
        };
        if let Some((Decision::Same, author)) = &decided.governing
            && g.merge(a, b, None)
        {
            g.edges.push(Edge {
                a,
                b,
                joined: Joined::Decision {
                    author: author.clone(),
                },
                candidates: 1,
                competitors: Vec::new(),
            });
        }
    }

    // Exact identifiers, whatever the time distance.
    let mut by_identifier: BTreeMap<(&'static str, &str), Vec<usize>> = BTreeMap::new();
    for (i, s) in seen.iter().enumerate() {
        for (namespace, value) in &s.identifiers {
            by_identifier
                .entry((*namespace, value.as_str()))
                .or_default()
                .push(i);
        }
    }
    let mut namespace_of: HashMap<(usize, usize), &'static str> = HashMap::new();
    let mut identified = Vec::new();
    for ((namespace, _), holders) in &by_identifier {
        for (x, &i) in holders.iter().enumerate() {
            for &j in &holders[x + 1..] {
                if seen[i].rank == seen[j].rank || !g.same_scope(i, j) {
                    continue;
                }
                let (i, j) = if seen[i].at <= seen[j].at {
                    (i, j)
                } else {
                    (j, i)
                };
                if namespace_of.contains_key(&(i, j)) {
                    continue;
                }
                namespace_of.insert((i, j), *namespace);
                identified.push(Candidate {
                    distance: seen[j].at - seen[i].at,
                    i,
                    j,
                });
            }
        }
    }
    g.closest_first(
        identified,
        Strategy::Identifier,
        |c| Joined::Identifier {
            namespace: namespace_of[&(c.i, c.j)].to_owned(),
        },
        None,
    );

    let Some(window) = family.window_ns else {
        show_judged(&mut g, &decided);
        return g;
    };
    let fallback = family.fallback_ns.unwrap_or(window);
    // Time candidates. Observations are sorted by time, so the inner loop
    // stops once the gap exceeds the fallback window.
    let time_compatible = |i: usize, j: usize| {
        !seen[i].identifier_only
            && !seen[j].identifier_only
            && seen[i].rank != seen[j].rank
            && seen[i].matching.is_some()
            && seen[i].matching == seen[j].matching
            && seen[i].scope.is_some()
            && seen[i].scope == seen[j].scope
    };
    let mut near = Vec::new();
    let mut far = Vec::new();
    for i in 0..n {
        for j in i + 1..n {
            let distance = seen[j].at - seen[i].at;
            if distance > fallback {
                break;
            }
            if !time_compatible(i, j) {
                continue;
            }
            let c = Candidate { distance, i, j };
            if distance <= window {
                near.push(c);
            } else {
                far.push(c);
            }
        }
    }
    let distance_ns = |c: &Candidate| i64::try_from(c.distance).unwrap_or(i64::MAX);
    g.closest_first(
        near,
        Strategy::Window,
        |c| Joined::Window {
            distance_ns: distance_ns(c),
        },
        Some(window),
    );

    // Fallback: only between observations still alone, and only when each
    // side has exactly one such partner. More than one leaves them apart and
    // shows the pairs for decision.
    let far: Vec<Candidate> = far
        .into_iter()
        .filter(|c| g.alone(c.i) && g.alone(c.j))
        .collect();
    let mut partners = vec![0usize; n];
    for c in &far {
        partners[c.i] += 1;
        partners[c.j] += 1;
    }
    for c in &far {
        let n = partners[c.i].max(partners[c.j]);
        if n == 1 {
            if g.merge(c.i, c.j, Some(window)) {
                g.edges.push(Edge {
                    a: c.i,
                    b: c.j,
                    joined: Joined::Fallback {
                        distance_ns: distance_ns(c),
                    },
                    candidates: 1,
                    competitors: Vec::new(),
                });
            }
        } else {
            g.consider(c.i, c.j, Strategy::Fallback, n);
        }
    }

    if let Some(absorb_ns) = family.absorb_ns {
        absorb(&mut g, window, absorb_ns);
    }
    show_judged(&mut g, &decided);
    g
}

/// Every decided pair the key reads is shown, whether or not a strategy
/// proposed it.
fn show_judged(g: &mut Grouping<'_, '_>, decided: &[(usize, usize)]) {
    for &(a, b) in decided {
        g.consider(a, b, Strategy::Decision, 1);
    }
}

/// Join groups, in order of their first start, to the group of their
/// identity whose extent holds that start and whose lead is more preferred
/// (or equally preferred, for a lone observation): the most preferred such
/// group, then the one starting nearest to it.
fn absorb(g: &mut Grouping<'_, '_>, window_ns: i128, absorb_ns: i128) {
    let seen = g.seen;
    let first_of = |group: &[usize]| group.iter().map(|&m| seen[m].at).min().unwrap_or(0);
    let lead_of = |group: &[usize]| {
        group
            .iter()
            .copied()
            .min_by_key(|&m| (seen[m].rank, seen[m].at, seen[m].citation.clone()))
            .expect("groups are not empty")
    };
    let identity = |m: usize| {
        seen[m]
            .scope
            .as_ref()
            .zip(seen[m].matching.as_ref())
            .map(|(scope, matching)| (scope.clone(), matching.clone()))
    };
    // Groups by first start. A group's first start only moves earlier, by
    // less than `window`, as it absorbs; the search widens by that much.
    let mut by_first: Vec<(i128, usize)> = g
        .members
        .iter()
        .enumerate()
        .filter(|(_, group)| !group.is_empty())
        .map(|(index, group)| (first_of(group), index))
        .collect();
    by_first.sort_unstable();
    for &(_, joiner) in &by_first.clone() {
        if g.members[joiner].is_empty() {
            continue;
        }
        let at = first_of(&g.members[joiner]);
        let lead = lead_of(&g.members[joiner]);
        let alone = g.members[joiner].len() == 1;
        let Some(lead_identity) = identity(lead) else {
            continue;
        };
        let from = by_first.partition_point(|&(first, _)| first < at - absorb_ns - window_ns);
        let mut best: Option<((usize, i128), usize)> = None;
        for &(_, index) in by_first[from..]
            .iter()
            .take_while(|&&(first, _)| first <= at + 2 * window_ns)
        {
            let group = &g.members[index];
            if index == joiner || group.is_empty() {
                continue;
            }
            let target = lead_of(group);
            let preferred = seen[target].rank < seen[lead].rank
                || (alone && seen[target].rank == seen[lead].rank);
            if !preferred || identity(target).as_ref() != Some(&lead_identity) {
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
                best = Some((rank, index));
            }
        }
        if let Some((_, index)) = best {
            let distinct = g.members[joiner].iter().any(|&a| {
                g.members[index]
                    .iter()
                    .any(|&b| g.decisions.distinct(&seen[a].citation, &seen[b].citation))
            });
            if distinct {
                continue;
            }
            let target = lead_of(&g.members[index]);
            let distance = at - first_of(&g.members[index]);
            let moved = std::mem::take(&mut g.members[joiner]);
            for &m in &moved {
                g.group_of[m] = index;
            }
            g.members[index].extend(moved);
            g.edges.push(Edge {
                a: target,
                b: lead,
                joined: Joined::Absorb {
                    distance_ns: i64::try_from(distance).unwrap_or(i64::MAX),
                },
                candidates: 1,
                competitors: Vec::new(),
            });
        }
    }
}

/// The fused kind's payload for one group, members in preference order.
fn fused_payload(family: &Family, group: &[usize], g: &Grouping<'_, '_>) -> Map<String, Value> {
    let seen = g.seen;
    let lead = group[0];
    let mut payload = Map::new();
    for &m in group {
        for (field, value) in &seen[m].fields {
            // An empty list is no value: a later member's labels still count.
            let missing = |v: &Value| v.is_null() || v.as_array().is_some_and(Vec::is_empty);
            if payload.get(field).is_none_or(missing) {
                payload.insert(field.clone(), value.clone());
            }
        }
    }
    let in_group: HashSet<usize> = group.iter().copied().collect();
    let edges: Vec<&Edge> = g
        .edges
        .iter()
        .filter(|e| in_group.contains(&e.a) && in_group.contains(&e.b))
        .collect();
    let members: Vec<FusedMember> = group
        .iter()
        .map(|&m| FusedMember {
            record: seen[m].citation.clone(),
            source: seen[m].event.source.clone(),
            event_type: seen[m].event.event_type.clone(),
            joined: if m == lead {
                Joined::Lead
            } else {
                edges
                    .iter()
                    .find(|e| e.a == m || e.b == m)
                    .map_or(Joined::Lead, |e| e.joined.clone())
            },
        })
        .collect();
    let basis = if edges
        .iter()
        .any(|e| matches!(e.joined, Joined::Decision { .. }))
    {
        Basis::Operator
    } else if edges.iter().any(|e| {
        matches!(
            e.joined,
            Joined::Window { .. } | Joined::Fallback { .. } | Joined::Absorb { .. }
        )
    }) {
        Basis::Rule
    } else {
        Basis::Identifier
    };
    let ambiguous = edges.iter().map(|e| e.candidates).max().unwrap_or(1).max(1);
    let competitors: BTreeSet<&str> = edges
        .iter()
        .flat_map(|e| e.competitors.iter())
        .map(|&c| seen[c].citation.as_str())
        .collect();
    payload.insert("record".into(), json!(seen[lead].citation));
    payload.insert(family.start_field.into(), json!(seen[lead].start));
    payload.insert("members".into(), json!(members));
    payload.insert("basis".into(), json!(basis));
    payload.insert("ambiguous".into(), json!(ambiguous));
    payload.insert("competitors".into(), json!(competitors));
    payload.insert("rule_version".into(), json!(family.version));
    payload
}

fn considered_pair(family: &Family, c: &Considered, g: &Grouping<'_, '_>) -> ConsideredPair {
    let seen = g.seen;
    let (a, b) = if seen[c.a].citation <= seen[c.b].citation {
        (c.a, c.b)
    } else {
        (c.b, c.a)
    };
    let decided = g
        .decisions
        .pairs
        .get(&pair_of(&seen[a].citation, &seen[b].citation));
    let governing = decided.and_then(|j| j.governing.as_ref());
    ConsideredPair {
        family: family.name.to_owned(),
        left: seen[a].citation.clone(),
        right: seen[b].citation.clone(),
        left_source: seen[a].event.source.clone(),
        right_source: seen[b].event.source.clone(),
        at: seen[a].start.min(seen[b].start),
        strategy: c.strategy,
        candidates: c.candidates,
        distance_ns: i64::try_from((seen[a].at - seen[b].at).abs()).unwrap_or(i64::MAX),
        chosen: g.group_of[a] == g.group_of[b],
        state: match governing {
            None => PairState::Undecided,
            Some((Decision::Same, _)) => PairState::Same,
            Some((Decision::Distinct, _)) => PairState::Distinct,
        },
        decided_by: governing.map(|(_, by)| by.clone()),
        other_decisions: decided.map(|j| j.others.clone()).unwrap_or_default(),
    }
}

fn hour_of(key: &Value) -> Option<i128> {
    key.get("hour")?.as_i64().map(i128::from)
}

fn hour_index(ns: i128) -> i64 {
    i64::try_from(ns.div_euclid(HOUR_NS)).unwrap_or(i64::MAX)
}

/// The keyed derivation for one family.
pub struct Fusion(pub &'static Family);

impl Fusion {
    fn decision_kind(&self) -> Kind {
        Kind::only(Duplicate::EVENT_TYPE, &[("family", self.0.name)])
    }
}

impl AnyKeyed for Fusion {
    fn name(&self) -> &'static str {
        self.0.derivation
    }

    fn version(&self) -> u32 {
        self.0.version
    }

    fn inputs_types(&self) -> &'static [&'static str] {
        self.0.input_types
    }

    fn output_types(&self) -> &'static [&'static str] {
        self.0.output_types
    }

    fn keys_for(&self, input: &KeyView<'_>, _env: &Env) -> Vec<Value> {
        let family = self.0;
        let decision = self.decision_kind().admits(input.event_type, input.payload);
        if !family.keyed_by.is_empty() {
            if decision {
                return input
                    .payload
                    .get("family_key")
                    .filter(|k| k.is_object())
                    .cloned()
                    .into_iter()
                    .collect();
            }
            if !family.reads_view(input.event_type, input.payload) {
                return vec![];
            }
            return family
                .identity_key(input.event_type, input.payload)
                .into_iter()
                .collect();
        }
        let read = family.reads_view(input.event_type, input.payload) || decision;
        let (true, Some(t)) = (read, input.ts_orig) else {
            // No time, or nothing this family reads: nothing to fuse with.
            return vec![];
        };
        let ns = t.unix_ns();
        (hour_index(ns - family.reach())..=hour_index(ns + family.reach()))
            .map(|h| json!({ "hour": h }))
            .collect()
    }

    fn inputs(&self, key: &Value, _env: &Env) -> Option<InputQuery> {
        let family = self.0;
        if !family.keyed_by.is_empty() {
            // Every copy of this identity, whatever its time, and the
            // decisions made within it.
            let mut kinds: Vec<Kind> = Vec::new();
            for member in &family.members {
                let mut equal = Vec::new();
                for name in &family.keyed_by {
                    let field = member.fields.iter().find(|(f, _)| f == name)?.1.clone();
                    equal.push((field.pointer[1..].to_owned(), key.get(*name)?.clone()));
                }
                let kind = Kind {
                    event_type: member.event_type.to_owned(),
                    equal,
                };
                if !kinds.contains(&kind) {
                    kinds.push(kind);
                }
            }
            kinds.push(Kind {
                event_type: Duplicate::EVENT_TYPE.to_owned(),
                equal: vec![
                    ("family".to_owned(), json!(family.name)),
                    ("family_key".to_owned(), key.clone()),
                ],
            });
            return Some(InputQuery::all(kinds));
        }
        let start = hour_of(key)? * HOUR_NS;
        let mut kinds = family.member_kinds();
        kinds.push(self.decision_kind());
        let mut query = InputQuery::of(
            kinds,
            Timestamp::from_unix_ns(start - family.reach()).ok()?,
            Timestamp::from_unix_ns(start + HOUR_NS + family.reach()).ok()?,
        );
        // A token's partners, however far from this hour: each member
        // carries a token once, so the nearest record holding it on either
        // side is the partner. Every partner lies within the token's
        // lifetime, from its mint time to the command's end, so each read
        // stops there and a token without a partner costs one bounded range.
        let lifetime = sinex_model::TokenLifetime {
            length: family.token_length,
            slack_ns: family.window_ns.unwrap_or(0),
        };
        for token in &family.tokens {
            for list in [&mut query.before, &mut query.after] {
                list.push(Neighbour {
                    kinds: family.token_kinds(token),
                    per: Some(*token),
                    count: 1,
                    lifetime: Some(lifetime),
                });
            }
        }
        Some(query)
    }

    fn nearby_inputs(&self, input: &KeyView<'_>, _env: &Env) -> Option<InputQuery> {
        // A record holding a token dirties the hours of the record that
        // started the occurrence: the token's mint time is that start.
        let family = self.0;
        let mut kinds = Vec::new();
        let mut minted = None;
        for token in &family.tokens {
            let Some(value) = input.payload.get(*token).and_then(Value::as_str) else {
                continue;
            };
            let Some(ms) = sinex_model::uuid7_millisecond(value).map(i128::from) else {
                continue;
            };
            minted = Some(ms);
            for kind in family.token_kinds(token) {
                let mut kind = kind;
                kind.equal.push(((*token).to_owned(), json!(value)));
                kinds.push(kind);
            }
        }
        let ms = minted?;
        Some(InputQuery::of(
            kinds,
            Timestamp::from_unix_ns(ms * 1_000_000).ok()?,
            Timestamp::from_unix_ns((ms + 1) * 1_000_000).ok()?,
        ))
    }

    fn compute(&self, key: &Value, inputs: &[Event], _env: &Env) -> Computed {
        let family = self.0;
        // An hour key emits what starts in its hour; an identity key emits
        // all of its identity's groups.
        let hour = if family.keyed_by.is_empty() {
            let Some(start) = hour_of(key).map(|h| h * HOUR_NS) else {
                return Computed::default();
            };
            start..start + HOUR_NS
        } else {
            i128::MIN..i128::MAX
        };
        let mut seen: Vec<Seen<'_>> = inputs.iter().filter_map(|e| observe(family, e)).collect();
        // Equal times order by member, then record: never by reading ID or
        // commit order.
        seen.sort_by(|a, b| (a.at, a.rank, &a.citation).cmp(&(b.at, b.rank, &b.citation)));
        let decisions = Decisions::read(family, inputs);
        let g = group(&seen, family, &decisions);
        let built = |kind: &str, payload: Value, parents: Vec<&Event>| {
            let registered = sinex_model::event_types::find(kind).ok_or_else(|| {
                sinex_model::BuildError::Declaration(format!("validator_unavailable: {kind}"))
            })?;
            derived_json(
                &(registered.declaration)(),
                Some(registered.build),
                &payload,
                family.derivation,
                parents,
            )
        };
        let mut outputs = Vec::new();
        for group in &g.members {
            if group.len() < 2 {
                continue;
            }
            let mut group = group.clone();
            group.sort_by(|&a, &b| {
                (seen[a].rank, seen[a].at, &seen[a].citation).cmp(&(
                    seen[b].rank,
                    seen[b].at,
                    &seen[b].citation,
                ))
            });
            let first = group.iter().map(|&m| seen[m].at).min().unwrap_or(i128::MIN);
            if !hour.contains(&first) {
                continue;
            }
            let payload = Value::Object(fused_payload(family, &group, &g));
            outputs.push(built(
                family.output,
                payload,
                group.iter().map(|&m| seen[m].event).collect(),
            ));
        }
        for c in &g.considered {
            if !hour.contains(&seen[c.a].at.min(seen[c.b].at)) {
                continue;
            }
            let row = considered_pair(family, c, &g);
            let payload = serde_json::to_value(&row).expect("duplicate rows serialize");
            outputs.push(built(
                ConsideredPair::EVENT_TYPE,
                payload,
                vec![seen[c.a].event, seen[c.b].event],
            ));
        }
        match outputs.into_iter().collect::<Result<Vec<_>, _>>() {
            Ok(outputs) => Computed {
                error: None,
                outputs,
                waiting: false,
            },
            Err(error) => Computed {
                error: Some(error),
                ..Computed::default()
            },
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        if !self.0.keyed_by.is_empty() {
            // Another copy can arrive at any time; it dirties the key again.
            return KeyState::Closed;
        }
        let Some(hour) = hour_of(key) else {
            return KeyState::Closed;
        };
        let settled = (hour + 1) * HOUR_NS + self.0.reach() + SETTLE_NS;
        if env.now.unix_ns() >= settled {
            KeyState::Closed
        } else {
            KeyState::Open
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
