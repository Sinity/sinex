//! Turning a curation request into records. Each act becomes zero or more
//! drafts against one snapshot of the live records; the shared `authored`
//! planner then places every draft (a new record, a revision of the record
//! holding its uniqueness slot, or "already"), refusing the whole request when
//! any write is refused. The daemon stores the planned records as the
//! request's material and admits them only if [`verify`] still holds inside
//! the admitting transaction.
use crate::curation::{self, Snapshot, Statement, slot_digest};
use crate::reads;
use authored::{Arrival, OnStale, OrderKey, Plan, Revision, Target as Withdrawn, Write};
use serde::Serialize;
use serde_json::{Value, json};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{
    Act, Curation, CurationKind, PropertyType, TagRelation, Target, ValueOrigin,
};
use sinex_store::Connection as Conn;
use std::collections::BTreeMap;

pub use authored::Placed;

/// Who is writing, computed by the daemon from the authenticated caller.
#[derive(Clone, Debug)]
pub struct Caller {
    pub author: String,
    pub via: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Store(#[from] curation::Error),
    #[error("item lookup failed: {0}")]
    Items(#[from] reads::Error),
    /// The act cannot be a record: an unknown item, a value outside its
    /// scale, an empty label.
    #[error("act {act}: {reason}")]
    Invalid { act: usize, reason: String },
    /// The shared planner refused a write: a stale base, a slot another
    /// record holds, an unknown withdrawal target.
    #[error("refused: {0}")]
    Refused(String),
    #[error("request {0} was already written with other content")]
    RequestConflict(String),
}

fn invalid(act: usize, reason: impl Into<String>) -> PlanError {
    PlanError::Invalid {
        act: act + 1,
        reason: reason.into(),
    }
}

/// What one record's key holds while a request is being resolved: text, or
/// the root of an earlier draft of the same request (a collection the
/// request creates).
#[derive(Clone, Debug, PartialEq)]
pub enum Key {
    None,
    Text(String),
    Draft(usize),
}

/// One record a request asks for, before placement.
#[derive(Clone, Debug, PartialEq)]
pub struct Draft {
    pub kind: CurationKind,
    pub target: Option<Target>,
    pub key: Key,
    pub value: Option<Value>,
    pub origin: ValueOrigin,
    pub cites: Option<String>,
    pub revises: Vec<Revision>,
    pub withdraws: Vec<Withdrawn>,
}

impl Draft {
    fn authored(kind: CurationKind) -> Self {
        Self {
            kind,
            target: None,
            key: Key::None,
            value: None,
            origin: ValueOrigin::Authored,
            cites: None,
            revises: Vec::new(),
            withdraws: Vec::new(),
        }
    }

    fn withdrawal(withdraws: Vec<Withdrawn>) -> Self {
        Self {
            withdraws,
            ..Self::authored(CurationKind::Withdrawal)
        }
    }
}

/// A placed request, ready to store and admit.
#[derive(Clone, Debug)]
pub struct Planned {
    pub request: String,
    pub records: Vec<Curation>,
    /// Where each draft went, in order.
    pub placed: Vec<Placed>,
}

pub enum Outcome {
    Written(Planned),
    /// The request was already written with this content: its records.
    Repeat(Vec<Curation>),
}

/// A new request ID.
pub fn new_request() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// A caller's request ID: letters, digits, `-` and `_`, so that write IDs
/// (`<request>.<n>`) and revisions (`<root>@<rev>`) stay unambiguous.
pub fn check_request(id: &str) -> Result<(), String> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(format!(
            "request ID `{id}` must be letters, digits, `-` and `_`"
        ));
    }
    Ok(())
}

fn digest_hex<T: Serialize + ?Sized>(content: &T) -> String {
    authored::RequestDigest::of(content)
        .expect("requests serialize")
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Plan a request of acts from the CLI, MCP or web. Stale bases are refused.
pub async fn plan(
    conn: &mut Conn,
    request: Option<String>,
    acts: &[Act],
    caller: &Caller,
    at: Timestamp,
) -> Result<Outcome, PlanError> {
    let id = match request {
        Some(id) => {
            check_request(&id).map_err(PlanError::Refused)?;
            id
        }
        None => new_request(),
    };
    let digest = digest_hex(acts);
    let mut snapshot = Snapshot::new();
    snapshot
        .load_where(conn, &["request"], std::slice::from_ref(&id))
        .await?;
    let earlier: Vec<Curation> = snapshot
        .records()
        .values()
        .filter(|record| record.request == id)
        .cloned()
        .collect();
    if let Some(first) = earlier.first() {
        return if first.request_digest == digest {
            Ok(Outcome::Repeat(earlier))
        } else {
            Err(PlanError::RequestConflict(id))
        };
    }
    let mut resolver = Resolver {
        snapshot,
        caller,
        drafts: Vec::new(),
        collections: BTreeMap::new(),
        entries: BTreeMap::new(),
        scales: BTreeMap::new(),
    };
    for (index, act) in acts.iter().enumerate() {
        resolver.act(conn, index, act).await?;
    }
    let Resolver {
        mut snapshot,
        drafts,
        ..
    } = resolver;
    let slots: Vec<authored::UniqueSlot> = drafts
        .iter()
        .flat_map(|draft| {
            let key = match &draft.key {
                Key::Text(text) => Some(text.as_str()),
                _ => None,
            };
            Statement {
                kind: draft.kind,
                target: draft.target.as_ref(),
                key,
                value: draft.value.as_ref(),
                author: &caller.author,
                origin: draft.origin,
                cites: draft.cites.as_deref(),
            }
            .slots()
        })
        .collect();
    snapshot.load_slots(conn, &slots).await?;
    let arrival = Arrival {
        author: caller.author.clone(),
        via: caller.via.clone(),
        at: millis(at),
        on_stale: OnStale::Refuse,
    };
    let (records, placed) = place(&mut snapshot, &id, &digest, drafts, arrival, at)?;
    Ok(Outcome::Written(Planned {
        request: id,
        records,
        placed,
    }))
}

/// Plan imported or proposed drafts. A stale base forks instead of refusing,
/// so the order in which imports arrive cannot change the result.
pub async fn plan_drafts(
    conn: &mut Conn,
    drafts: Vec<Draft>,
    caller: &Caller,
    at: Timestamp,
) -> Result<Planned, PlanError> {
    let id = new_request();
    let mut snapshot = Snapshot::new();
    let slots: Vec<_> = drafts
        .iter()
        .flat_map(|draft| {
            let key = match &draft.key {
                Key::Text(text) => Some(text.as_str()),
                _ => None,
            };
            Statement {
                kind: draft.kind,
                target: draft.target.as_ref(),
                key,
                value: draft.value.as_ref(),
                author: &caller.author,
                origin: draft.origin,
                cites: draft.cites.as_deref(),
            }
            .slots()
        })
        .collect();
    snapshot.load_slots(conn, &slots).await?;
    let digest = digest_hex(
        &drafts
            .iter()
            .map(|d| json!([d.kind, d.target, d.value, d.cites]))
            .collect::<Vec<_>>(),
    );
    let arrival = Arrival {
        author: caller.author.clone(),
        via: caller.via.clone(),
        at: millis(at),
        on_stale: OnStale::Fork,
    };
    let (records, placed) = place(&mut snapshot, &id, &digest, drafts, arrival, at)?;
    Ok(Planned {
        request: id,
        records,
        placed,
    })
}

fn millis(at: Timestamp) -> i64 {
    i64::try_from(at.unix_ns() / 1_000_000).unwrap_or(i64::MAX)
}

/// The product's part of a planned record.
#[derive(Clone, Debug, PartialEq)]
struct Body {
    kind: CurationKind,
    target: Option<Target>,
    origin: ValueOrigin,
    cites: Option<String>,
}

fn place(
    snapshot: &mut Snapshot,
    request: &str,
    digest: &str,
    drafts: Vec<Draft>,
    arrival: Arrival,
    at: Timestamp,
) -> Result<(Vec<Curation>, Vec<Placed>), PlanError> {
    let count = drafts.len();
    let author = arrival.author.clone();
    let via = arrival.via.clone();
    let mut plan: Plan<'_, authored::History, Body> =
        Plan::new(request, count, arrival, &mut snapshot.history);
    let mut roots: Vec<String> = Vec::with_capacity(count);
    for draft in drafts {
        let key = match draft.key {
            Key::None => None,
            Key::Text(text) => Some(text),
            Key::Draft(index) => Some(roots[index].clone()),
        };
        let unique = Statement {
            kind: draft.kind,
            target: draft.target.as_ref(),
            key: key.as_deref(),
            value: draft.value.as_ref(),
            author: &author,
            origin: draft.origin,
            cites: draft.cites.as_deref(),
        }
        .unique();
        let placed = plan
            .add(Write {
                kind: draft.kind.as_str().to_owned(),
                key,
                value: draft.value,
                body: Body {
                    kind: draft.kind,
                    target: draft.target,
                    origin: draft.origin,
                    cites: draft.cites,
                },
                revises: draft.revises,
                withdraws: draft.withdraws,
                unique,
            })
            .map_err(|refused| PlanError::Refused(refused.to_string()))?;
        roots.push(match &placed {
            Placed::Written { revision, .. } => revision.root.clone(),
            Placed::Already { heads } => heads[0].root.clone(),
        });
    }
    let planned = plan
        .finish()
        .map_err(|refused| PlanError::Refused(refused.to_string()))?;
    let records = planned
        .records
        .into_iter()
        .map(|record| Curation {
            root: record.header.root,
            rev: record.header.rev,
            revises: record
                .header
                .revises
                .iter()
                .map(Revision::to_string)
                .collect(),
            withdraws: record
                .header
                .withdraws
                .iter()
                .map(Withdrawn::to_string)
                .collect(),
            kind: record.body.kind,
            target: record.body.target,
            key: record.header.key,
            value: record.header.value,
            origin: record.body.origin,
            cites: record.body.cites,
            author: author.clone(),
            via: via.clone(),
            at,
            request: request.to_owned(),
            request_digest: digest.to_owned(),
            slots: record.slots.iter().map(slot_digest).collect(),
        })
        .collect();
    Ok((records, planned.placed))
}

/// Check, inside the transaction that admits them, that planned authored
/// records still place as they were planned: every withdrawal target exists,
/// every base is current, and no other record holds a slot a record claims.
/// Each record is checked against the stored records plus the request's
/// earlier records. Imported and proposed records fork by design and are
/// not checked.
pub async fn verify(conn: &mut Conn, records: &[Curation]) -> Result<(), String> {
    let error = |e: curation::Error| e.to_string();
    let mut snapshot = Snapshot::new();
    let mut roots: Vec<String> = Vec::new();
    for record in records {
        roots.push(record.root.clone());
        for target in &record.withdraws {
            roots.push(
                Withdrawn::parse(target)
                    .map_err(|e| e.to_string())?
                    .root()
                    .to_owned(),
            );
        }
    }
    snapshot.load_roots(conn, roots).await.map_err(error)?;
    let slots: Vec<authored::UniqueSlot> = records
        .iter()
        .flat_map(|r| Statement::of(r).slots())
        .collect();
    snapshot.load_slots(conn, &slots).await.map_err(error)?;
    for record in records {
        let revision = curation::revision(record);
        let claimed: Vec<String> = Statement::of(record)
            .slots()
            .iter()
            .map(slot_digest)
            .collect();
        if claimed != record.slots {
            return Err(format!(
                "{revision}: its stored slots are not its statement's"
            ));
        }
        if record.origin == ValueOrigin::Authored {
            if let Some(expected) = record
                .value
                .as_ref()
                .filter(|_| record.kind == CurationKind::Withdrawal)
                .and_then(|value| value.get("expects"))
                .and_then(Value::as_str)
            {
                let expected = Revision::parse(expected).map_err(|e| e.to_string())?;
                let heads = snapshot.heads(&expected.root).map_err(error)?;
                if !heads.contains(&expected) {
                    return Err(format!(
                        "{revision}: {expected} is not current; current: {}",
                        list(&heads)
                    ));
                }
            }
            for target in &record.withdraws {
                let target = Withdrawn::parse(target).map_err(|e| e.to_string())?;
                let known = snapshot.records().keys().any(|r| match &target {
                    Withdrawn::Root(root) => &r.root == root,
                    Withdrawn::Revision(rev) => r == rev,
                });
                if !known {
                    return Err(format!(
                        "{revision}: withdrawal target {target} is not live"
                    ));
                }
            }
            let heads = snapshot.heads(&record.root).map_err(error)?;
            for base in &record.revises {
                let base = Revision::parse(base).map_err(|e| e.to_string())?;
                if !heads.contains(&base) {
                    return Err(format!(
                        "{revision}: base {base} is not current; current: {}",
                        list(&heads)
                    ));
                }
                let same_statement = snapshot
                    .record(&base)
                    .is_some_and(|b| Statement::of(b).slots() == Statement::of(record).slots());
                if !same_statement {
                    return Err(format!("{revision}: base {base} is another statement"));
                }
            }
            let slots = Statement::of(record).slots();
            let holders = snapshot.holders(&slots).map_err(error)?;
            let others: Vec<String> = holders
                .into_keys()
                .filter(|root| root != &record.root)
                .collect();
            if !others.is_empty() {
                return Err(format!(
                    "{revision}: its slot is now held by {}",
                    others.join(", ")
                ));
            }
        }
        snapshot.add(record.clone()).map_err(error)?;
    }
    Ok(())
}

/// A defined scale, as a rating or comparison checks against it.
#[derive(Clone, Debug, PartialEq)]
pub struct Scale {
    pub name: String,
    pub question: String,
    pub range: Option<(f64, f64)>,
    pub choices: Option<Vec<String>>,
}

impl Scale {
    pub fn from_value(name: &str, value: &Value) -> Option<Self> {
        Some(Self {
            name: name.to_owned(),
            question: value.get("question")?.as_str()?.to_owned(),
            range: value
                .get("range")
                .and_then(|r| Some((r.get(0)?.as_f64()?, r.get(1)?.as_f64()?))),
            choices: value.get("choices").and_then(|c| {
                c.as_array()?
                    .iter()
                    .map(|v| v.as_str().map(str::to_owned))
                    .collect()
            }),
        })
    }

    pub fn value(&self) -> Value {
        let mut value = json!({ "question": self.question });
        if let Some((low, high)) = self.range {
            value["range"] = json!([low, high]);
        }
        if let Some(choices) = &self.choices {
            value["choices"] = json!(choices);
        }
        value
    }

    /// Whether a rating value belongs to the scale. Unknown is not a value:
    /// `null`, non-finite numbers and out-of-range numbers are refused.
    pub fn check(&self, value: &Value) -> Result<(), String> {
        match (&self.range, &self.choices, value) {
            (Some((low, high)), _, Value::Number(n)) => {
                let v = n.as_f64().unwrap_or(f64::NAN);
                if v.is_finite() && *low <= v && v <= *high {
                    Ok(())
                } else {
                    Err(format!(
                        "a rating on {} is a number from {low} to {high}",
                        self.name
                    ))
                }
            }
            (_, Some(choices), Value::String(s)) if choices.contains(s) => Ok(()),
            (_, Some(choices), _) => Err(format!(
                "a rating on {} is one of: {}",
                self.name,
                choices.join(", ")
            )),
            _ => Err(format!("a rating on {} is a number", self.name)),
        }
    }

    /// A redefinition keeps every value the old definition accepted: a
    /// range only widens and choices are only added.
    pub fn accepts_all_of(&self, old: &Scale) -> bool {
        match (old.range, self.range, &old.choices, &self.choices) {
            (Some((old_low, old_high)), Some((low, high)), None, None) => {
                low <= old_low && old_high <= high
            }
            (None, None, Some(old), Some(new)) => old.iter().all(|c| new.contains(c)),
            _ => false,
        }
    }
}

struct Resolver<'a> {
    snapshot: Snapshot,
    caller: &'a Caller,
    drafts: Vec<Draft>,
    /// Collections this request creates, by name, with the creating draft.
    collections: BTreeMap<String, usize>,
    /// Entry positions per collection key as resolved so far, including the
    /// entries this request inserts.
    entries: BTreeMap<Key, Vec<(OrderKey, String)>>,
    /// Scales this request defines.
    scales: BTreeMap<String, Scale>,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn rank(key: &Key) -> (u8, &str, usize) {
            match key {
                Key::None => (0, "", 0),
                Key::Text(text) => (1, text, 0),
                Key::Draft(index) => (2, "", *index),
            }
        }
        rank(self).cmp(&rank(other))
    }
}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for Key {}

fn suffix() -> u32 {
    uuid::Uuid::now_v7().as_u128() as u32
}

fn parse_revision(act: usize, text: &str) -> Result<Revision, PlanError> {
    Revision::parse(text).map_err(|e| invalid(act, e.to_string()))
}

impl Resolver<'_> {
    fn statement<'s>(
        &'s self,
        kind: CurationKind,
        target: Option<&'s Target>,
        key: Option<&'s str>,
        value: Option<&'s Value>,
    ) -> Statement<'s> {
        Statement {
            kind,
            target,
            key,
            value,
            author: &self.caller.author,
            origin: ValueOrigin::Authored,
            cites: None,
        }
    }

    /// The current roots holding a statement's slots.
    async fn holders(
        &mut self,
        conn: &mut Conn,
        kind: CurationKind,
        target: Option<&Target>,
        key: Option<&str>,
        value: Option<&Value>,
    ) -> Result<BTreeMap<String, Vec<Revision>>, PlanError> {
        let slots = self.statement(kind, target, key, value).slots();
        self.snapshot.load_slots(conn, &slots).await?;
        Ok(self.snapshot.holders(&slots)?)
    }

    /// Check the target names a live item and, for a fixed revision or
    /// rendition, one it has. With `pin`, a target that follows the item is
    /// fixed to its current revision.
    async fn target(
        &self,
        conn: &mut Conn,
        act: usize,
        target: &Target,
        pin: bool,
    ) -> Result<Target, PlanError> {
        let item = reads::item(conn, &target.item)
            .await?
            .ok_or_else(|| invalid(act, format!("no library item {}", target.item)))?;
        let mut out = target.clone();
        if let Some(revision) = &target.revision
            && !reads::revisions(conn, &target.item)
                .await?
                .iter()
                .any(|r| &r.revision == revision)
        {
            return Err(invalid(
                act,
                format!("{} has no revision {revision}", target.item),
            ));
        }
        if let Some(rendition) = &target.rendition
            && !reads::renditions(conn, &target.item)
                .await?
                .iter()
                .any(|r| &r.rendition == rendition)
        {
            return Err(invalid(
                act,
                format!("{} has no rendition {rendition}", target.item),
            ));
        }
        if pin && out.revision.is_none() {
            out.revision = Some(item.revision);
        }
        Ok(out)
    }

    /// The one current record holding a name (a collection or a scale).
    async fn named(
        &mut self,
        conn: &mut Conn,
        act: usize,
        kind: CurationKind,
        name: &str,
    ) -> Result<Option<Curation>, PlanError> {
        let holders = self.snapshot.named(conn, kind, name).await?;
        match holders.len() {
            0 => Ok(None),
            1 => {
                let (root, heads) = holders.into_iter().next().expect("one holder");
                if heads.len() > 1 {
                    return Err(invalid(
                        act,
                        format!(
                            "{} `{name}` ({root}) has conflicting revisions: {}",
                            kind.as_str(),
                            list(&heads)
                        ),
                    ));
                }
                Ok(self.snapshot.record(&heads[0]).cloned())
            }
            _ => Err(invalid(
                act,
                format!(
                    "several {}s are named `{name}`: {}",
                    kind.as_str(),
                    holders.keys().cloned().collect::<Vec<_>>().join(", ")
                ),
            )),
        }
    }

    async fn scale(&mut self, conn: &mut Conn, act: usize, name: &str) -> Result<Scale, PlanError> {
        if let Some(scale) = self.scales.get(name) {
            return Ok(scale.clone());
        }
        let record = self
            .named(conn, act, CurationKind::Scale, name)
            .await?
            .ok_or_else(|| {
                invalid(
                    act,
                    format!("no scale `{name}`; define it first (define_scale)"),
                )
            })?;
        record
            .value
            .as_ref()
            .and_then(|value| Scale::from_value(name, value))
            .ok_or_else(|| invalid(act, format!("scale `{name}` has no readable definition")))
    }

    /// The base a write names with `expects`: it must be a revision of the
    /// record holding the same statement (the same uniqueness slots: kind,
    /// target, key, the caller as author, authored origin), so `expects`
    /// never turns a write into a revision of another statement or of
    /// another author's record.
    #[allow(clippy::too_many_arguments)]
    async fn expected(
        &mut self,
        conn: &mut Conn,
        act: usize,
        expects: &Option<String>,
        kind: CurationKind,
        target: &Target,
        key: &str,
        value: Option<&Value>,
    ) -> Result<Vec<Revision>, PlanError> {
        let Some(expected) = expects else {
            return Ok(Vec::new());
        };
        let base = parse_revision(act, expected)?;
        self.snapshot.load_roots(conn, [base.root.clone()]).await?;
        let wanted = self.statement(kind, Some(target), Some(key), value).slots();
        let same = self
            .snapshot
            .record(&base)
            .is_some_and(|record| Statement::of(record).slots() == wanted);
        if !same {
            return Err(invalid(
                act,
                format!(
                    "{base} is not a revision of your {} `{key}` on {}",
                    kind.as_str(),
                    target.item
                ),
            ));
        }
        Ok(vec![base])
    }

    /// The current single revision of an entry, with its record.
    async fn entry(
        &mut self,
        conn: &mut Conn,
        act: usize,
        entry: &str,
    ) -> Result<(Revision, Curation), PlanError> {
        self.snapshot.load_roots(conn, [entry.to_owned()]).await?;
        let heads = self.snapshot.heads(entry)?;
        let head = match heads.as_slice() {
            [] => return Err(invalid(act, format!("no current entry {entry}"))),
            [head] => head.clone(),
            _ => {
                return Err(invalid(
                    act,
                    format!("entry {entry} has conflicting revisions: {}", list(&heads)),
                ));
            }
        };
        let record = self.snapshot.record(&head).cloned().expect("loaded head");
        if record.kind != CurationKind::Entry {
            return Err(invalid(act, format!("{entry} is not a collection entry")));
        }
        Ok((head, record))
    }

    /// The current entries of a collection, ordered, as resolved so far.
    async fn positions(
        &mut self,
        conn: &mut Conn,
        collection: &Key,
    ) -> Result<Vec<(OrderKey, String)>, PlanError> {
        if let Some(known) = self.entries.get(collection) {
            return Ok(known.clone());
        }
        let mut out = Vec::new();
        if let Key::Text(root) = collection {
            self.snapshot
                .load_where(conn, &["key"], std::slice::from_ref(root))
                .await?;
            for record in self.snapshot.current()? {
                if record.kind == CurationKind::Entry
                    && record.key.as_deref() == Some(root.as_str())
                    && let Some(position) = entry_position(&record)
                {
                    out.push((position, record.root));
                }
            }
        }
        out.sort();
        self.entries.insert(collection.clone(), out.clone());
        Ok(out)
    }

    /// A key at the end, or right after or before a named entry, skipping
    /// the entry being moved.
    fn position_in(
        act: usize,
        entries: &[(OrderKey, String)],
        after: Option<&str>,
        before: Option<&str>,
        moving: Option<&str>,
    ) -> Result<OrderKey, PlanError> {
        let list: Vec<&(OrderKey, String)> = entries
            .iter()
            .filter(|(_, root)| Some(root.as_str()) != moving)
            .collect();
        let find = |entry: &str| {
            list.iter()
                .position(|(_, root)| root == entry)
                .ok_or_else(|| invalid(act, format!("{entry} is not an entry of this collection")))
        };
        let (low, high) = match (after, before) {
            (Some(_), Some(_)) => {
                return Err(invalid(act, "give `after` or `before`, not both"));
            }
            (Some(after), None) => {
                let at = find(after)?;
                (Some(&list[at].0), list.get(at + 1).map(|(key, _)| key))
            }
            (None, Some(before)) => {
                let at = find(before)?;
                (at.checked_sub(1).map(|i| &list[i].0), Some(&list[at].0))
            }
            (None, None) => (list.last().map(|(key, _)| key), None),
        };
        OrderKey::between(low, high, suffix()).map_err(|e| invalid(act, e.to_string()))
    }

    async fn act(&mut self, conn: &mut Conn, act: usize, request: &Act) -> Result<(), PlanError> {
        match request {
            Act::Tag {
                target,
                tag,
                expects,
            } => {
                let tag = label(act, tag)?;
                let target = self.target(conn, act, target, false).await?;
                let rejected = Value::Bool(false);
                let opposite = self
                    .holders(
                        conn,
                        CurationKind::Tag,
                        Some(&target),
                        Some(&tag),
                        Some(&rejected),
                    )
                    .await?;
                let revises = self
                    .expected(conn, act, expects, CurationKind::Tag, &target, &tag, None)
                    .await?;
                // The opposite statement is withdrawn by its own record, so
                // withdrawing this tag later does not bring it back.
                if !opposite.is_empty() {
                    self.drafts.push(Draft::withdrawal(
                        opposite.into_keys().map(Withdrawn::Root).collect(),
                    ));
                }
                self.drafts.push(Draft {
                    target: Some(target),
                    key: Key::Text(tag),
                    revises,
                    ..Draft::authored(CurationKind::Tag)
                });
            }
            Act::Reject { target, tag } => {
                let tag = label(act, tag)?;
                let target = self.target(conn, act, target, false).await?;
                let applied = self
                    .holders(conn, CurationKind::Tag, Some(&target), Some(&tag), None)
                    .await?;
                if !applied.is_empty() {
                    self.drafts.push(Draft::withdrawal(
                        applied.into_keys().map(Withdrawn::Root).collect(),
                    ));
                }
                self.drafts.push(Draft {
                    target: Some(target),
                    key: Key::Text(tag),
                    value: Some(Value::Bool(false)),
                    ..Draft::authored(CurationKind::Tag)
                });
            }
            Act::Untag { target, tag } => {
                let applied = self
                    .holders(conn, CurationKind::Tag, Some(target), Some(tag), None)
                    .await?;
                if applied.is_empty() {
                    return Err(invalid(
                        act,
                        format!(
                            "{} has not applied `{tag}` to {}; withdraw another author's record by its reference",
                            self.caller.author, target.item
                        ),
                    ));
                }
                self.drafts.push(Draft::withdrawal(
                    applied.into_keys().map(Withdrawn::Root).collect(),
                ));
            }
            Act::Set {
                target,
                property,
                value,
                expects,
            } => {
                let property = label(act, property)?;
                if !matches!(
                    value,
                    Value::Null | Value::String(_) | Value::Number(_) | Value::Bool(_)
                ) {
                    return Err(invalid(
                        act,
                        "a property value is text, a number, true or false, or null for empty",
                    ));
                }
                if !value.is_null()
                    && let Some(definition) = self
                        .named(conn, act, CurationKind::PropertyDefinition, &property)
                        .await?
                {
                    check_property(&property, definition.value.as_ref(), value)
                        .map_err(|reason| invalid(act, reason))?;
                }
                let target = self.target(conn, act, target, false).await?;
                let revises = self
                    .expected(
                        conn,
                        act,
                        expects,
                        CurationKind::Property,
                        &target,
                        &property,
                        Some(value),
                    )
                    .await?;
                self.drafts.push(Draft {
                    target: Some(target),
                    key: Key::Text(property),
                    value: Some(value.clone()),
                    revises,
                    ..Draft::authored(CurationKind::Property)
                });
            }
            Act::Note { target, text } => {
                if text.trim().is_empty() {
                    return Err(invalid(act, "a note needs text"));
                }
                let target = self.target(conn, act, target, false).await?;
                self.drafts.push(Draft {
                    target: Some(target),
                    value: Some(Value::String(text.clone())),
                    ..Draft::authored(CurationKind::Note)
                });
            }
            Act::Rate {
                target,
                scale,
                value,
                expects,
            } => {
                let defined = self.scale(conn, act, scale).await?;
                defined
                    .check(value)
                    .map_err(|reason| invalid(act, reason))?;
                let target = self.target(conn, act, target, false).await?;
                let revises = self
                    .expected(
                        conn,
                        act,
                        expects,
                        CurationKind::Rating,
                        &target,
                        scale,
                        Some(value),
                    )
                    .await?;
                self.drafts.push(Draft {
                    target: Some(target),
                    key: Key::Text(scale.clone()),
                    value: Some(value.clone()),
                    revises,
                    ..Draft::authored(CurationKind::Rating)
                });
            }
            Act::Collect {
                target,
                collection,
                after,
                before,
            } => {
                let name = label(act, collection)?;
                let target = self.target(conn, act, target, true).await?;
                let key = match self.collections.get(&name) {
                    Some(index) => Key::Draft(*index),
                    None => match self
                        .named(conn, act, CurationKind::Collection, &name)
                        .await?
                    {
                        Some(existing) => Key::Text(existing.root),
                        None => {
                            self.drafts.push(Draft {
                                key: Key::Text(name.clone()),
                                ..Draft::authored(CurationKind::Collection)
                            });
                            let index = self.drafts.len() - 1;
                            self.collections.insert(name, index);
                            Key::Draft(index)
                        }
                    },
                };
                let mut entries = self.positions(conn, &key).await?;
                let position =
                    Self::position_in(act, &entries, after.as_deref(), before.as_deref(), None)?;
                self.drafts.push(Draft {
                    target: Some(target),
                    key: key.clone(),
                    value: Some(json!({ "position": position })),
                    ..Draft::authored(CurationKind::Entry)
                });
                // Later acts of this request place against this entry too; it
                // has no root until placed, so it cannot be named by them.
                entries.push((position, String::new()));
                entries.sort();
                self.entries.insert(key, entries);
            }
            Act::Uncollect { entry } => {
                self.entry(conn, act, entry).await?;
                self.drafts
                    .push(Draft::withdrawal(vec![Withdrawn::Root(entry.clone())]));
            }
            Act::Move {
                entry,
                after,
                before,
            } => {
                let (head, record) = self.entry(conn, act, entry).await?;
                let key = Key::Text(record.key.clone().unwrap_or_default());
                let mut entries = self.positions(conn, &key).await?;
                let position = Self::position_in(
                    act,
                    &entries,
                    after.as_deref(),
                    before.as_deref(),
                    Some(entry),
                )?;
                self.drafts.push(Draft {
                    target: record.target.clone(),
                    key: key.clone(),
                    value: Some(json!({ "position": position })),
                    revises: vec![head],
                    ..Draft::authored(CurationKind::Entry)
                });
                for (key, root) in &mut entries {
                    if root == entry {
                        *key = position.clone();
                    }
                }
                entries.sort();
                self.entries.insert(key, entries);
            }
            Act::Judge {
                scale,
                order,
                blind,
            } => {
                self.scale(conn, act, scale).await?;
                let mut seen = std::collections::BTreeSet::new();
                let mut groups = Vec::with_capacity(order.len());
                for group in order {
                    if group.is_empty() {
                        return Err(invalid(act, "a tie group is never empty"));
                    }
                    let mut pinned = Vec::with_capacity(group.len());
                    for target in group {
                        if !seen.insert(target.item.clone()) {
                            return Err(invalid(
                                act,
                                format!("{} is named twice in one comparison", target.item),
                            ));
                        }
                        pinned.push(self.target(conn, act, target, true).await?);
                    }
                    groups.push(pinned);
                }
                if seen.len() < 2 {
                    return Err(invalid(act, "a comparison orders two or more items"));
                }
                self.drafts.push(Draft {
                    key: Key::Text(scale.clone()),
                    value: Some(json!({ "order": groups, "blind": blind })),
                    ..Draft::authored(CurationKind::Comparison)
                });
            }
            Act::DefineScale {
                name,
                question,
                range,
                choices,
            } => {
                let name = label(act, name)?;
                let scale = Scale {
                    name: name.clone(),
                    question: question.trim().to_owned(),
                    range: *range,
                    choices: choices.clone(),
                };
                if scale.question.is_empty() {
                    return Err(invalid(act, "a scale states the question a judge answers"));
                }
                match (&scale.range, &scale.choices) {
                    (Some((low, high)), None)
                        if low.is_finite() && high.is_finite() && low < high => {}
                    (None, Some(choices))
                        if !choices.is_empty()
                            && choices.iter().all(|c| !c.trim().is_empty())
                            && choices
                                .iter()
                                .collect::<std::collections::BTreeSet<_>>()
                                .len()
                                == choices.len() => {}
                    _ => {
                        return Err(invalid(
                            act,
                            "a scale has a finite range low < high, or distinct non-empty choices",
                        ));
                    }
                }
                if let Some(existing) = self.named(conn, act, CurationKind::Scale, &name).await?
                    && let Some(old) = existing
                        .value
                        .as_ref()
                        .and_then(|value| Scale::from_value(&name, value))
                    && !scale.accepts_all_of(&old)
                {
                    return Err(invalid(
                        act,
                        format!(
                            "redefining `{name}` would refuse values it accepted; a range only widens and choices are only added"
                        ),
                    ));
                }
                self.drafts.push(Draft {
                    key: Key::Text(name.clone()),
                    value: Some(scale.value()),
                    ..Draft::authored(CurationKind::Scale)
                });
                self.scales.insert(name, scale);
            }
            Act::DefineProperty {
                name,
                value_type,
                choices,
            } => {
                let name = label(act, name)?;
                let choices = match (value_type, choices) {
                    (PropertyType::Choice, Some(c)) if !c.is_empty() => Some(c.clone()),
                    (PropertyType::Choice, _) => {
                        return Err(invalid(act, "a choice property lists its choices"));
                    }
                    (_, None) => None,
                    (_, Some(_)) => {
                        return Err(invalid(act, "only a choice property has choices"));
                    }
                };
                if let Some(existing) = self
                    .named(conn, act, CurationKind::PropertyDefinition, &name)
                    .await?
                    && let Some(old) = existing.value.as_ref()
                {
                    let same_type = old.get("value_type") == Some(&json!(value_type));
                    let kept = old
                        .get("choices")
                        .and_then(Value::as_array)
                        .is_none_or(|old| {
                            old.iter().all(|c| {
                                choices.as_ref().is_some_and(|new| {
                                    c.as_str().is_some_and(|c| new.iter().any(|n| n == c))
                                })
                            })
                        });
                    if !same_type || !kept {
                        return Err(invalid(
                            act,
                            format!("redefining `{name}` would refuse values it accepted"),
                        ));
                    }
                }
                let mut value = json!({ "value_type": value_type });
                if let Some(choices) = choices {
                    value["choices"] = json!(choices);
                }
                self.drafts.push(Draft {
                    key: Key::Text(name),
                    value: Some(value),
                    ..Draft::authored(CurationKind::PropertyDefinition)
                });
            }
            Act::DefineTag {
                tag,
                namespace,
                description,
            } => {
                let tag = label(act, tag)?;
                self.drafts.push(Draft {
                    key: Key::Text(tag),
                    value: Some(json!({ "namespace": namespace, "description": description })),
                    ..Draft::authored(CurationKind::TagDefinition)
                });
            }
            Act::Relate {
                relation,
                tag,
                other,
            } => {
                let (tag, other) = (label(act, tag)?, label(act, other)?);
                if tag == other {
                    return Err(invalid(act, "a tag is not related to itself"));
                }
                let pair = match relation {
                    TagRelation::Alias => {
                        let mut pair = [tag, other];
                        pair.sort();
                        pair
                    }
                    TagRelation::Implies => {
                        self.snapshot
                            .load_kind(conn, CurationKind::TagRelation)
                            .await?;
                        let edges: Vec<(String, String)> = self
                            .snapshot
                            .current()?
                            .into_iter()
                            .chain(self.drafts.iter().filter_map(|d| {
                                (d.kind == CurationKind::TagRelation).then(|| Curation {
                                    key: match &d.key {
                                        Key::Text(k) => Some(k.clone()),
                                        _ => None,
                                    },
                                    value: d.value.clone(),
                                    ..placeholder(CurationKind::TagRelation)
                                })
                            }))
                            .filter(|r| {
                                r.kind == CurationKind::TagRelation
                                    && r.key.as_deref() == Some("implies")
                            })
                            .filter_map(|r| {
                                let pair: (String, String) =
                                    serde_json::from_value(r.value?).ok()?;
                                Some(pair)
                            })
                            .collect();
                        if closes_cycle(&edges, &tag, &other) {
                            return Err(invalid(
                                act,
                                format!(
                                    "`{tag}` implies `{other}` would close a cycle of implications"
                                ),
                            ));
                        }
                        [tag, other]
                    }
                };
                self.drafts.push(Draft {
                    key: Key::Text(
                        match relation {
                            TagRelation::Alias => "alias",
                            TagRelation::Implies => "implies",
                        }
                        .to_owned(),
                    ),
                    value: Some(json!(pair)),
                    ..Draft::authored(CurationKind::TagRelation)
                });
            }
            Act::Dismiss {
                collection,
                target,
                query,
            } => {
                let collection = self
                    .named(conn, act, CurationKind::Collection, collection)
                    .await?
                    .ok_or_else(|| invalid(act, format!("no collection named `{collection}`")))?;
                let target = self.target(conn, act, target, false).await?;
                self.drafts.push(Draft {
                    target: Some(target),
                    key: Key::Text(collection.root),
                    value: Some(Value::String(label(act, query)?)),
                    ..Draft::authored(CurationKind::Dismissal)
                });
            }
            Act::Withdraw { record, expects } => {
                let target = Withdrawn::parse(record).map_err(|e| invalid(act, e.to_string()))?;
                self.snapshot
                    .load_roots(conn, [target.root().to_owned()])
                    .await?;
                if let Some(expected) = expects {
                    let expected = parse_revision(act, expected)?;
                    let heads = self.snapshot.heads(target.root())?;
                    if expected.root != target.root() || !heads.contains(&expected) {
                        return Err(PlanError::Refused(format!(
                            "{expected} is not current; current: {}",
                            list(&heads)
                        )));
                    }
                }
                let mut draft = Draft::withdrawal(vec![target]);
                if let Some(expected) = expects {
                    draft.value = Some(json!({ "expects": expected }));
                }
                self.drafts.push(draft);
            }
            Act::Undo { request } => {
                let roots = self
                    .snapshot
                    .load_where(conn, &["request"], std::slice::from_ref(request))
                    .await?;
                let written: Vec<Revision> = self
                    .snapshot
                    .records()
                    .iter()
                    .filter(|(_, record)| &record.request == request)
                    .map(|(revision, _)| revision.clone())
                    .collect();
                if written.is_empty() {
                    return Err(invalid(
                        act,
                        format!("request {request} wrote no live record"),
                    ));
                }
                let dependents: Vec<Revision> = self
                    .snapshot
                    .records()
                    .iter()
                    .filter(|(_, record)| &record.request != request)
                    .filter(|(_, record)| {
                        record
                            .revises
                            .iter()
                            .any(|base| written.iter().any(|w| &w.to_string() == base))
                            || record.withdraws.iter().any(|target| {
                                roots.contains(target)
                                    || written.iter().any(|w| &w.to_string() == target)
                            })
                    })
                    .map(|(revision, _)| revision.clone())
                    .collect();
                if !dependents.is_empty() {
                    return Err(PlanError::Refused(format!(
                        "request {request} has later records that depend on it: {}",
                        list(&dependents)
                    )));
                }
                self.drafts.push(Draft::withdrawal(
                    written.into_iter().map(Withdrawn::Revision).collect(),
                ));
            }
        }
        Ok(())
    }
}

/// A name or label: any text with something besides whitespace, punctuation
/// included, without surrounding whitespace.
fn label(act: usize, text: &str) -> Result<String, PlanError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(invalid(act, "a label needs text"));
    }
    Ok(trimmed.to_owned())
}

/// Whether adding `from` implies `to` to these implications closes a cycle:
/// `to` already implies `from`, directly or through others.
pub fn closes_cycle(edges: &[(String, String)], from: &str, to: &str) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![to];
    while let Some(tag) = pending.pop() {
        if tag == from {
            return true;
        }
        if seen.insert(tag) {
            pending.extend(
                edges
                    .iter()
                    .filter(|(a, _)| a == tag)
                    .map(|(_, b)| b.as_str()),
            );
        }
    }
    false
}

/// A value checked against its property's definition.
fn check_property(name: &str, definition: Option<&Value>, value: &Value) -> Result<(), String> {
    let Some(definition) = definition else {
        return Ok(());
    };
    let fits = match definition.get("value_type").and_then(Value::as_str) {
        Some("text") => value.is_string(),
        Some("number") => value.is_number(),
        Some("boolean") => value.is_boolean(),
        Some("choice") => definition
            .get("choices")
            .and_then(Value::as_array)
            .is_some_and(|choices| choices.contains(value)),
        _ => true,
    };
    if fits {
        Ok(())
    } else {
        Err(format!(
            "`{value}` is not a value of property `{name}` ({definition})"
        ))
    }
}

/// A record holding only a kind, for comparisons inside a request.
fn placeholder(kind: CurationKind) -> Curation {
    Curation {
        root: String::new(),
        rev: String::new(),
        revises: Vec::new(),
        withdraws: Vec::new(),
        kind,
        target: None,
        key: None,
        value: None,
        origin: ValueOrigin::Authored,
        cites: None,
        author: String::new(),
        via: String::new(),
        at: Timestamp::now(),
        request: String::new(),
        request_digest: String::new(),
        slots: Vec::new(),
    }
}

/// An entry's order key.
pub fn entry_position(record: &Curation) -> Option<OrderKey> {
    record
        .value
        .as_ref()?
        .get("position")?
        .as_str()
        .and_then(|text| OrderKey::parse(text).ok())
}

fn list(revisions: &[Revision]) -> String {
    revisions
        .iter()
        .map(Revision::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The records of a request as the caller sees them.
pub fn written(records: &[Curation]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            json!({
                "record": format!("{}@{}", record.root, record.rev),
                "kind": record.kind,
                "revises": record.revises,
                "withdraws": record.withdraws,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
