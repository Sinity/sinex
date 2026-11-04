//! Curation records as authored records: the bridge between the library's
//! stored `library.curation` revisions and the shared `authored` rules.
//!
//! Each kind declares its uniqueness key here, so "the same statement" has one
//! definition for planning a write, for the slot digests a record stores and
//! for the slots a loaded record occupies. Which revisions are current is
//! never decided here: [`Snapshot`] loads the live records a question needs
//! and the `authored` crate's currency function answers it.
use authored::{
    Currency, Header, History, Part, Revision, RootState, Target as Withdrawn, UniqueSlot,
    Uniqueness, WhenHeld,
};
use serde::de::DeserializeOwned;
use sinex_model::event_types::library::{Curation, CurationKind, Target, ValueOrigin};
use sinex_store::Connection as Conn;
use sinex_store::payloads::{self, RecordStates};
use std::collections::{BTreeMap, BTreeSet};

/// The stored event type of every curation revision.
pub const CURATION: &str = "library.curation";

/// The author whose authored values rank first.
pub const OPERATOR: &str = "operator";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] sinex_store::Error),
    #[error("stored {0} payload does not decode: {1}")]
    Decode(&'static str, String),
    #[error("stored curation record {0} names an invalid revision: {1}")]
    Identity(String, authored::IdentityError),
    #[error("withdrawals of {0} form a cycle")]
    Cycle(Revision),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn decode<T: DeserializeOwned>(
    kind: &'static str,
    rows: Vec<serde_json::Value>,
) -> Result<Vec<T>> {
    rows.into_iter()
        .map(|v| serde_json::from_value(v).map_err(|e| Error::Decode(kind, e.to_string())))
        .collect()
}

/// The uniqueness key each kind declares: which records hold "the same
/// statement", so a second write revises the first (or, for a tag, answers
/// that it is already there). Kinds without one may hold many records.
pub fn uniqueness(kind: CurationKind) -> Option<Uniqueness> {
    const STATEMENT: &[&str] = &["target", "key", "author", "origin"];
    match kind {
        // The value is in the key: tagging again says nothing new, and
        // rejecting a tag is a different statement than applying it.
        CurationKind::Tag => Some(Uniqueness::new(
            &["target", "key", "value", "author", "origin"],
            WhenHeld::Already,
        )),
        CurationKind::Property | CurationKind::Rating => {
            Some(Uniqueness::new(STATEMENT, WhenHeld::Revise))
        }
        // Labels are revisable; identity is the record's root.
        CurationKind::Collection
        | CurationKind::Scale
        | CurationKind::PropertyDefinition
        | CurationKind::TagDefinition => Some(Uniqueness::new(&["key"], WhenHeld::Revise)),
        // A relation or a dismissal says everything in its key.
        CurationKind::TagRelation => Some(Uniqueness::new(&["key", "value"], WhenHeld::Already)),
        CurationKind::Dismissal => Some(Uniqueness::new(
            &["target", "key", "value"],
            WhenHeld::Already,
        )),
        // Every note, comparison and entry counts on its own: an item can be
        // in one collection twice, and every comparison is a judgment.
        CurationKind::Note
        | CurationKind::Comparison
        | CurationKind::Entry
        | CurationKind::Withdrawal => None,
    }
}

/// The canonical text of a target inside a uniqueness slot.
pub fn target_text(target: &Target) -> String {
    serde_json::to_string(target).expect("targets serialize")
}

/// The origin as one slot value: each imported source and each proposing
/// method holds its own statements, so their support is kept apart.
pub fn origin_text(origin: ValueOrigin, cites: Option<&str>) -> String {
    match origin {
        ValueOrigin::Authored => "authored".into(),
        ValueOrigin::Imported => format!("imported:{}", cites.unwrap_or_default()),
        ValueOrigin::Proposed => format!("proposed:{}", cites.unwrap_or_default()),
    }
}

/// What a record says, as its kind's uniqueness key reads it.
pub struct Statement<'a> {
    pub kind: CurationKind,
    pub target: Option<&'a Target>,
    pub key: Option<&'a str>,
    pub value: Option<&'a serde_json::Value>,
    pub author: &'a str,
    pub origin: ValueOrigin,
    pub cites: Option<&'a str>,
}

impl Statement<'_> {
    pub fn of(record: &Curation) -> Statement<'_> {
        Statement {
            kind: record.kind,
            target: record.target.as_ref(),
            key: record.key.as_deref(),
            value: record.value.as_ref(),
            author: &record.author,
            origin: record.origin,
            cites: record.cites.as_deref(),
        }
    }

    /// The slots this statement occupies, with what a held slot means.
    pub fn unique(&self) -> Option<authored::Unique> {
        let declared = uniqueness(self.kind)?;
        Some(declared.unique(self.kind.as_str(), |component| {
            match component {
                "target" => self
                    .target
                    .map_or(Part::Absent, |target| Part::One(target_text(target))),
                "key" => self
                    .key
                    .map_or(Part::Absent, |key| Part::One(key.to_owned())),
                "value" => self
                    .value
                    .map_or(Part::Absent, |value| Part::One(value.to_string())),
                "author" => Part::One(self.author.to_owned()),
                "origin" => Part::One(origin_text(self.origin, self.cites)),
                other => unreachable!("no curation kind declares `{other}`"),
            }
        }))
    }

    pub fn slots(&self) -> Vec<UniqueSlot> {
        self.unique().map(|unique| unique.slots).unwrap_or_default()
    }
}

/// The stored name of a slot.
pub fn slot_digest(slot: &UniqueSlot) -> String {
    slot.digest().iter().map(|b| format!("{b:02x}")).collect()
}

/// A stored record's revision.
pub fn revision(record: &Curation) -> Revision {
    Revision::new(&record.root, &record.rev)
}

/// The shared header of a stored record.
pub fn header(record: &Curation) -> Result<Header> {
    let invalid = |e| Error::Identity(record.rev.clone(), e);
    Ok(Header {
        root: record.root.clone(),
        rev: record.rev.clone(),
        kind: record.kind.as_str().to_owned(),
        key: record.key.clone(),
        value: record.value.clone(),
        author: record.author.clone(),
        via: record.via.clone(),
        at: i64::try_from(record.at.unix_ns() / 1_000_000).unwrap_or(i64::MAX),
        revises: record
            .revises
            .iter()
            .map(|r| Revision::parse(r).map_err(invalid))
            .collect::<Result<_>>()?,
        withdraws: record
            .withdraws
            .iter()
            .map(|t| Withdrawn::parse(t).map_err(invalid))
            .collect::<Result<_>>()?,
    })
}

/// Live curation records held in memory with the shared lookups, loaded on
/// demand. Whatever a question touches is loaded with everything that can
/// change its currency: every revision of each loaded record, and every
/// withdrawal naming one of them, transitively.
#[derive(Default)]
pub struct Snapshot {
    pub history: History,
    records: BTreeMap<Revision, Curation>,
    loaded: BTreeSet<String>,
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every loaded record, by revision.
    pub fn records(&self) -> &BTreeMap<Revision, Curation> {
        &self.records
    }

    pub fn record(&self, revision: &Revision) -> Option<&Curation> {
        self.records.get(revision)
    }

    /// Hold a record not yet stored, so later questions see it.
    pub fn add(&mut self, record: Curation) -> Result<()> {
        let slots = Statement::of(&record).slots();
        self.history.push(header(&record)?, slots);
        self.records.insert(revision(&record), record);
        Ok(())
    }

    /// Load these records and everything their currency depends on.
    pub async fn load_roots(
        &mut self,
        conn: &mut Conn,
        roots: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        let mut pending: BTreeSet<String> = roots
            .into_iter()
            .filter(|root| !self.loaded.contains(root))
            .collect();
        while !pending.is_empty() {
            let asked: Vec<String> = std::mem::take(&mut pending).into_iter().collect();
            self.loaded.extend(asked.iter().cloned());
            let rows =
                payloads::live_where(conn, CURATION, &["root"], &asked, RecordStates::Any).await?;
            let mut names = Vec::new();
            for record in decode::<Curation>(CURATION, rows)? {
                names.push(record.root.clone());
                names.push(format!("{}@{}", record.root, record.rev));
                self.add(record)?;
            }
            names.sort();
            names.dedup();
            if names.is_empty() {
                continue;
            }
            let rows = payloads::live_containing(
                conn,
                CURATION,
                &["withdraws"],
                &names,
                RecordStates::Any,
            )
            .await?;
            for withdrawal in decode::<Curation>(CURATION, rows)? {
                if !self.loaded.contains(&withdrawal.root) {
                    pending.insert(withdrawal.root);
                }
            }
        }
        Ok(())
    }

    /// Load the records whose payload `field` is one of `values`, with their
    /// currency closure. Returns their roots.
    pub async fn load_where(
        &mut self,
        conn: &mut Conn,
        field: &[&str],
        values: &[String],
    ) -> Result<BTreeSet<String>> {
        if values.is_empty() {
            return Ok(BTreeSet::new());
        }
        let rows = payloads::live_where(conn, CURATION, field, values, RecordStates::Any).await?;
        let roots: BTreeSet<String> = decode::<Curation>(CURATION, rows)?
            .into_iter()
            .map(|record| record.root)
            .collect();
        self.load_roots(conn, roots.iter().cloned()).await?;
        Ok(roots)
    }

    /// Load every record holding one of these slots.
    pub async fn load_slots(&mut self, conn: &mut Conn, slots: &[UniqueSlot]) -> Result<()> {
        let digests: Vec<String> = slots.iter().map(slot_digest).collect();
        if digests.is_empty() {
            return Ok(());
        }
        let rows =
            payloads::live_containing(conn, CURATION, &["slots"], &digests, RecordStates::Any)
                .await?;
        let roots: Vec<String> = decode::<Curation>(CURATION, rows)?
            .into_iter()
            .map(|record| record.root)
            .collect();
        self.load_roots(conn, roots).await
    }

    /// The current records holding a name: a collection's or a scale's.
    /// More than one root is an ambiguity the caller reports.
    pub async fn named(
        &mut self,
        conn: &mut Conn,
        kind: CurationKind,
        name: &str,
    ) -> Result<BTreeMap<String, Vec<Revision>>> {
        let slots = Statement {
            kind,
            target: None,
            key: Some(name),
            value: None,
            author: "",
            origin: ValueOrigin::Authored,
            cites: None,
        }
        .slots();
        self.load_slots(conn, &slots).await?;
        self.holders(&slots)
    }

    /// Every live record of one kind, with its currency closure.
    pub async fn load_kind(&mut self, conn: &mut Conn, kind: CurationKind) -> Result<()> {
        self.load_where(conn, &["kind"], &[kind.as_str().to_owned()])
            .await
            .map(drop)
    }

    /// The current revisions of a loaded record: one, none when it is
    /// withdrawn, several when it is in conflict.
    pub fn heads(&mut self, root: &str) -> Result<Vec<Revision>> {
        Currency::new(&mut self.history).heads(root).map_err(cycle)
    }

    pub fn state(&mut self, root: &str) -> Result<RootState> {
        Currency::new(&mut self.history)
            .root_state(root)
            .map_err(cycle)
    }

    /// The current records holding any of these slots, by root.
    pub fn holders(&mut self, slots: &[UniqueSlot]) -> Result<BTreeMap<String, Vec<Revision>>> {
        let mut found = BTreeSet::new();
        for slot in slots {
            found.extend(
                authored::Holders::holders(&mut self.history, slot)
                    .unwrap_or_else(|never| match never {}),
            );
        }
        let mut currency = Currency::new(&mut self.history);
        let mut by_root: BTreeMap<String, Vec<Revision>> = BTreeMap::new();
        for holder in found {
            if currency.current(&holder).map_err(cycle)? {
                by_root.entry(holder.root.clone()).or_default().push(holder);
            }
        }
        Ok(by_root)
    }

    /// Every current loaded record, in revision order.
    pub fn current(&mut self) -> Result<Vec<Curation>> {
        let roots: BTreeSet<String> = self.records.keys().map(|r| r.root.clone()).collect();
        let mut out = Vec::new();
        for root in roots {
            for head in self.heads(&root)? {
                out.push(self.records[&head].clone());
            }
        }
        Ok(out)
    }
}

fn cycle(error: authored::CurrencyError<std::convert::Infallible>) -> Error {
    match error {
        authored::CurrencyError::Cycle(revision) => Error::Cycle(revision),
        authored::CurrencyError::Source(never) => match never {},
    }
}

#[cfg(test)]
mod tests;
