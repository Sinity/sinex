//! Duplicate decisions: `same` or `distinct` over an unordered pair of
//! records, written by a person or an agent. One kind for every pair Sinex
//! asks about: a fusion family's records and the library's items.
//!
//! A duplicate decision is an authored record and follows the shared
//! authored-record rules (`authored` in polylogue-sinex-common): it carries
//! the header (root, revision, author, channel, time, the revisions it
//! revises and the records it withdraws), its currency is the descendant
//! rule, and its uniqueness key is the unordered pair and the author, so an
//! author's later decision on a pair revises the earlier one. A withdrawal is
//! a record of this kind with no decision that withdraws the author's
//! record. Nothing is replaced by write order or time.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The decision on a pair.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Same,
    Distinct,
}

/// One revision of a duplicate decision, or a withdrawal of one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(version = 1, type = "curation.duplicate",
    key = ["rev"],
    replace = repeat,
    time = pair_at,
    quality = intrinsic
)]
pub struct Duplicate {
    /// The ID of the record's first revision.
    pub root: String,
    /// This revision's ID; equals `root` for the first revision.
    pub rev: String,
    /// Computed from the authenticated caller, never supplied.
    pub author: String,
    /// The channel the request arrived on. Every decision reaches the
    /// daemon through its API, so this is `api`.
    pub via: String,
    /// The writer's clock in Unix milliseconds, for display; currency never
    /// reads it.
    pub at: i64,
    /// The revisions this one revises, as `<root>@<rev>`.
    pub revises: Vec<String>,
    /// The revisions (`<root>@<rev>`) or records (`<root>`) this one
    /// withdraws.
    pub withdraws: Vec<String>,
    /// The two record citations, sorted.
    pub pair: Vec<String>,
    /// `None` for a withdrawal.
    pub decision: Option<Decision>,
    /// The earlier of the pair's own times, when they have times, so the
    /// derivation that reads the pair also reads the decision.
    pub pair_at: Option<Timestamp>,
    /// The fusion family whose records the pair are, when it is one.
    pub family: Option<String>,
    /// For a fusion family keyed by identity, the key that holds the pair.
    pub family_key: Option<Value>,
}

impl Duplicate {
    /// The uniqueness key: one current decision per unordered pair and author.
    pub const UNIQUENESS: authored::Uniqueness =
        authored::Uniqueness::new(&["pair", "author"], authored::WhenHeld::Revise);

    /// The record's authored header, for currency.
    pub fn header(&self) -> Result<authored::Header, authored::IdentityError> {
        Ok(authored::Header {
            root: self.root.clone(),
            rev: self.rev.clone(),
            kind: Self::EVENT_TYPE_NAME.to_owned(),
            key: None,
            value: self
                .decision
                .map(|d| serde_json::to_value(d).expect("decisions serialize")),
            author: self.author.clone(),
            via: self.via.clone(),
            at: self.at,
            revises: self
                .revises
                .iter()
                .map(|r| authored::Revision::parse(r))
                .collect::<Result<_, _>>()?,
            withdraws: self
                .withdraws
                .iter()
                .map(|t| authored::Target::parse(t))
                .collect::<Result<_, _>>()?,
        })
    }

    /// The uniqueness slots this record occupies.
    pub fn slots(&self) -> Vec<authored::UniqueSlot> {
        Self::UNIQUENESS
            .unique(Self::EVENT_TYPE_NAME, |component| match component {
                "pair" => authored::Part::Set(self.pair.clone()),
                _ => authored::Part::One(self.author.clone()),
            })
            .slots
    }

    const EVENT_TYPE_NAME: &'static str = "curation.duplicate";
}

#[cfg(test)]
mod tests;
