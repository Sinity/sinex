//! Fusion records for the defined families: fused records and the pairs a
//! fusion pass had to choose about.
//!
//! A fused record is one occurrence that two or more sources recorded. It is
//! derived with every member's reading as a parent, cites each member by its
//! stable record citation (never a reading ID, so the association survives a
//! replay of either side), takes its time from the most preferred member and
//! each other field from the most preferred member that has it, and records
//! how every member joined. A record no other source saw stays a source
//! record only; a consumer reads the fused records plus the unmatched source
//! records.
//!
//! Each is identified by its preferred member's record citation (`record`),
//! never by its content and time: the same command run twice in one second is
//! two runs.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What decided a fused record: an exact shared identifier, a matching rule
/// (time window, unique fallback, absorption), or a duplicate decision.
/// A record with several kinds of links records the weakest automatic one
/// (`rule` before `identifier`), and `operator` when any duplicate decision decided it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    Identifier,
    Rule,
    Operator,
}

/// How one member joined its fused record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "by", rename_all = "snake_case")]
pub enum Joined {
    /// The most preferred member, which the others joined.
    Lead,
    /// It shares an exact identifier in this namespace with a member.
    Identifier { namespace: String },
    /// The closest partner within the family's window.
    Window { distance_ns: i64 },
    /// The only partner within the family's wider fallback window.
    Fallback { distance_ns: i64 },
    /// A repeat inside a span led by a more preferred member.
    Absorb { distance_ns: i64 },
    /// A `same` duplicate decision by this author.
    Decision { author: String },
}

/// One member of a fused record, cited by its record, not its reading.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Member {
    /// The member's stable record citation: `<type>:<native id>` when the
    /// source has one, otherwise its record location.
    pub record: String,
    pub source: String,
    pub event_type: String,
    pub joined: Joined,
}

/// One command run, from the shell integration's reports, Atuin and zsh
/// history. zsh history holds many pairs of the same command in the same
/// second, which is why the record, not the text and time, identifies it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3, type = "command.fused",
    key = ["record"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct CommandFused {
    /// The preferred member's record citation.
    pub record: String,
    pub command: String,
    pub host: String,
    pub started_at: Timestamp,
    pub cwd: Option<String>,
    pub session: Option<String>,
    pub duration_ns: Option<i64>,
    pub exit_code: Option<i64>,
    /// The execution token the shell integration minted for this run.
    pub execution: Option<String>,
    pub members: Vec<Member>,
    pub basis: Basis,
    /// How many candidates the weakest choice was made among; 1 when there
    /// was no choice.
    pub ambiguous: u32,
    /// The record citations of the candidates not chosen.
    pub competitors: Vec<String>,
    /// The fusion family's version, which covers its declaration.
    pub rule_version: u32,
}

/// One mail message stored by two or more mail sources of one account
/// (Gmail, IMAP, Maildir, mbox, Outlook): its account and `Message-ID`
/// identify it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 4, type = "mail.fused",
    key = ["record"],
    replace = newer,
    time = sent_at,
    quality = intrinsic
)]
pub struct MailFused {
    pub account: String,
    pub record: String,
    pub message_id: Option<String>,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub thread_id: Option<String>,
    pub labels: Vec<String>,
    /// The preferred member's `Date` header.
    pub sent_at: Timestamp,
    pub members: Vec<Member>,
    pub basis: Basis,
    pub ambiguous: u32,
    pub competitors: Vec<String>,
    pub rule_version: u32,
}

/// Which strategy proposed a considered pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    Identifier,
    Window,
    Fallback,
    /// No strategy proposed it; a duplicate decision names the pair.
    Decision,
}

/// The decision on a pair: none yet, the same occurrence, or two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PairState {
    Undecided,
    Same,
    Distinct,
}

/// A pair of records a fusion pass had to choose about: one of several
/// candidates for the same partner, or a pair a duplicate decision names.
/// Written by the fusion pass with its fused records and queried as
/// `duplicates`. The pair is cited by records, never readings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 1, type = "fusion.considered_pair",
    key = ["family", "left", "right"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct ConsideredPair {
    pub family: String,
    /// The lesser of the two record citations.
    pub left: String,
    pub right: String,
    pub left_source: String,
    pub right_source: String,
    /// The earlier of the two starts.
    pub at: Timestamp,
    pub strategy: Strategy,
    /// How many candidates the choice was among.
    pub candidates: u32,
    pub distance_ns: i64,
    /// Whether this pair is joined in a fused record.
    pub chosen: bool,
    /// The governing decision.
    pub state: PairState,
    /// Who made the governing decision.
    pub decided_by: Option<String>,
    /// Standing decisions that do not govern (an agent's, where the family
    /// records agent decisions without applying them).
    pub other_decisions: Vec<StandingDecision>,
}

/// One author's standing decision on a pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StandingDecision {
    pub author: String,
    pub decision: crate::event_types::duplicate::Decision,
}
