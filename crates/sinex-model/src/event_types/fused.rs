//! Fused records: one occurrence that two or more sources recorded, as one
//! event, for the defined fusion families. Each is derived by a fusion rule
//! with every source record that saw it as a parent, takes its time from the
//! most preferred source that saw it and each other field from the most
//! preferred source that has it, and lists the sources in `observed_by`. A
//! record no other source saw stays a source record only; a consumer reads
//! the fused records plus the unmatched source records.
//!
//! Each is identified by its preferred member's record (`record`), never by
//! its content and time: the same command run twice in one second is two
//! runs.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One command run, from Atuin and zsh history. zsh history holds about a
/// thousand pairs of the same command in the same second on the operator's
/// machine, which is why the record, not the text and time, identifies it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "command.fused",
    key = ["record"],
    replace = newer,
    time = started_at,
    quality = intrinsic
)]
pub struct CommandFused {
    /// The preferred member's identity: `<type>:<native id>` when the source
    /// has one (Atuin), otherwise its record location.
    pub record: String,
    pub command: String,
    pub host: String,
    pub started_at: Timestamp,
    pub cwd: Option<String>,
    pub session: Option<String>,
    pub duration_ns: Option<i64>,
    pub exit_code: Option<i64>,
    /// The sources that observed this command.
    pub observed_by: Vec<String>,
}

/// One mail message stored by two or more mail sources of one account
/// (Gmail, IMAP, Maildir, mbox, Outlook): its account and `Message-ID`
/// identify it, and its `Date` is the same wherever it is stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 3, type = "mail.fused",
    key = ["record"],
    replace = newer,
    time = sent_at,
    quality = intrinsic
)]
pub struct MailFused {
    pub account: Option<String>,
    pub record: String,
    pub message_id: Option<String>,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub thread_id: Option<String>,
    pub labels: Vec<String>,
    /// The `Date` header.
    pub sent_at: Timestamp,
    pub observed_by: Vec<String>,
}
