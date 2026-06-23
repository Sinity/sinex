//! Email mailbox event payloads.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

/// Staged mailbox acquisition shape for email message/attachment/thread events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailMailboxFormat {
    Rfc822DropStaged,
    MaildirStaged,
    MboxStaged,
    GmailApi,
    ImapProvider,
}

impl EmailMailboxFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rfc822DropStaged => "rfc822-drop-staged",
            Self::MaildirStaged => "maildir-staged",
            Self::MboxStaged => "mbox-staged",
            Self::GmailApi => "gmail-api",
            Self::ImapProvider => "imap-provider",
        }
    }
}

/// Provider family behind an email live/scheduled acquisition mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailProviderKind {
    Gmail,
    Imap,
}

impl EmailProviderKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gmail => "gmail",
            Self::Imap => "imap",
        }
    }
}

/// Runtime shape for a provider-backed email mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailProviderRuntime {
    ScheduledSync,
    IdleLive,
}

impl EmailProviderRuntime {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScheduledSync => "scheduled-sync",
            Self::IdleLive => "idle-live",
        }
    }
}

/// Provider cursor coordinate persisted by email sync observations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailSyncCursorKind {
    GmailHistoryId,
    GmailPageToken,
    ImapUidvalidityUid,
    ImapModseq,
}

impl EmailSyncCursorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GmailHistoryId => "gmail-history-id",
            Self::GmailPageToken => "gmail-page-token",
            Self::ImapUidvalidityUid => "imap-uidvalidity-uid",
            Self::ImapModseq => "imap-modseq",
        }
    }
}

/// Whether a provider cursor is currently replay-continuous.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailContinuityState {
    Current,
    Gap,
    Degraded,
    Unknown,
}

/// Provider action emitted when a cursor discontinuity requires mailbox resync.
pub const EMAIL_REQUIRED_ACTION_RESYNC_MAILBOX: &str = "resync_mailbox";

/// Authorization state visible to provider coverage/debt surfaces.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailAuthorizationState {
    Authorized,
    Missing,
    Expired,
    Rejected,
    Unknown,
}

/// Provider transport state visible to provider runtime observations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailNetworkState {
    Online,
    Offline,
    RateLimited,
    Error,
    Unknown,
}

/// Provider quota/backoff state for scheduled and live sync modes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailRateLimitState {
    Clear,
    Throttled,
    Backoff,
    Exhausted,
}

/// Provider worker state for scheduled and live sync modes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EmailSyncState {
    Idle,
    Syncing,
    Backfilling,
    Paused,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.message.received")]
pub struct EmailMessageReceivedPayload {
    pub message_id: Option<String>,
    pub date: Option<Timestamp>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub list_id: Option<String>,
    pub folder: Option<String>,
    pub source_file: String,
    pub raw_material_id: String,
    pub mailbox_format: EmailMailboxFormat,
    pub maildir_subdir: Option<String>,
    pub maildir_flags: Vec<String>,
    pub maildir_stable_filename: Option<String>,
    pub mbox_file: Option<String>,
    pub mbox_byte_start: Option<u64>,
    pub mbox_byte_end: Option<u64>,
    pub size_bytes: u64,
    pub body_bytes: u64,
    pub attachment_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.message.sent")]
pub struct EmailMessageSentPayload {
    pub message_id: Option<String>,
    pub date: Option<Timestamp>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub list_id: Option<String>,
    pub folder: Option<String>,
    pub source_file: String,
    pub raw_material_id: String,
    pub mailbox_format: EmailMailboxFormat,
    pub maildir_subdir: Option<String>,
    pub maildir_flags: Vec<String>,
    pub maildir_stable_filename: Option<String>,
    pub mbox_file: Option<String>,
    pub mbox_byte_start: Option<u64>,
    pub mbox_byte_end: Option<u64>,
    pub size_bytes: u64,
    pub body_bytes: u64,
    pub attachment_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.attachment.observed")]
pub struct EmailAttachmentObservedPayload {
    pub message_id: Option<String>,
    pub folder: Option<String>,
    pub source_file: String,
    pub raw_material_id: String,
    pub mailbox_format: EmailMailboxFormat,
    pub attachment_index: u32,
    pub disposition: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub content_id: Option<String>,
    pub material_policy_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.thread.observed")]
pub struct EmailThreadObservedPayload {
    pub thread_key: String,
    pub thread_root_message_id: Option<String>,
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub date: Option<Timestamp>,
    pub subject: Option<String>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub folder: Option<String>,
    pub source_file: String,
    pub raw_material_id: String,
    pub mailbox_format: EmailMailboxFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.sync_cursor.observed")]
pub struct EmailSyncCursorObservedPayload {
    pub provider: EmailProviderKind,
    pub account_binding_ref: String,
    pub mailbox_scope: Option<String>,
    pub cursor_kind: EmailSyncCursorKind,
    pub cursor_value: Option<String>,
    pub uidvalidity: Option<String>,
    pub uid: Option<String>,
    pub gmail_history_id: Option<String>,
    pub page_token: Option<String>,
    pub observed_at: Timestamp,
    pub continuity_state: EmailContinuityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_action: Option<String>,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.capture_runtime.observed")]
pub struct EmailCaptureRuntimeObservedPayload {
    pub provider: EmailProviderKind,
    pub account_binding_ref: String,
    pub mode_id: String,
    pub observed_at: Timestamp,
    pub provider_runtime: EmailProviderRuntime,
    pub auth_state: EmailAuthorizationState,
    pub network_state: EmailNetworkState,
    pub rate_limit_state: Option<EmailRateLimitState>,
    pub sync_state: EmailSyncState,
    pub pending_messages: Option<u32>,
    pub pending_material_bytes: Option<u64>,
    pub caveats: Vec<String>,
    pub actions: Vec<String>,
}
