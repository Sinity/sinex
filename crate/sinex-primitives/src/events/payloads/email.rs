//! Email mailbox event payloads.

use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

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
    pub mailbox_format: String,
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
    pub mailbox_format: String,
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
    pub mailbox_format: String,
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
    pub mailbox_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.sync_cursor.observed")]
pub struct EmailSyncCursorObservedPayload {
    pub provider: String,
    pub account_binding_ref: String,
    pub mailbox_scope: Option<String>,
    pub cursor_kind: String,
    pub cursor_value: Option<String>,
    pub uidvalidity: Option<String>,
    pub uid: Option<String>,
    pub gmail_history_id: Option<String>,
    pub page_token: Option<String>,
    pub observed_at: Timestamp,
    pub continuity_state: String,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "email", event_type = "email.capture_runtime.observed")]
pub struct EmailCaptureRuntimeObservedPayload {
    pub provider: String,
    pub account_binding_ref: String,
    pub mode_id: String,
    pub observed_at: Timestamp,
    pub auth_state: String,
    pub network_state: String,
    pub rate_limit_state: Option<String>,
    pub sync_state: String,
    pub pending_messages: Option<u32>,
    pub pending_material_bytes: Option<u64>,
    pub caveats: Vec<String>,
    pub actions: Vec<String>,
}
