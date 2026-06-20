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
