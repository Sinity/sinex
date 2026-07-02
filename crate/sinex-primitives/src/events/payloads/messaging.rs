//! Messaging-domain event payloads.
//!
//! Currently hosts the Facebook Messenger GDPR-export payload.
//! Adding other messaging-provider exports (Signal, Telegram, IRC private
//! messages) belongs in this module rather than spawning a new domain per
//! provider.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One message observation from a Facebook Messenger GDPR export thread file.
///
/// Each thread file is a JSON object with `participants`, `threadName`, and a
/// `messages[]` array. We emit one event per message. The thread name is
/// preserved so threads can be reconstructed downstream; sender + thread
/// together name the conversation participants.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "messenger", event_type = "message.sent")]
pub struct MessengerMessageSentPayload {
    /// Wall-clock time the message was sent (Messenger uses epoch milliseconds).
    pub sent_at: Timestamp,

    /// Thread name as Facebook recorded it (often "<other-participant>_<N>").
    pub thread_name: String,

    /// Sender display name (Facebook profile name at export time).
    pub sender_name: String,

    /// All participants in the thread, including the sender. Useful for
    /// reconstructing group chats vs 1:1 threads downstream.
    pub participants: Vec<String>,

    /// Message type: `text`, `share`, `subscribe`, `call`, etc. Verbatim
    /// from the export's `type` field.
    pub message_type: String,

    /// Message text body. None when the message has no text component
    /// (e.g., pure media share or system message).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,

    /// Whether the message was unsent (recalled by the sender).
    pub is_unsent: bool,

    /// Count of media attachments. The export's `media` array is dropped
    /// (it points to per-export blob paths that are not stable across
    /// snapshots); only the count is preserved so message presence is not
    /// erased.
    pub media_count: u32,

    /// Count of reactions attached to the message. Reaction details
    /// (who-reacted-with-what) are dropped to keep payload bounded;
    /// the count surfaces engagement signal.
    pub reaction_count: u32,
}

#[cfg(test)]
#[path = "messaging_test.rs"]
mod tests;
