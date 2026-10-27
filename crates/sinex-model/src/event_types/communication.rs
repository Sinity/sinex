//! Chat logs and email.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One line of a chat log (WeeChat IRC logs): a message, an action, or a
/// join, part, quit or other notice. Logs have no message IDs and only grow, so
/// the record location is the identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "chat.message",
    key = location,
    replace = repeat,
    time = sent_at,
    quality = intrinsic
)]
pub struct ChatMessage {
    /// The log's buffer, such as `irc.libera.#lesswrong`.
    pub buffer: String,
    pub sent_at: Timestamp,
    /// The sender's nick, without its mode prefix.
    pub nick: Option<String>,
    /// The mode prefix shown before the nick (`@`, `+`), if any.
    pub prefix: Option<String>,
    pub text: String,
    pub kind: ChatLineKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChatLineKind {
    Message,
    Action,
    Join,
    Part,
    Quit,
    Notice,
    /// Server and client lines without a sender.
    System,
}

/// One email message, from an mbox file or a Maildir. The `Message-ID` header
/// identifies it (a hash of the raw message when the header is missing), and a
/// later export with different labels is a newer reading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "mail.message",
    key = ["message_id"],
    replace = newer,
    time = date,
    quality = intrinsic
)]
pub struct MailMessage {
    pub message_id: String,
    /// The `Date` header; `None` when missing or unreadable.
    pub date: Option<Timestamp>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    /// Gmail's `X-Gmail-Labels`, split.
    pub labels: Vec<String>,
    /// Gmail's `X-GM-THRID`.
    pub thread_id: Option<String>,
    /// The first `text/plain` part, decoded.
    pub text: Option<String>,
    /// True when the message has HTML but no plain text part.
    pub html_only: bool,
    pub attachments: Vec<MailAttachment>,
    /// The raw message's size in bytes.
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MailAttachment {
    pub filename: Option<String>,
    pub content_type: String,
    pub size: u64,
}

/// A message in a direct or group conversation of a messaging service
/// (Facebook Messenger, SMS). Exports carry no thread or message ID that
/// survives between them, so a message is the thread (its participants), its
/// time and its sender; the same message read from another export or input is
/// the same occurrence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "conversation.message",
    key = ["service", "thread", "sent_at", "sender"],
    replace = newer,
    time = sent_at,
    quality = intrinsic,
    removed_when = unsent_at
)]
pub struct ConversationMessage {
    pub service: String,
    /// The thread's participants, sorted and joined by `|`.
    pub thread: String,
    /// The name the export gave the thread.
    pub thread_name: Option<String>,
    pub participants: Vec<String>,
    pub sender: String,
    pub sent_at: Timestamp,
    pub text: Option<String>,
    /// The service's message type (`text`, `media`, `call`).
    pub kind: Option<String>,
    /// Media and attachments, by the path the export wrote them under.
    pub media: Vec<String>,
    pub reactions: Vec<serde_json::Value>,
    /// The service's message ID, when the export has one.
    pub message_id: Option<String>,
    /// When the sender unsent it; the export gives no time, so the send time
    /// stands for it.
    pub unsent_at: Option<Timestamp>,
}
