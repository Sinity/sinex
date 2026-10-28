//! Chat logs and email.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One line of a chat log (WeeChat IRC logs): a message, an action, or a
/// join, part, quit or other notice. Logs have no message IDs and only grow, so
/// the record location is the identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "chat.message",
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

/// One email message. Its Message-ID, provider coordinates, or record
/// location identifies it; equal bytes alone never identify a message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MailMessage {
    /// Mailbox account, when the input identifies it.
    pub account: Option<String>,
    pub message_id: Option<String>,
    /// Provider ID, scoped to its account or mailbox and input.
    pub native_id: Option<String>,
    /// The `Date` header; `None` when missing or unreadable.
    pub date: Option<Timestamp>,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    /// Blind-copy recipients, when kept in the source headers.
    pub bcc: Vec<String>,
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

impl crate::EventPayload for MailMessage {
    const VERSION: u64 = 3;
    const EVENT_TYPE: &'static str = "mail.message";
    const KEY: crate::KeyKind = crate::KeyKind::Fields(&["account", "message_id"]);
    const REPLACE: crate::ReplaceRule = crate::ReplaceRule::ReplaceWhenNewer;

    fn key_kind(&self) -> crate::KeyKind {
        if self
            .account
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .native_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
        {
            return crate::KeyKind::Fields(&["account", "native_id"]);
        }
        if self
            .message_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
        {
            Self::KEY
        } else if self
            .native_id
            .as_deref()
            .is_some_and(|id| !id.trim().is_empty())
        {
            crate::KeyKind::Fields(&["native_id"])
        } else {
            crate::KeyKind::Location
        }
    }

    fn ts_orig(&self) -> crate::TsOrig {
        crate::TsOrig::maybe(self.date, crate::TimeQuality::Intrinsic)
    }

    fn declaration() -> crate::KindDeclaration {
        crate::KindDeclaration {
            kind: crate::KindRef {
                name: Self::EVENT_TYPE.into(),
                version: Self::VERSION,
            },
            declaring_producer: "sinex".into(),
            payload_schema: Some(Self::payload_schema()),
            normalization: crate::kinds::Normalization::Rust,
            key_rule: Some({
                use crate::kinds::KeyRule;
                let fallback = KeyRule::WhenNonempty {
                    field: "message_id".into(),
                    trim: true,
                    then: Box::new(KeyRule::Fields {
                        fields: vec!["account".into(), "message_id".into()],
                    }),
                    otherwise: Box::new(KeyRule::WhenNonempty {
                        field: "native_id".into(),
                        trim: true,
                        then: Box::new(KeyRule::Fields {
                            fields: vec!["native_id".into()],
                        }),
                        otherwise: Box::new(KeyRule::Location),
                    }),
                };
                KeyRule::WhenNonempty {
                    field: "account".into(),
                    trim: true,
                    then: Box::new(KeyRule::WhenNonempty {
                        field: "native_id".into(),
                        trim: true,
                        then: Box::new(KeyRule::Fields {
                            fields: vec!["account".into(), "native_id".into()],
                        }),
                        otherwise: Box::new(fallback.clone()),
                    }),
                    otherwise: Box::new(fallback),
                }
            }),
            replace_rule: Some(Self::REPLACE),
            time_rule: Some(crate::kinds::TimeRule::FirstPresent {
                fields: vec!["date".into()],
                quality: crate::TimeQuality::Intrinsic,
            }),
            removed_rule: Some(crate::kinds::RemovedRule::Never),
            keep_known: vec![],
            owned_blobs: vec![],
            search: Self::SEARCH,
            field_hints: vec![],
        }
    }

    fn payload_schema() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(Self)).expect("schemas serialize")
    }
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
/// given the same identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "conversation.message",
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

/// A phone's call log entry: a call, or a message the phone's log lists
/// beside calls. The account's own key for the entry identifies it across
/// exports.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2,
    type = "call.logged",
    key = ["account_key"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct CallLogged {
    /// The key the account export gave the entry.
    pub account_key: String,
    pub at: Option<Timestamp>,
    /// The other party's number as logged; empty when withheld.
    pub number: Option<String>,
    /// The contact or caller-ID name the phone showed.
    pub name: Option<String>,
    /// `incoming`, `outgoing`, `missed`, `voicemail`, `rejected`, `blocked`
    /// or `answered_elsewhere`, from Android's call type; `None` for a code
    /// Android does not define.
    pub direction: Option<String>,
    /// Android's call type code as logged.
    pub call_type: Option<i64>,
    pub duration_seconds: Option<i64>,
    /// The phone maker's code for what kind of entry it is (a voice call, a
    /// message, a call through an app), as logged and not interpreted.
    pub log_type: Option<i64>,
    /// A message entry's text and subject.
    pub text: Option<String>,
    pub subject: Option<String>,
    /// The entry's other fields, by the name the phone gave them.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}
