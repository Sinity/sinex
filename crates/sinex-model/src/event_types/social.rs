//! What the operator published and voted on in online communities (Wykop,
//! Reddit, forums).
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A post, comment or link the operator published.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "post.published",
    key = ["service", "kind", "native_id"],
    replace = newer,
    time = created_at,
    quality = intrinsic
)]
pub struct PostPublished {
    pub service: String,
    /// `entry`, `entry_comment`, `link`, `link_comment`, `post`, `comment`.
    pub kind: String,
    pub native_id: String,
    pub created_at: Option<Timestamp>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub url: Option<String>,
    /// What it replies to or belongs to (the entry or link a comment is on).
    pub parent: Option<String>,
    /// The community or tags it was filed under.
    pub community: Option<String>,
    pub tags: Vec<String>,
    pub score: Option<i64>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A vote the operator cast on someone's post. Exports list votes without the
/// time they were cast, so the event time is unknown; the target's own time is
/// a field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "vote.cast",
    key = ["service", "target_kind", "target_id"],
    replace = newer,
    time = cast_at,
    quality = intrinsic,
    removed_when = withdrawn_at
)]
pub struct VoteCast {
    pub service: String,
    pub target_kind: String,
    pub target_id: String,
    /// `up` or `down`.
    pub direction: String,
    pub cast_at: Option<Timestamp>,
    pub target_created_at: Option<Timestamp>,
    pub target_title: Option<String>,
    pub target_url: Option<String>,
    pub target_author: Option<String>,
    /// Set when the export records the vote as withdrawn (Reddit's `none`).
    pub withdrawn_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// A notification an online community sent the operator (Wykop: a new entry
/// or link under a tag the operator observes). The service's notification ID
/// is the identity; a later reading that marks it read is newer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "notification.received",
    key = ["service", "native_id"],
    replace = newer,
    time = created_at,
    quality = intrinsic
)]
pub struct NotificationReceived {
    pub service: String,
    pub native_id: String,
    /// The service's notification type (`new_entry_with_observed_tag`).
    pub kind: String,
    pub created_at: Option<Timestamp>,
    pub read: Option<bool>,
    /// What it points at, as `entry:<id>` or `link:<id>`.
    pub target: Option<String>,
    pub target_url: Option<String>,
    /// Who caused it.
    pub actor: Option<String>,
    /// The tag or channel it came through.
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One entry of an account setting the operator keeps at a service, such as a
/// blocked user, tag or domain. The setting and its value are the identity;
/// exports give no time the operator set it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "account.setting",
    key = ["service", "setting", "value"],
    replace = newer
)]
pub struct AccountSetting {
    pub service: String,
    /// `blacklist/users`, `blacklist/tags`, `blacklist/domains`.
    pub setting: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}
