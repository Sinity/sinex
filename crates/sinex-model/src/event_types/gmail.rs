//! Gmail message observations and provider history. Provider IDs are opaque.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "mail.provider_message", key = ["account", "provider_message_id", "history_id"], replace = repeat)]
pub struct ProviderMessage {
    pub account: String,
    pub provider_message_id: String,
    pub thread_id: String,
    pub history_id: String,
    pub labels: Vec<String>,
    pub internal_date: Option<Timestamp>,
    pub message_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageReference {
    pub id: String,
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageChange {
    pub message: MessageReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "mail.changed", key = ["account", "history_id"], replace = repeat)]
pub struct MailChanged {
    pub account: String,
    pub history_id: String,
    pub messages: Vec<MessageReference>,
    pub added: Vec<MessageChange>,
    pub deleted: Vec<MessageChange>,
    pub labels_added: Vec<MessageChange>,
    pub labels_removed: Vec<MessageChange>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "mail.history_gap", key = ["account", "requested_history_id"], replace = repeat)]
pub struct HistoryGap {
    pub account: String,
    pub requested_history_id: String,
    pub status: u16,
    pub reason: String,
    pub observed_at: Timestamp,
}

/// The provider could list a message but no longer supplies its bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "mail.provider_missing", key = ["account", "provider_message_id", "requested_history_id"], replace = repeat)]
pub struct ProviderMissing {
    pub account: String,
    pub provider_message_id: String,
    pub requested_history_id: String,
    pub observed_at: Timestamp,
}

#[cfg(test)]
mod tests;
