//! AI session message payloads.
//!
//! Hosts both Claude and ChatGPT provider exports under one domain module.
//! Future providers (Gemini, Codex, etc.) will also land here rather than
//! spawning separate modules.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One message in a Claude conversation export (`conversations.json`).
///
/// Source: Claude GDPR export → `chat_messages[].sender` is `"human"` or
/// `"assistant"`. The `text` field carries the rendered message body.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "claude", event_type = "ai.message")]
pub struct ClaudeAiMessagePayload {
    /// UUID of the conversation this message belongs to.
    pub session_id: String,
    /// UUID of the message itself.
    pub message_id: String,
    /// `"human"` or `"assistant"`.
    pub role: String,
    /// Rendered message text (may be empty for tool-use or attachment-only turns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// ISO 8601 timestamp of the message as recorded in the export.
    pub message_ts: Timestamp,
    /// Human-readable conversation title (if set by the user).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_name: Option<String>,
}

/// One message in a ChatGPT conversation export (`conversations-NNN.json`).
///
/// Source: ChatGPT data export → `mapping` node graph, walking from
/// `current_node` backwards to reconstruct the canonical thread. Only text
/// messages (`content_type = "text"`) are included; tool-use / DALL-E / etc.
/// nodes are skipped.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "chatgpt", event_type = "ai.message")]
pub struct ChatGptAiMessagePayload {
    /// ChatGPT conversation id.
    pub session_id: String,
    /// Node id of the message.
    pub message_id: String,
    /// `"user"`, `"assistant"`, `"system"`, or `"tool"`.
    pub role: String,
    /// Rendered message text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Unix-epoch timestamp (as stored in the export).
    pub message_ts: Timestamp,
    /// Conversation title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_title: Option<String>,
    /// Model slug when it appears on the message metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload as _;

    #[test]
    fn claude_declares_source_and_event_type() {
        assert_eq!(ClaudeAiMessagePayload::SOURCE.as_static_str(), "claude");
        assert_eq!(
            ClaudeAiMessagePayload::EVENT_TYPE.as_static_str(),
            "ai.message"
        );
    }

    #[test]
    fn chatgpt_declares_source_and_event_type() {
        assert_eq!(ChatGptAiMessagePayload::SOURCE.as_static_str(), "chatgpt");
        assert_eq!(
            ChatGptAiMessagePayload::EVENT_TYPE.as_static_str(),
            "ai.message"
        );
    }
}
