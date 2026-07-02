//! IRC event payloads.
//!
//! These payloads mirror the normalized shape emitted by the `WeeChat` log
//! parser: every line carries a nick/sentinel and message text, while join and
//! part events may additionally expose a channel.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "irc", event_type = "irc.join")]
pub struct IrcJoinPayload {
    pub nick: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "irc", event_type = "irc.part")]
pub struct IrcPartPayload {
    pub nick: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "irc", event_type = "irc.server_notice")]
pub struct IrcServerNoticePayload {
    pub nick: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "irc", event_type = "irc.message")]
pub struct IrcMessagePayload {
    pub nick: String,
    pub message: String,
}

#[cfg(test)]
#[path = "irc_test.rs"]
mod tests;
