//! `WeeChat` IRC log file parser (#1051).
//!
//! Parses `WeeChat` IRC client log files in tab-delimited format:
//! `YYYY-MM-DD HH:MM:SS\tprefix\tmessage`
//!
//! ## Prefix mapping
//!
//! | Prefix  | Event type        | Meaning                                  |
//! |---------|-------------------|------------------------------------------|
//! | `-->`   | `irc.join`        | A user joined a channel                  |
//! | `<--`   | `irc.part`        | A user parted/left a channel             |
//! | `--`    | `irc.server_notice` | A server notice (MOTD, mode, etc.)     |
//! | nick    | `irc.message`     | A regular chat message from a user       |
//!
//! ## Format
//!
//! Each line is tab-delimited with three fields. The timestamp is in
//! local time (`WeeChat` default); the parser assumes UTC as a baseline
//! and records `TimingEvidence::Intrinsic` with the `"timestamp"` field.
//!
//! ## Occurrence identity
//!
//! The material anchor (`MaterialAnchor::Line`) is sufficient for
//! idempotency across replay — the same physical log line always
//! produces the same event.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;
use time::macros::format_description;

use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::temporal::Timestamp;

use super::{MaterialParser, ParserError, ParserResult};

// ---------------------------------------------------------------------------
// WeeChat timestamp format: `YYYY-MM-DD HH:MM:SS`
// ---------------------------------------------------------------------------

const TIMESTAMP_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

/// Parse a `WeeChat` timestamp string into a [`Timestamp`].
fn parse_weechat_ts(s: &str) -> ParserResult<Timestamp> {
    let dt = PrimitiveDateTime::parse(s, TIMESTAMP_FORMAT)
        .map_err(|e| ParserError::Parse(format!("invalid WeeChat timestamp '{s}': {e}")))?;
    Ok(Timestamp::new(dt.assume_utc()))
}

// ---------------------------------------------------------------------------
// Prefix classification
// ---------------------------------------------------------------------------

/// The result of classifying a `WeeChat` log prefix.
struct WeeChatClassification {
    /// The static event type string (e.g. `irc.message`).
    event_type: &'static str,
    /// The extracted nick (or `"__server__"` for server notices).
    nick: String,
    /// Optional channel extracted from join/part messages.
    channel: Option<String>,
}

/// Classify a `WeeChat` line prefix into an event type and extract nick/channel.
fn classify(prefix: &str, message: &str) -> WeeChatClassification {
    match prefix {
        "-->" => {
            let (nick, channel) = extract_nick_and_channel(message);
            WeeChatClassification {
                event_type: "irc.join",
                nick,
                channel,
            }
        }
        "<--" => {
            let (nick, channel) = extract_nick_and_channel(message);
            WeeChatClassification {
                event_type: "irc.part",
                nick,
                channel,
            }
        }
        "--" => WeeChatClassification {
            event_type: "irc.server_notice",
            nick: "__server__".into(),
            channel: None,
        },
        nick => WeeChatClassification {
            event_type: "irc.message",
            nick: nick.to_string(),
            channel: None,
        },
    }
}

/// Extract the nick (first word before space or `(`) and an optional
/// channel (word starting with `#`) from a join / part message.
fn extract_nick_and_channel(message: &str) -> (String, Option<String>) {
    let nick = message
        .split([' ', '('])
        .next()
        .unwrap_or("unknown")
        .to_string();

    let channel = message
        .split_whitespace()
        .find(|w| w.starts_with('#'))
        .map(String::from);

    (nick, channel)
}

// ---------------------------------------------------------------------------
// Parser configuration
// ---------------------------------------------------------------------------

/// Configuration for [`WeeChatLogParser`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WeeChatLogConfig;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parser for `WeeChat` IRC client log files.
///
/// Expects input from an [`AppendOnlyFileAdapter`](super::AppendOnlyFileAdapter)
/// that yields one [`SourceRecord`](sinex_primitives::parser::SourceRecord) per
/// line. Each record is parsed into a typed IRC event intent.
#[derive(Debug, Clone, Default)]
pub struct WeeChatLogParser;

#[async_trait]
impl MaterialParser for WeeChatLogParser {
    type Config = WeeChatLogConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("weechat-log"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("weechat"),
            declared_event_types: vec![
                (
                    EventSource::from_static("irc"),
                    EventType::from_static("irc.join"),
                ),
                (
                    EventSource::from_static("irc"),
                    EventType::from_static("irc.part"),
                ),
                (
                    EventSource::from_static("irc"),
                    EventType::from_static("irc.server_notice"),
                ),
                (
                    EventSource::from_static("irc"),
                    EventType::from_static("irc.message"),
                ),
            ],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            description: "Parses WeeChat IRC client log files into typed IRC events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let line = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid UTF-8 in WeeChat log: {e}")))?;

        let line = line.trim();
        if line.is_empty() {
            return Ok(vec![]);
        }

        // ---- split into timestamp / prefix / message ----
        let mut parts = line.splitn(3, '\t');
        let ts_str = parts
            .next()
            .ok_or_else(|| ParserError::Parse("missing timestamp field".into()))?;
        let prefix = parts
            .next()
            .ok_or_else(|| ParserError::Parse("missing prefix field".into()))?;
        let message = parts.next().unwrap_or("");

        // ---- parse fields ----
        let ts_orig = parse_weechat_ts(ts_str)?;
        let cls = classify(prefix, message);

        // ---- build payload ----
        let mut payload = serde_json::Map::new();
        payload.insert("nick".into(), serde_json::json!(cls.nick));
        payload.insert("message".into(), serde_json::json!(message));
        if let Some(ref ch) = cls.channel {
            payload.insert("channel".into(), serde_json::json!(ch));
        }

        let anchor = record.anchor.clone();

        let intent = ParsedEventIntent::builder()
            .source_id(ctx.source_id.clone())
            .parser_id(ParserId::from_static("weechat-log"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static(cls.event_type))
            .event_source(EventSource::from_static("irc"))
            .payload(serde_json::Value::Object(payload))
            .ts_orig(ts_orig)
            .timing(TimingEvidence::Intrinsic {
                field: "timestamp".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(anchor)
            .privacy_context(ProcessingContext::Command)
            .build();

        Ok(vec![intent])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "weechat_test.rs"]
mod tests;
