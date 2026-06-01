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
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceUnitId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::{self, ProcessingContext};
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
            source_unit_id: SourceUnitId::from_static("weechat"),
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
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "event_type_from_prefix".into(),
                "anchor_line".into(),
                "nick_extraction".into(),
            ],
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

        let payload =
            privacy::process_json(&serde_json::Value::Object(payload), ProcessingContext::Command)
                .map_err(|e| ParserError::Privacy(format!("privacy engine: {e}")))?;

        let anchor = record.anchor.clone();

        let intent = ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("weechat-log"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static(cls.event_type))
            .event_source(EventSource::from_static("irc"))
            .payload(payload)
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
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;

    use sinex_primitives::Uuid;
    use sinex_primitives::parser::MaterialAnchor;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("weechat"),
            source_material_id: sinex_primitives::ids::Id::new(),
            record_anchor: MaterialAnchor::Line {
                byte_start: 0,
                line: 1,
            },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn make_record(
        bytes: &[u8],
        line: u64,
        byte_start: u64,
    ) -> sinex_primitives::parser::SourceRecord {
        sinex_primitives::parser::SourceRecord {
            material_id: sinex_primitives::ids::Id::new(),
            anchor: MaterialAnchor::Line { byte_start, line },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    #[sinex_test]
    async fn parse_irc_message() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(b"2024-01-15 14:23:45\tsinity\thello world", 1, 0);
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        let intent = &intents[0];

        assert_eq!(intent.event_type.as_str(), "irc.message");
        assert_eq!(intent.event_source.as_str(), "irc");
        assert_eq!(intent.payload["nick"], "sinity");
        assert_eq!(intent.payload["message"], "hello world");

        // Verify timestamp
        let ts = intent.ts_orig.inner();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month(), time::Month::January);
        assert_eq!(ts.day(), 15);
        assert_eq!(ts.hour(), 14);
        assert_eq!(ts.minute(), 23);
        assert_eq!(ts.second(), 45);
        Ok(())
    }

    #[sinex_test]
    async fn parse_irc_message_redacts_secret_payload_strings() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(
            b"2024-01-15 14:23:45\tsinity\tTOKEN=ghp_1234567890abcdefghijklmnopqrstuvwxyz",
            1,
            0,
        );
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(
            intents[0].payload["message"],
            "TOKEN=<GITHUB_TOKEN>",
            "WeeChat imperative parser must invoke Command-context privacy"
        );
        Ok(())
    }

    #[sinex_test]
    async fn parse_irc_join() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(
            b"2024-06-01 10:00:00\t-->\tuser (~user@host) joined #general",
            2,
            50,
        );
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        let intent = &intents[0];

        assert_eq!(intent.event_type.as_str(), "irc.join");
        assert_eq!(intent.payload["nick"], "user");
        assert_eq!(intent.payload["channel"], "#general");
        Ok(())
    }

    #[sinex_test]
    async fn parse_irc_part() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(
            b"2024-06-01 12:30:00\t<--\tuser (~user@host) left #general",
            3,
            100,
        );
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        let intent = &intents[0];

        assert_eq!(intent.event_type.as_str(), "irc.part");
        assert_eq!(intent.payload["nick"], "user");
        assert_eq!(intent.payload["channel"], "#general");
        Ok(())
    }

    #[sinex_test]
    async fn parse_server_notice() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(
            b"2024-06-01 09:00:00\t--\tNotice: Server restart scheduled",
            4,
            150,
        );
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        let intent = &intents[0];

        assert_eq!(intent.event_type.as_str(), "irc.server_notice");
        assert_eq!(intent.payload["nick"], "__server__");
        assert_eq!(
            intent.payload["message"],
            "Notice: Server restart scheduled"
        );
        Ok(())
    }

    #[sinex_test]
    async fn skip_empty_lines() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(b"", 1, 0);
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert!(intents.is_empty(), "empty lines should produce no intents");
        Ok(())
    }

    #[sinex_test]
    async fn invalid_timestamp_is_error() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(b"not-a-timestamp\tnick\tmessage", 1, 0);
        let ctx = test_ctx();

        let result = parser.parse_record(record, &ctx).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid WeeChat timestamp"), "got: {err}");
        Ok(())
    }

    #[sinex_test]
    async fn parse_anchor_preserved() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let anchor = MaterialAnchor::Line {
            byte_start: 999,
            line: 42,
        };
        let record = make_record(b"2024-01-01 00:00:00\tnick\tmsg", 42, 999);
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].anchor, anchor);
        Ok(())
    }

    #[sinex_test]
    async fn parse_timing_evidence_is_intrinsic() -> xtask::sandbox::TestResult<()> {
        let mut parser = WeeChatLogParser;
        let record = make_record(b"2024-01-01 00:00:00\tnick\tmsg", 1, 0);
        let ctx = test_ctx();

        let intents = parser.parse_record(record, &ctx).await.unwrap();
        assert_eq!(intents.len(), 1);
        assert!(
            matches!(
                intents[0].timing,
                TimingEvidence::Intrinsic { ref field, confidence: TimingConfidence::Intrinsic } if field == "timestamp"
            ),
            "expected Intrinsic timing with field='timestamp', got {:?}",
            intents[0].timing
        );
        Ok(())
    }
}
