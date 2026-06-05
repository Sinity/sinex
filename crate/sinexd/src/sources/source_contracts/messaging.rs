//! Facebook Messenger GDPR export parser (#1090).
//!
//! Reads one JSON file per conversation thread (Facebook GDPR export shape:
//! `{participants: [...], threadName: "...", messages: [...]}`) and emits
//! one `messenger`/`message.sent` event per message.
//!
//! Each export is per-thread, so the [`StaticFileAdapter`] stages one file
//! at a time. The parser unpacks the messages array and emits N intents.
//!
//! ## Privacy
//!
//! GDPR exports include real chat content with named participants. We
//! mark the source `PrivacyTier::Sensitive` and emit events with
//! `ProcessingContext::Document` so admission policy can strip the
//! `text` field when needed. `participants` and `sender_name` are
//! intentionally preserved — they're the social-graph signal, not the
//! conversation content.
//!
//! `media[]` and `reactions[]` entries are dropped from the payload
//! (only their counts are kept). Media entries point to per-export blob
//! paths that don't survive cross-snapshot; reaction details rarely
//! survive privacy review.
//!
//! ## Occurrence identity
//!
//! `(thread_name, sender_name, timestamp_ms, text_hint)` — Facebook
//! doesn't expose per-message ids in the GDPR export, so we synthesize
//! a tuple. `text_hint` is the first 64 bytes of text (or empty) so
//! distinct messages with the same (thread, sender, ts) but different
//! bodies dedupe correctly. The hint is part of the key, not exposed
//! in the payload.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceBuildImpact, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// ---------------------------------------------------------------------------
// Raw export shape
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MessengerThread {
    #[serde(default)]
    participants: Vec<String>,
    #[serde(default, rename = "threadName")]
    thread_name: String,
    #[serde(default)]
    messages: Vec<MessengerMessage>,
}

#[derive(Debug, Deserialize)]
struct MessengerMessage {
    #[serde(default, rename = "isUnsent")]
    is_unsent: bool,
    #[serde(default)]
    media: Vec<serde_json::Value>,
    #[serde(default)]
    reactions: Vec<serde_json::Value>,
    #[serde(default, rename = "senderName")]
    sender_name: String,
    #[serde(default)]
    text: Option<String>,
    /// Epoch milliseconds.
    timestamp: i64,
    #[serde(default, rename = "type")]
    message_type: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessengerParserConfig;

#[derive(Debug, Clone, Default)]
pub struct MessengerThreadParser;

#[async_trait]
impl MaterialParser for MessengerThreadParser {
    type Config = MessengerParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("facebook-messenger-thread"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("facebook-messenger-thread"),
            declared_event_types: vec![(
                EventSource::from_static("messenger"),
                EventType::from_static("message.sent"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses Facebook Messenger GDPR export thread JSON \
                files. Preserves participants + sender + text; drops media \
                blob references and reaction details (count only)."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let thread: MessengerThread = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid Messenger thread JSON: {e}")))?;

        let mut intents = Vec::with_capacity(thread.messages.len());
        for (index, msg) in thread.messages.into_iter().enumerate() {
            intents.push(parse_message(
                msg,
                index,
                &thread.thread_name,
                &thread.participants,
                ctx,
            )?);
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        vec!["/messages".to_owned()]
    }
}

fn parse_message(
    msg: MessengerMessage,
    index: usize,
    thread_name: &str,
    participants: &[String],
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let sent_at = Timestamp::new(
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(msg.timestamp) * 1_000_000)
            .map_err(|e| {
                ParserError::Parse(format!(
                    "invalid Messenger timestamp {} ms: {e}",
                    msg.timestamp
                ))
            })?,
    );

    let media_count = msg.media.len() as u32;
    let reaction_count = msg.reactions.len() as u32;

    let text_hint: String = msg.text.as_deref().unwrap_or("").chars().take(64).collect();
    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("facebook-messenger-thread"),
        fields: vec![
            ("thread_name".into(), thread_name.to_string()),
            ("sender_name".into(), msg.sender_name.clone()),
            ("timestamp_ms".into(), msg.timestamp.to_string()),
            ("text_hint".into(), text_hint),
        ],
    };

    let payload = serde_json::json!({
        "sent_at": sent_at,
        "thread_name": thread_name,
        "sender_name": msg.sender_name,
        "participants": participants,
        "message_type": if msg.message_type.is_empty() { "text".to_string() } else { msg.message_type },
        "text": msg.text,
        "is_unsent": msg.is_unsent,
        "media_count": media_count,
        "reaction_count": reaction_count,
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("facebook-messenger-thread"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("message.sent"))
        .event_source(EventSource::from_static("messenger"))
        .payload(payload)
        .ts_orig(sent_at)
        .timing(TimingEvidence::Intrinsic {
            field: "timestamp".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::ByteRange {
            start: index as u64,
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ---------------------------------------------------------------------------
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "facebook-messenger-thread",
        namespace: "messaging",
        event_types: &[("messenger", "message.sent")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(thread_name, sender_name, timestamp_ms, text_hint)",
        ),
        access_policy: "personal_private_messages",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:facebook-messenger-thread"),
        "facebook-messenger-thread",
        "messaging",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("message.sent")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("facebook-messenger-thread")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("facebook_messenger_thread_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_source!(
    source_id: "facebook-messenger-thread",
    adapter: StaticFileAdapter,
    parser: MessengerThreadParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("facebook-messenger-thread"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    const SAMPLE_THREAD: &str = r#"{
      "participants": ["Alice", "Bob"],
      "threadName": "Bob_1",
      "messages": [
        {
          "isUnsent": false,
          "media": [],
          "reactions": [],
          "senderName": "Alice",
          "text": "hello there",
          "timestamp": 1710626737370,
          "type": "text"
        },
        {
          "isUnsent": false,
          "media": [{"uri": "media/photo.jpg"}, {"uri": "media/photo2.jpg"}],
          "reactions": [{"actor": "Bob", "reaction": "love"}],
          "senderName": "Bob",
          "text": "look at this",
          "timestamp": 1710626800000,
          "type": "text"
        }
      ]
    }"#;

    #[sinex_test]
    async fn parses_thread_into_two_intents() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "messenger");
            assert_eq!(intent.event_type.as_str(), "message.sent");
        }
        Ok(())
    }

    #[sinex_test]
    async fn preserves_thread_sender_participants() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["thread_name"], "Bob_1");
        assert_eq!(intents[0].payload["sender_name"], "Alice");
        assert_eq!(intents[0].payload["participants"][0], "Alice");
        assert_eq!(intents[0].payload["participants"][1], "Bob");
        Ok(())
    }

    #[sinex_test]
    async fn media_and_reactions_summarized_to_count() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents[0].payload["media_count"], 0);
        assert_eq!(intents[0].payload["reaction_count"], 0);
        assert_eq!(intents[1].payload["media_count"], 2);
        assert_eq!(intents[1].payload["reaction_count"], 1);
        // The full media/reactions arrays must NOT be present.
        assert!(intents[1].payload.get("media").is_none());
        assert!(intents[1].payload.get("reactions").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn epoch_ms_timestamp_parses_correctly() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // 1_710_626_737_370 ms = 2024-03-16 21:25:37.370 UTC
        let ts = intents[0].ts_orig.inner();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 3);
        assert_eq!(ts.day(), 16);
        Ok(())
    }

    #[sinex_test]
    async fn anchor_uses_message_index() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::ByteRange { start: 0, len: 1 }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::ByteRange { start: 1, len: 1 }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_includes_text_hint() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_THREAD.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        // Last field should be text_hint = first 64 chars of "hello there"
        assert_eq!(key.fields[3], ("text_hint".into(), "hello there".into()));
        Ok(())
    }

    #[sinex_test]
    async fn missing_text_falls_back_to_empty_hint() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let no_text = r#"{
          "participants": ["A"],
          "threadName": "A",
          "messages": [
            {"isUnsent": false, "media": [], "reactions": [], "senderName": "A",
             "timestamp": 1710626737370, "type": "share"}
          ]
        }"#;
        let intents = parser
            .parse_record(record_for(no_text.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields[3], ("text_hint".into(), String::new()));
        assert!(intents[0].payload["text"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn unicode_text_hint_clamps_to_chars_not_bytes() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let unicode = "\
            {\"participants\":[\"A\"],\"threadName\":\"T\",\"messages\":[{\
            \"isUnsent\":false,\"media\":[],\"reactions\":[],\
            \"senderName\":\"A\",\
            \"text\":\"\u{4f60}\u{597d}\u{4e16}\u{754c}repeatedmany\",\
            \"timestamp\":1710626737370,\"type\":\"text\"}]}";
        let intents = parser
            .parse_record(record_for(unicode.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        let hint = &key.fields[3].1;
        assert!(hint.chars().count() <= 64);
        Ok(())
    }

    #[sinex_test]
    async fn invalid_json_errors() -> TestResult<()> {
        let mut parser = MessengerThreadParser;
        let result = parser
            .parse_record(record_for(b"not json"), &test_ctx())
            .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid Messenger thread JSON"), "got: {err}");
        Ok(())
    }
}
