//! Facebook Messenger GDPR export parser.
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

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

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

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "facebook-messenger-thread",
    namespace = "messaging",
    event_source = "messenger",
    event_type = "message.sent",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(thread_name, sender_name, timestamp_ms, text_hint)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "messaging_test.rs"]
mod tests;
