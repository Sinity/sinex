//! AI session export parsers — Claude and `ChatGPT` (#1068).
//!
//! ## Claude
//!
//! Reads `conversations.json` from a Claude GDPR export (a single JSON array).
//! Each array element is a conversation object:
//!
//! ```json
//! {
//!   "uuid": "<session-uuid>",
//!   "name": "<title or empty>",
//!   "created_at": "<iso8601>",
//!   "chat_messages": [
//!     {
//!       "uuid": "<msg-uuid>",
//!       "sender": "human" | "assistant",
//!       "created_at": "<iso8601>",
//!       "content": [{ "type": "text", "text": "..." }]
//!     }
//!   ]
//! }
//! ```
//!
//! Each message becomes one `claude`/`ai.message` event.
//!
//! ## `ChatGPT`
//!
//! Reads one of potentially many `conversations-NNN.json` files from a `ChatGPT`
//! data export. Each file is a JSON array of conversation objects. Each
//! conversation uses a `mapping` node graph where nodes have `parent`/`children`
//! references. The canonical thread is reconstructed by walking backwards from
//! `current_node` to the root, then reversing. Only `content_type = "text"`
//! messages from `user`/`assistant`/`system`/`tool` roles are included; system
//! and tool-use nodes without printable text are skipped.
//!
//! ## Privacy
//!
//! Both exports contain free-form conversation text. Privacy tier is
//! `Sensitive`, context is `Document`. The admission policy can strip the
//! `text` field under `Suppress` if needed.
//!
//! ## Occurrence identity
//!
//! **Claude**: `(session_id, message_id)` — both are stable UUIDs from the export.
//!
//! **`ChatGPT`**: `(session_id, message_id)` — `session_id` = `conversation.id`,
//! `message_id` = mapping node id (a stable UUID in the export).
//!
//! ## Anchoring
//!
//! `ByteRange { start: <conversation_index * 1_000_000 + message_index>, len: 1 }`
//!
//! For Claude: `conversations.json` is a single flat array, so
//! `start = conv_index * 1_000_000 + msg_index` encodes both the conversation
//! and the message position.  For `ChatGPT`: same scheme across the per-batch
//! file; `conv_index` is the conversation's index within the current file's
//! array.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Shared anchor helper
// ---------------------------------------------------------------------------

/// Encode (`conversation_index`, `message_index`) into a single u64 anchor.
///
/// We reserve `1_000_000` positions per conversation. That is sufficient for any
/// real-world conversation length; the scheme stays stable across partial
/// re-exports as long as the file's conversation order is stable.
fn anchor(conv_index: usize, msg_index: usize) -> u64 {
    (conv_index as u64) * 1_000_000 + (msg_index as u64)
}

// ===========================================================================
// Claude parser
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw export shape
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ClaudeConversation {
    uuid: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    chat_messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Deserialize)]
struct ClaudeMessage {
    uuid: String,
    sender: String,
    created_at: String,
    #[serde(default)]
    content: Vec<ClaudeContentBlock>,
    /// Flat text field present in older export batches.
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeContentBlock {
    #[serde(rename = "type", default)]
    block_type: String,
    #[serde(default)]
    text: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClaudeSessionParserConfig;

#[derive(Debug, Clone, Default)]
pub struct ClaudeSessionParser;

#[async_trait]
impl MaterialParser for ClaudeSessionParser {
    type Config = ClaudeSessionParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("claude-ai-session"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("ai-session-claude"),
            declared_event_types: vec![(
                EventSource::from_static("claude"),
                EventType::from_static("ai.message"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "anchor_conv_msg_index".into(),
                "occurrence_key_session_id_message_id".into(),
                "text_privacy_context_document".into(),
            ],
            description: "Parses Claude GDPR export conversations.json. \
                Emits one ai.message event per chat message. Content text \
                is tagged Document for admission-layer suppression."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let conversations: Vec<ClaudeConversation> = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid Claude conversations.json: {e}")))?;

        let mut intents = Vec::new();
        for (conv_index, conv) in conversations.into_iter().enumerate() {
            for (msg_index, msg) in conv.chat_messages.into_iter().enumerate() {
                intents.push(parse_claude_message(
                    msg, conv_index, msg_index, &conv.uuid, &conv.name, ctx,
                )?);
            }
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["/[]/uuid", "/[]/chat_messages"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

fn parse_claude_message(
    msg: ClaudeMessage,
    conv_index: usize,
    msg_index: usize,
    session_id: &str,
    conversation_name: &str,
    ctx: &ParserContext,
) -> ParserResult<ParsedEventIntent> {
    let message_ts = Timestamp::new(
        time::OffsetDateTime::parse(
            &msg.created_at,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|e| {
            ParserError::Parse(format!(
                "invalid Claude message timestamp {:?}: {e}",
                msg.created_at
            ))
        })?,
    );

    // Prefer the structured content array; fall back to the flat `text` field
    // present in older export batches.
    let text: Option<String> = {
        let from_content: String = msg
            .content
            .iter()
            .filter(|b| b.block_type == "text" || b.block_type.is_empty())
            .map(|b| b.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if from_content.is_empty() {
            msg.text.filter(|t| !t.is_empty())
        } else {
            Some(from_content)
        }
    };

    let occurrence_key = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("ai-session-claude"),
        fields: vec![
            ("session_id".into(), session_id.to_string()),
            ("message_id".into(), msg.uuid.clone()),
        ],
    };

    let conversation_name_opt: Option<String> = if conversation_name.is_empty() {
        None
    } else {
        Some(conversation_name.to_string())
    };

    let payload = serde_json::json!({
        "session_id": session_id,
        "message_id": msg.uuid,
        "role": msg.sender,
        "text": text,
        "message_ts": message_ts,
        "conversation_name": conversation_name_opt,
    });

    Ok(ParsedEventIntent::builder()
        .source_unit_id(ctx.source_unit_id.clone())
        .parser_id(ParserId::from_static("claude-ai-session"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("ai.message"))
        .event_source(EventSource::from_static("claude"))
        .payload(payload)
        .ts_orig(message_ts)
        .timing(TimingEvidence::Intrinsic {
            field: "created_at".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::ByteRange {
            start: anchor(conv_index, msg_index),
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ---------------------------------------------------------------------------
// Source-unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "ai-session-claude",
        namespace: "ai_session",
        event_types: &[("claude", "ai.message")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "anchor_conv_msg_index",
            "occurrence_key_session_id_message_id",
            "text_privacy_context_document",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(session_id, message_id)"),
        access_policy: "personal_ai_conversations",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:ai-session-claude"),
        "ai-session-claude",
        "ai_session",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("ai.message")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("ai-session-claude")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("ai_session_claude_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "ai-session-claude",
    adapter: StaticFileAdapter,
    parser: ClaudeSessionParser,
);

// ===========================================================================
// ChatGPT parser
// ===========================================================================

// ---------------------------------------------------------------------------
// Raw export shape
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ChatGptConversation {
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    current_node: Option<String>,
    #[serde(default)]
    default_model_slug: Option<String>,
    #[serde(default)]
    mapping: std::collections::HashMap<String, ChatGptNode>,
}

#[derive(Debug, Deserialize)]
struct ChatGptNode {
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    message: Option<ChatGptMessage>,
}

#[derive(Debug, Deserialize)]
struct ChatGptMessage {
    id: String,
    author: ChatGptAuthor,
    #[serde(default)]
    create_time: Option<f64>,
    content: ChatGptContent,
    #[serde(default)]
    metadata: ChatGptMessageMetadata,
}

#[derive(Debug, Deserialize)]
struct ChatGptAuthor {
    role: String,
}

#[derive(Debug, Deserialize)]
struct ChatGptContent {
    content_type: String,
    #[serde(default)]
    parts: Vec<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize)]
struct ChatGptMessageMetadata {
    #[serde(default)]
    model_slug: Option<String>,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatGptSessionParserConfig;

#[derive(Debug, Clone, Default)]
pub struct ChatGptSessionParser;

#[async_trait]
impl MaterialParser for ChatGptSessionParser {
    type Config = ChatGptSessionParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("chatgpt-ai-session"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("ai-session-chatgpt"),
            declared_event_types: vec![(
                EventSource::from_static("chatgpt"),
                EventType::from_static("ai.message"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "anchor_conv_msg_index".into(),
                "occurrence_key_session_id_message_id".into(),
                "text_privacy_context_document".into(),
                "mapping_walk_current_node_to_root".into(),
            ],
            description: "Parses ChatGPT data export conversations-NNN.json. \
                Reconstructs the canonical thread by walking from current_node \
                to root. Emits one ai.message event per text-content message. \
                Tool-use and non-text content nodes are skipped."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let conversations: Vec<ChatGptConversation> = serde_json::from_slice(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid ChatGPT conversations JSON: {e}")))?;

        let mut intents = Vec::new();
        for (conv_index, conv) in conversations.into_iter().enumerate() {
            let thread = extract_chatgpt_thread(&conv)?;
            for (msg_index, msg) in thread.into_iter().enumerate() {
                if let Some(intent) = parse_chatgpt_message(
                    msg,
                    conv_index,
                    msg_index,
                    &conv.id,
                    &conv.title,
                    conv.default_model_slug.as_deref(),
                    ctx,
                )? {
                    intents.push(intent);
                }
            }
        }

        Ok(intents)
    }

    fn required_input_keys(&self) -> Vec<String> {
        ["/[]/id", "/[]/current_node", "/[]/mapping"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

/// Walk from `current_node` to the root and return the messages in
/// chronological order (root first, most recent last).
fn extract_chatgpt_thread(conv: &ChatGptConversation) -> ParserResult<Vec<&ChatGptMessage>> {
    let mut path: Vec<&ChatGptMessage> = Vec::new();
    let mut node_id = conv.current_node.as_deref();

    while let Some(id) = node_id {
        let node = conv.mapping.get(id).ok_or_else(|| {
            ParserError::Parse(format!(
                "ChatGPT conversation {}: node {id} missing from mapping",
                conv.id
            ))
        })?;
        if let Some(msg) = &node.message {
            path.push(msg);
        }
        node_id = node.parent.as_deref();
    }

    path.reverse();
    Ok(path)
}

fn parse_chatgpt_message(
    msg: &ChatGptMessage,
    conv_index: usize,
    msg_index: usize,
    session_id: &str,
    title: &str,
    default_model: Option<&str>,
    ctx: &ParserContext,
) -> ParserResult<Option<ParsedEventIntent>> {
    // Only emit text-content messages.
    if msg.content.content_type != "text" {
        return Ok(None);
    }

    // Extract text from parts (may be strings or structured objects).
    let text: String = msg
        .content
        .parts
        .iter()
        .filter_map(|p| p.as_str().map(std::string::ToString::to_string))
        .collect::<Vec<_>>()
        .join("\n");

    // Skip messages with no text (empty assistant turns, etc.).
    let text_opt: Option<String> = if text.is_empty() { None } else { Some(text) };

    let create_time = msg.create_time.ok_or_else(|| {
        ParserError::Parse(format!("ChatGPT message {} missing create_time", msg.id))
    })?;

    let secs = create_time.trunc() as i64;
    let nanos = ((create_time.fract()) * 1e9) as i64;
    let message_ts = Timestamp::new(
        time::OffsetDateTime::from_unix_timestamp(secs).map_err(|e| {
            ParserError::Parse(format!("invalid ChatGPT timestamp {create_time}: {e}"))
        })? + time::Duration::nanoseconds(nanos),
    );

    let model = msg
        .metadata
        .model_slug
        .as_deref()
        .or(default_model)
        .map(str::to_string);

    let title_opt: Option<String> = if title.is_empty() {
        None
    } else {
        Some(title.to_string())
    };

    let occurrence_key = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("ai-session-chatgpt"),
        fields: vec![
            ("session_id".into(), session_id.to_string()),
            ("message_id".into(), msg.id.clone()),
        ],
    };

    let payload = serde_json::json!({
        "session_id": session_id,
        "message_id": msg.id,
        "role": msg.author.role,
        "text": text_opt,
        "message_ts": message_ts,
        "conversation_title": title_opt,
        "model": model,
    });

    Ok(Some(
        ParsedEventIntent::builder()
            .source_unit_id(ctx.source_unit_id.clone())
            .parser_id(ParserId::from_static("chatgpt-ai-session"))
            .parser_version("1.0.0")
            .event_type(EventType::from_static("ai.message"))
            .event_source(EventSource::from_static("chatgpt"))
            .payload(payload)
            .ts_orig(message_ts)
            .timing(TimingEvidence::Intrinsic {
                field: "create_time".into(),
                confidence: TimingConfidence::Intrinsic,
            })
            .anchor(MaterialAnchor::ByteRange {
                start: anchor(conv_index, msg_index),
                len: 1,
            })
            .occurrence_key(occurrence_key)
            .privacy_context(ProcessingContext::Document)
            .build(),
    ))
}

// ---------------------------------------------------------------------------
// Source-unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "ai-session-chatgpt",
        namespace: "ai_session",
        event_types: &[("chatgpt", "ai.message")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "anchor_conv_msg_index",
            "occurrence_key_session_id_message_id",
            "text_privacy_context_document",
            "mapping_walk_current_node_to_root",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(session_id, message_id)"),
        access_policy: "personal_ai_conversations",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:ai-session-chatgpt"),
        "ai-session-chatgpt",
        "ai_session",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("ai.message")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("ai-session-chatgpt")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("ai_session_chatgpt_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "ai-session-chatgpt",
    adapter: StaticFileAdapter,
    parser: ChatGptSessionParser,
);

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    fn claude_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("ai-session-claude"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn chatgpt_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("ai-session-chatgpt"),
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

    // --- Claude tests ---

    #[sinex_test]
    async fn claude_parses_two_conversations_into_correct_intent_count() -> TestResult<()> {
        let json = serde_json::json!([
            {
                "uuid": "conv-aaa",
                "name": "First",
                "chat_messages": [
                    {
                        "uuid": "msg-001",
                        "sender": "human",
                        "created_at": "2024-06-01T10:00:00.000000Z",
                        "content": [{"type": "text", "text": "Hello there"}]
                    },
                    {
                        "uuid": "msg-002",
                        "sender": "assistant",
                        "created_at": "2024-06-01T10:00:05.000000Z",
                        "content": [{"type": "text", "text": "Hi!"}]
                    }
                ]
            },
            {
                "uuid": "conv-bbb",
                "name": "",
                "chat_messages": [
                    {
                        "uuid": "msg-003",
                        "sender": "human",
                        "created_at": "2024-06-02T09:00:00.000000Z",
                        "content": [{"type": "text", "text": "Separate session"}]
                    }
                ]
            }
        ]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = claude_ctx();
        let intents = ClaudeSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        assert_eq!(
            intents.len(),
            3,
            "expected 3 intents across 2 conversations"
        );
        assert_eq!(intents[0].event_source.as_static_str(), "claude");
        assert_eq!(intents[0].event_type.as_static_str(), "ai.message");
        Ok(())
    }

    #[sinex_test]
    async fn claude_preserves_session_id_and_message_id() -> TestResult<()> {
        let json = serde_json::json!([{
            "uuid": "session-xyz",
            "name": "Test session",
            "chat_messages": [{
                "uuid": "msg-unique-001",
                "sender": "human",
                "created_at": "2025-01-15T12:00:00.000000Z",
                "content": [{"type": "text", "text": "Question"}]
            }]
        }]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = claude_ctx();
        let mut intents = ClaudeSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        let intent = intents.remove(0);
        assert_eq!(intent.payload["session_id"], "session-xyz");
        assert_eq!(intent.payload["message_id"], "msg-unique-001");
        assert_eq!(intent.payload["role"], "human");
        assert_eq!(intent.payload["conversation_name"], "Test session");
        Ok(())
    }

    #[sinex_test]
    async fn claude_anchor_encodes_conv_and_msg_index() -> TestResult<()> {
        let json = serde_json::json!([
            {
                "uuid": "conv-1",
                "name": "",
                "chat_messages": [
                    {"uuid": "m0", "sender": "human", "created_at": "2025-01-01T00:00:00Z",
                     "content": [{"type": "text", "text": "a"}]},
                    {"uuid": "m1", "sender": "assistant", "created_at": "2025-01-01T00:00:01Z",
                     "content": [{"type": "text", "text": "b"}]}
                ]
            },
            {
                "uuid": "conv-2",
                "name": "",
                "chat_messages": [
                    {"uuid": "m2", "sender": "human", "created_at": "2025-01-02T00:00:00Z",
                     "content": [{"type": "text", "text": "c"}]}
                ]
            }
        ]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = claude_ctx();
        let intents = ClaudeSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        // conv=0, msg=0 -> 0*1_000_000 + 0 = 0
        assert_eq!(
            intents[0].anchor,
            MaterialAnchor::ByteRange { start: 0, len: 1 }
        );
        // conv=0, msg=1 -> 0*1_000_000 + 1 = 1
        assert_eq!(
            intents[1].anchor,
            MaterialAnchor::ByteRange { start: 1, len: 1 }
        );
        // conv=1, msg=0 -> 1*1_000_000 + 0 = 1_000_000
        assert_eq!(
            intents[2].anchor,
            MaterialAnchor::ByteRange {
                start: 1_000_000,
                len: 1
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn claude_occurrence_key_fields_and_order() -> TestResult<()> {
        let json = serde_json::json!([{
            "uuid": "s1",
            "name": "",
            "chat_messages": [
                {"uuid": "m1", "sender": "human", "created_at": "2025-03-01T00:00:00Z",
                 "content": [{"type": "text", "text": "x"}]}
            ]
        }]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = claude_ctx();
        let intents = ClaudeSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields[0], ("session_id".into(), "s1".into()));
        assert_eq!(key.fields[1], ("message_id".into(), "m1".into()));
        Ok(())
    }

    #[sinex_test]
    async fn claude_falls_back_to_flat_text_field() -> TestResult<()> {
        let json = serde_json::json!([{
            "uuid": "s1",
            "name": "",
            "chat_messages": [{
                "uuid": "m1",
                "sender": "human",
                "created_at": "2025-01-01T00:00:00Z",
                "content": [],
                "text": "Fallback text only"
            }]
        }]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = claude_ctx();
        let intents = ClaudeSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        assert_eq!(intents[0].payload["text"], "Fallback text only");
        Ok(())
    }

    #[sinex_test]
    async fn claude_invalid_json_returns_parser_error() -> TestResult<()> {
        let bytes = b"not json at all";
        let ctx = claude_ctx();
        let result = ClaudeSessionParser
            .parse_record(record_for(bytes), &ctx)
            .await;
        assert!(matches!(result, Err(ParserError::Parse(_))));
        Ok(())
    }

    // --- ChatGPT tests ---

    fn chatgpt_minimal_json() -> serde_json::Value {
        // One conversation: root <- user <- assistant (typical linear chain).
        serde_json::json!([
            {
                "id": "chatgpt-conv-1",
                "title": "Test Convo",
                "current_node": "node-asst",
                "default_model_slug": "gpt-4",
                "mapping": {
                    "node-root": {
                        "parent": null,
                        "children": ["node-user"],
                        "message": null
                    },
                    "node-user": {
                        "parent": "node-root",
                        "children": ["node-asst"],
                        "message": {
                            "id": "node-user",
                            "author": {"role": "user"},
                            "create_time": 1717228800.0,
                            "content": {
                                "content_type": "text",
                                "parts": ["Hello GPT"]
                            },
                            "metadata": {}
                        }
                    },
                    "node-asst": {
                        "parent": "node-user",
                        "children": [],
                        "message": {
                            "id": "node-asst",
                            "author": {"role": "assistant"},
                            "create_time": 1717228860.0,
                            "content": {
                                "content_type": "text",
                                "parts": ["Hello user!"]
                            },
                            "metadata": {"model_slug": "gpt-4o"}
                        }
                    }
                }
            }
        ])
    }

    #[sinex_test]
    async fn chatgpt_parses_thread_into_intents() -> TestResult<()> {
        let json = chatgpt_minimal_json();
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = chatgpt_ctx();
        let intents = ChatGptSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        // root node has no message, so 2 text messages emitted
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].event_source.as_static_str(), "chatgpt");
        assert_eq!(intents[0].event_type.as_static_str(), "ai.message");
        Ok(())
    }

    #[sinex_test]
    async fn chatgpt_preserves_session_and_message_ids() -> TestResult<()> {
        let json = chatgpt_minimal_json();
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = chatgpt_ctx();
        let intents = ChatGptSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        // first intent = user message (path reversed: root, user, asst → user first)
        assert_eq!(intents[0].payload["session_id"], "chatgpt-conv-1");
        assert_eq!(intents[0].payload["message_id"], "node-user");
        assert_eq!(intents[0].payload["role"], "user");
        assert_eq!(intents[0].payload["text"], "Hello GPT");
        Ok(())
    }

    #[sinex_test]
    async fn chatgpt_model_slug_from_metadata() -> TestResult<()> {
        let json = chatgpt_minimal_json();
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = chatgpt_ctx();
        let intents = ChatGptSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        // second intent = assistant; metadata has model_slug = "gpt-4o"
        assert_eq!(intents[1].payload["model"], "gpt-4o");
        Ok(())
    }

    #[sinex_test]
    async fn chatgpt_skips_non_text_content() -> TestResult<()> {
        let json = serde_json::json!([{
            "id": "c1",
            "title": "",
            "current_node": "n2",
            "mapping": {
                "n1": {
                    "parent": null,
                    "message": {
                        "id": "n1",
                        "author": {"role": "user"},
                        "create_time": 1717228800.0,
                        "content": {"content_type": "tether_browsing_display", "parts": []},
                        "metadata": {}
                    }
                },
                "n2": {
                    "parent": "n1",
                    "message": {
                        "id": "n2",
                        "author": {"role": "assistant"},
                        "create_time": 1717228860.0,
                        "content": {"content_type": "text", "parts": ["actual text"]},
                        "metadata": {}
                    }
                }
            }
        }]);
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = chatgpt_ctx();
        let intents = ChatGptSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        // Only the text node is emitted
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].payload["text"], "actual text");
        Ok(())
    }

    #[sinex_test]
    async fn chatgpt_invalid_json_returns_parser_error() -> TestResult<()> {
        let bytes = b"{not valid}";
        let ctx = chatgpt_ctx();
        let result = ChatGptSessionParser
            .parse_record(record_for(bytes), &ctx)
            .await;
        assert!(matches!(result, Err(ParserError::Parse(_))));
        Ok(())
    }

    #[sinex_test]
    async fn chatgpt_occurrence_key_fields_and_order() -> TestResult<()> {
        let json = chatgpt_minimal_json();
        let bytes = serde_json::to_vec(&json).unwrap();
        let ctx = chatgpt_ctx();
        let intents = ChatGptSessionParser
            .parse_record(record_for(&bytes), &ctx)
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields[0].0, "session_id");
        assert_eq!(key.fields[1].0, "message_id");
        Ok(())
    }
}
