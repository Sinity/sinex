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

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "ai-session-claude",
    namespace = "ai_session",
    event_source = "claude",
    event_type = "ai.message",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(session_id, message_id)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct ClaudeSessionParser;

#[async_trait]
impl MaterialParser for ClaudeSessionParser {
    type Config = ClaudeSessionParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("claude-ai-session"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("ai-session-claude"),
            declared_event_types: vec![(
                EventSource::from_static("claude"),
                EventType::from_static("ai.message"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
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
        source_id: SourceId::from_static("ai-session-claude"),
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
        .source_id(ctx.source_id.clone())
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

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "ai-session-chatgpt",
    namespace = "ai_session",
    event_source = "chatgpt",
    event_type = "ai.message",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(session_id, message_id)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct ChatGptSessionParser;

#[async_trait]
impl MaterialParser for ChatGptSessionParser {
    type Config = ChatGptSessionParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("chatgpt-ai-session"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("ai-session-chatgpt"),
            declared_event_types: vec![(
                EventSource::from_static("chatgpt"),
                EventType::from_static("ai.message"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
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
        source_id: SourceId::from_static("ai-session-chatgpt"),
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
            .source_id(ctx.source_id.clone())
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "ai_session_test.rs"]
mod tests;
