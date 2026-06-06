//! Integration tests for the Claude and `ChatGPT` AI-session parsers (#1068).
//!
//! These tests exercise the parser logic end-to-end using synthetic JSON
//! payloads that mirror the real GDPR/data-export formats.

use sinexd::runtime::parser::{MaterialParser, ParserError};
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceId},
    temporal::Timestamp,
};
use sinexd::sources::source_contracts::ai_session::{ChatGptSessionParser, ClaudeSessionParser};
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn claude_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("ai-session-claude"),
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
        source_id: SourceId::from_static("ai-session-chatgpt"),
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

// ---------------------------------------------------------------------------
// Claude parser tests
// ---------------------------------------------------------------------------

/// Two conversations with 2 and 1 messages respectively → 3 total intents.
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

/// `session_id` and `message_id` are preserved from the export.
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

/// Anchor encodes `conv_index * 1_000_000 + msg_index`.
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
    assert_eq!(
        intents[0].anchor,
        MaterialAnchor::ByteRange { start: 0, len: 1 }
    );
    assert_eq!(
        intents[1].anchor,
        MaterialAnchor::ByteRange { start: 1, len: 1 }
    );
    assert_eq!(
        intents[2].anchor,
        MaterialAnchor::ByteRange {
            start: 1_000_000,
            len: 1
        }
    );
    Ok(())
}

/// Occurrence key fields: [`session_id`, `message_id`] in order.
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

/// Older export batches use a flat `text` field instead of `content` array.
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

/// Non-JSON input → `ParserError::Parse`.
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

// ---------------------------------------------------------------------------
// ChatGPT parser tests
// ---------------------------------------------------------------------------

fn chatgpt_minimal_json() -> serde_json::Value {
    // One conversation with a root node, one user message, one assistant message.
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
                        "content": {"content_type": "text", "parts": ["Hello GPT"]},
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
                        "content": {"content_type": "text", "parts": ["Hello user!"]},
                        "metadata": {"model_slug": "gpt-4o"}
                    }
                }
            }
        }
    ])
}

/// Root node has no message; 2 text nodes → 2 intents.
#[sinex_test]
async fn chatgpt_parses_thread_into_intents() -> TestResult<()> {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].event_source.as_static_str(), "chatgpt");
    assert_eq!(intents[0].event_type.as_static_str(), "ai.message");
    Ok(())
}

/// `session_id`, `message_id`, role, text are all preserved.
#[sinex_test]
async fn chatgpt_preserves_session_and_message_ids() -> TestResult<()> {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[0].payload["session_id"], "chatgpt-conv-1");
    assert_eq!(intents[0].payload["message_id"], "node-user");
    assert_eq!(intents[0].payload["role"], "user");
    assert_eq!(intents[0].payload["text"], "Hello GPT");
    Ok(())
}

/// `model_slug` from message metadata takes priority over conversation default.
#[sinex_test]
async fn chatgpt_model_slug_from_metadata() -> TestResult<()> {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[1].payload["model"], "gpt-4o");
    Ok(())
}

/// Non-text content nodes (tool outputs, DALL-E, etc.) are skipped.
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
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["text"], "actual text");
    Ok(())
}

/// Non-JSON input → `ParserError::Parse`.
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

/// Occurrence key fields: [`session_id`, `message_id`] in order.
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
