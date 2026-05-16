//! Integration tests for the Claude and ChatGPT AI-session parsers (#1068).
//!
//! These tests exercise the parser logic end-to-end using synthetic JSON
//! payloads that mirror the real GDPR/data-export formats.

use sinex_primitives::{
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId},
    temporal::Timestamp,
    Uuid,
};
use sinex_source_worker::sources::ai_session::{ChatGptSessionParser, ClaudeSessionParser};
use sinex_node_sdk::parser::{MaterialParser, ParserError};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Claude parser tests
// ---------------------------------------------------------------------------

/// Two conversations with 2 and 1 messages respectively → 3 total intents.
#[tokio::test]
async fn claude_parses_two_conversations_into_correct_intent_count() {
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
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents.len(), 3, "expected 3 intents across 2 conversations");
    assert_eq!(intents[0].event_source.as_static_str(), "claude");
    assert_eq!(intents[0].event_type.as_static_str(), "ai.message");
}

/// session_id and message_id are preserved from the export.
#[tokio::test]
async fn claude_preserves_session_id_and_message_id() {
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
    let mut intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    let intent = intents.remove(0);
    assert_eq!(intent.payload["session_id"], "session-xyz");
    assert_eq!(intent.payload["message_id"], "msg-unique-001");
    assert_eq!(intent.payload["role"], "human");
    assert_eq!(intent.payload["conversation_name"], "Test session");
}

/// Anchor encodes `conv_index * 1_000_000 + msg_index`.
#[tokio::test]
async fn claude_anchor_encodes_conv_and_msg_index() {
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
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[0].anchor, MaterialAnchor::ByteRange { start: 0, len: 1 });
    assert_eq!(intents[1].anchor, MaterialAnchor::ByteRange { start: 1, len: 1 });
    assert_eq!(intents[2].anchor, MaterialAnchor::ByteRange { start: 1_000_000, len: 1 });
}

/// Occurrence key fields: [session_id, message_id] in order.
#[tokio::test]
async fn claude_occurrence_key_fields_and_order() {
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
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(key.fields[0], ("session_id".into(), "s1".into()));
    assert_eq!(key.fields[1], ("message_id".into(), "m1".into()));
}

/// Older export batches use a flat `text` field instead of `content` array.
#[tokio::test]
async fn claude_falls_back_to_flat_text_field() {
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
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[0].payload["text"], "Fallback text only");
}

/// Non-JSON input → ParserError::Parse.
#[tokio::test]
async fn claude_invalid_json_returns_parser_error() {
    let bytes = b"not json at all";
    let ctx = claude_ctx();
    let result = ClaudeSessionParser::default()
        .parse_record(record_for(bytes), &ctx)
        .await;
    assert!(matches!(result, Err(ParserError::Parse(_))));
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
#[tokio::test]
async fn chatgpt_parses_thread_into_intents() {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].event_source.as_static_str(), "chatgpt");
    assert_eq!(intents[0].event_type.as_static_str(), "ai.message");
}

/// session_id, message_id, role, text are all preserved.
#[tokio::test]
async fn chatgpt_preserves_session_and_message_ids() {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[0].payload["session_id"], "chatgpt-conv-1");
    assert_eq!(intents[0].payload["message_id"], "node-user");
    assert_eq!(intents[0].payload["role"], "user");
    assert_eq!(intents[0].payload["text"], "Hello GPT");
}

/// model_slug from message metadata takes priority over conversation default.
#[tokio::test]
async fn chatgpt_model_slug_from_metadata() {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents[1].payload["model"], "gpt-4o");
}

/// Non-text content nodes (tool outputs, DALL-E, etc.) are skipped.
#[tokio::test]
async fn chatgpt_skips_non_text_content() {
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
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["text"], "actual text");
}

/// Non-JSON input → ParserError::Parse.
#[tokio::test]
async fn chatgpt_invalid_json_returns_parser_error() {
    let bytes = b"{not valid}";
    let ctx = chatgpt_ctx();
    let result = ChatGptSessionParser::default()
        .parse_record(record_for(bytes), &ctx)
        .await;
    assert!(matches!(result, Err(ParserError::Parse(_))));
}

/// Occurrence key fields: [session_id, message_id] in order.
#[tokio::test]
async fn chatgpt_occurrence_key_fields_and_order() {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();
    let ctx = chatgpt_ctx();
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &ctx)
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(key.fields[0].0, "session_id");
    assert_eq!(key.fields[1].0, "message_id");
}

// ---------------------------------------------------------------------------
// AC verification tests (#1068)
// ---------------------------------------------------------------------------

/// AC #1 — StaticFile input shape is declared in both parser manifests.
///
/// The StaticFileAdapter wires these parsers to staged material files.
/// This test pins that the accepted_input_shapes contract is present so
/// the adapter registration stays coherent with the manifest.
#[tokio::test]
async fn ac1_manifest_declares_static_file_input_shape() {
    use sinex_primitives::parser::InputShapeKind;

    let claude_manifest = ClaudeSessionParser::default().manifest();
    assert!(
        claude_manifest
            .accepted_input_shapes
            .contains(&InputShapeKind::StaticFile),
        "ClaudeSessionParser must declare StaticFile input shape"
    );

    let chatgpt_manifest = ChatGptSessionParser::default().manifest();
    assert!(
        chatgpt_manifest
            .accepted_input_shapes
            .contains(&InputShapeKind::StaticFile),
        "ChatGptSessionParser must declare StaticFile input shape"
    );
}

/// AC #2a — Replaying the same Claude session twice yields identical occurrence keys.
///
/// This is the parser-side idempotency contract: given the same source bytes,
/// the occurrence key (session_id, message_id) is stable. Combined with
/// OccurrenceIdentity::Uuid5From in the source-unit descriptor, this means
/// ingestd can apply ON CONFLICT DO NOTHING for replay deduplication.
#[tokio::test]
async fn ac2_claude_idempotent_replay_produces_stable_occurrence_keys() {
    let json = serde_json::json!([{
        "uuid": "stable-session-id",
        "name": "Replay test",
        "chat_messages": [
            {
                "uuid": "msg-alpha",
                "sender": "human",
                "created_at": "2025-06-01T09:00:00Z",
                "content": [{"type": "text", "text": "first message"}]
            },
            {
                "uuid": "msg-beta",
                "sender": "assistant",
                "created_at": "2025-06-01T09:00:05Z",
                "content": [{"type": "text", "text": "second message"}]
            }
        ]
    }]);
    let bytes = serde_json::to_vec(&json).unwrap();

    let first_run = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &claude_ctx())
        .await
        .unwrap();
    let second_run = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &claude_ctx())
        .await
        .unwrap();

    assert_eq!(first_run.len(), second_run.len());
    for (a, b) in first_run.iter().zip(second_run.iter()) {
        let key_a = a.occurrence_key.as_ref().unwrap();
        let key_b = b.occurrence_key.as_ref().unwrap();
        assert_eq!(
            key_a.fields, key_b.fields,
            "occurrence key must be stable across replays"
        );
        // anchors must also be stable
        assert_eq!(a.anchor, b.anchor, "anchor must be stable across replays");
    }
}

/// AC #2b — Replaying the same ChatGPT session twice yields identical occurrence keys.
#[tokio::test]
async fn ac2_chatgpt_idempotent_replay_produces_stable_occurrence_keys() {
    let json = chatgpt_minimal_json();
    let bytes = serde_json::to_vec(&json).unwrap();

    let first_run = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &chatgpt_ctx())
        .await
        .unwrap();
    let second_run = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &chatgpt_ctx())
        .await
        .unwrap();

    assert_eq!(first_run.len(), second_run.len());
    for (a, b) in first_run.iter().zip(second_run.iter()) {
        let key_a = a.occurrence_key.as_ref().unwrap();
        let key_b = b.occurrence_key.as_ref().unwrap();
        assert_eq!(
            key_a.fields, key_b.fields,
            "occurrence key must be stable across replays"
        );
        assert_eq!(a.anchor, b.anchor, "anchor must be stable across replays");
    }
}

/// AC #3a — Claude turn ordering is chronological and traceable to anchors.
///
/// Messages within a conversation must be emitted in input order (msg_index
/// ascending) so downstream queries can reconstruct the conversation thread
/// by sorting on anchor start value. Each anchor encodes
/// (conv_index * 1_000_000 + msg_index), making ordering recoverable from
/// the anchor alone.
#[tokio::test]
async fn ac3_claude_turn_ordering_preserved_and_anchor_monotone() {
    let json = serde_json::json!([{
        "uuid": "order-session",
        "name": "Ordering test",
        "chat_messages": [
            {
                "uuid": "turn-0",
                "sender": "human",
                "created_at": "2025-01-10T10:00:00Z",
                "content": [{"type": "text", "text": "turn zero"}]
            },
            {
                "uuid": "turn-1",
                "sender": "assistant",
                "created_at": "2025-01-10T10:00:05Z",
                "content": [{"type": "text", "text": "turn one"}]
            },
            {
                "uuid": "turn-2",
                "sender": "human",
                "created_at": "2025-01-10T10:00:10Z",
                "content": [{"type": "text", "text": "turn two"}]
            }
        ]
    }]);
    let bytes = serde_json::to_vec(&json).unwrap();
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &claude_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 3);

    // Anchors must be strictly monotone (ordering contract).
    let anchors: Vec<u64> = intents
        .iter()
        .map(|i| match i.anchor {
            MaterialAnchor::ByteRange { start, .. } => start,
            _ => panic!("ai_session parser must use ByteRange anchors"),
        })
        .collect();
    for w in anchors.windows(2) {
        assert!(w[0] < w[1], "anchors must be strictly ascending: {:?}", anchors);
    }

    // message_id in payload traces back to occurrence key.
    for intent in &intents {
        let payload_mid = intent.payload["message_id"].as_str().unwrap();
        let key_mid = &intent.occurrence_key.as_ref().unwrap().fields[1].1;
        assert_eq!(
            payload_mid, key_mid,
            "payload.message_id must match occurrence_key.message_id for traceability"
        );
    }

    // Turn order matches input order by message_id.
    assert_eq!(intents[0].payload["message_id"], "turn-0");
    assert_eq!(intents[1].payload["message_id"], "turn-1");
    assert_eq!(intents[2].payload["message_id"], "turn-2");
}

/// AC #3b — ChatGPT thread walk order is root-to-leaf (chronological).
///
/// The parser reconstructs the path by walking from current_node to root
/// then reversing. This test verifies the emitted intents are in
/// chronological order (root → leaf) and that anchors are monotone.
#[tokio::test]
async fn ac3_chatgpt_turn_ordering_root_to_leaf_and_anchor_monotone() {
    // Three-node chain: root ← mid ← leaf (current_node = leaf)
    let json = serde_json::json!([{
        "id": "order-conv",
        "title": "Order test",
        "current_node": "leaf",
        "mapping": {
            "root": {
                "parent": null,
                "message": {
                    "id": "root",
                    "author": {"role": "user"},
                    "create_time": 1717228800.0,
                    "content": {"content_type": "text", "parts": ["first"]},
                    "metadata": {}
                }
            },
            "mid": {
                "parent": "root",
                "message": {
                    "id": "mid",
                    "author": {"role": "assistant"},
                    "create_time": 1717228860.0,
                    "content": {"content_type": "text", "parts": ["second"]},
                    "metadata": {}
                }
            },
            "leaf": {
                "parent": "mid",
                "message": {
                    "id": "leaf",
                    "author": {"role": "user"},
                    "create_time": 1717228920.0,
                    "content": {"content_type": "text", "parts": ["third"]},
                    "metadata": {}
                }
            }
        }
    }]);
    let bytes = serde_json::to_vec(&json).unwrap();
    let intents = ChatGptSessionParser::default()
        .parse_record(record_for(&bytes), &chatgpt_ctx())
        .await
        .unwrap();

    assert_eq!(intents.len(), 3);

    // Anchors must be strictly monotone.
    let anchors: Vec<u64> = intents
        .iter()
        .map(|i| match i.anchor {
            MaterialAnchor::ByteRange { start, .. } => start,
            _ => panic!("ai_session parser must use ByteRange anchors"),
        })
        .collect();
    for w in anchors.windows(2) {
        assert!(w[0] < w[1], "anchors must be strictly ascending: {:?}", anchors);
    }

    // Chronological order: root first, leaf last.
    assert_eq!(intents[0].payload["message_id"], "root");
    assert_eq!(intents[1].payload["message_id"], "mid");
    assert_eq!(intents[2].payload["message_id"], "leaf");

    // Each message_id in payload matches the occurrence key for traceability.
    for intent in &intents {
        let payload_mid = intent.payload["message_id"].as_str().unwrap();
        let key_mid = &intent.occurrence_key.as_ref().unwrap().fields[1].1;
        assert_eq!(
            payload_mid, key_mid,
            "payload.message_id must match occurrence_key.message_id for traceability"
        );
    }
}

/// AC #4 — Text fields carry Document privacy context (admission-layer suppression).
///
/// Raw text is present in the payload (the parser does not strip it) but is
/// tagged with ProcessingContext::Document so the admission layer can suppress
/// it per policy. This test pins that the privacy_context is set correctly
/// and that the text field actually reaches the payload.
#[tokio::test]
async fn ac4_text_tagged_document_for_admission_layer_suppression() {
    use sinex_primitives::privacy::ProcessingContext;

    let json = serde_json::json!([{
        "uuid": "priv-session",
        "name": "Privacy test",
        "chat_messages": [{
            "uuid": "priv-msg",
            "sender": "human",
            "created_at": "2025-04-01T00:00:00Z",
            "content": [{"type": "text", "text": "sensitive content here"}]
        }]
    }]);
    let bytes = serde_json::to_vec(&json).unwrap();
    let intents = ClaudeSessionParser::default()
        .parse_record(record_for(&bytes), &claude_ctx())
        .await
        .unwrap();

    let intent = &intents[0];
    assert_eq!(
        intent.privacy_context,
        ProcessingContext::Document,
        "privacy_context must be Document so admission layer can suppress text"
    );
    // Text reaches the payload (not pre-stripped by parser).
    assert_eq!(
        intent.payload["text"].as_str().unwrap(),
        "sensitive content here"
    );
}
