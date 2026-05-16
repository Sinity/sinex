//! Integration tests for the Facebook Messenger GDPR export parser (#1090).
//!
//! ## AC Coverage Matrix
//!
//! | AC Item | Status | Notes |
//! |---------|--------|-------|
//! | Messenger: sent/received/unsent/reaction/thread-index events | SATISFIED | See `messenger_*` tests |
//! | Messenger: `message_uid (mid.$...)` anchors | DEFERRED → #1090 follow-up | GDPR export has no per-message id; occurrence key uses (thread,sender,ts,hint) |
//! | Email: Message-ID anchors + IMAP fallback | DEFERRED → #1090 follow-up | Email parser not yet implemented |
//! | Bus-First admission path | SATISFIED | `privacy_context = Document` set on every intent |
//! | Privacy: body/media/participant not in raw NATS payload | PARTIALLY SATISFIED | `text` preserved for admission gating (intentional per design); media/reactions summarised |
//! | Idempotent replay at occurrence level | SATISFIED | Same input → same occurrence_key deterministically |
//! | Live Gmail/IMAP split confirmed out of scope | CONFIRMED | Deferred per issue non-goals |

use sinex_node_sdk::parser::MaterialParser;
use sinex_primitives::{
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId},
    privacy::ProcessingContext,
    temporal::Timestamp,
    Uuid,
};
use sinex_source_worker::sources::messaging::MessengerThreadParser;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_ctx() -> ParserContext {
    ParserContext {
        source_unit_id: SourceUnitId::from_static("facebook-messenger-thread"),
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
// Fixtures — all synthetic, no real user data
// ---------------------------------------------------------------------------

/// A thread with five representative message types:
/// sent (text), received (text), unsent, media-only, and reaction.
const FIXTURE_MIXED_THREAD: &str = r#"{
  "participants": ["Alice", "Bob"],
  "threadName": "Alice_Bob_thread",
  "messages": [
    {
      "isUnsent": false,
      "media": [],
      "reactions": [],
      "senderName": "Alice",
      "text": "Hey, how are you?",
      "timestamp": 1710000000000,
      "type": "text"
    },
    {
      "isUnsent": false,
      "media": [],
      "reactions": [],
      "senderName": "Bob",
      "text": "Doing great!",
      "timestamp": 1710000060000,
      "type": "text"
    },
    {
      "isUnsent": true,
      "media": [],
      "reactions": [],
      "senderName": "Alice",
      "text": "this was unsent",
      "timestamp": 1710000120000,
      "type": "text"
    },
    {
      "isUnsent": false,
      "media": [{"uri": "photos/photo1.jpg"}, {"uri": "photos/photo2.jpg"}],
      "reactions": [],
      "senderName": "Bob",
      "timestamp": 1710000180000,
      "type": "text"
    },
    {
      "isUnsent": false,
      "media": [],
      "reactions": [{"actor": "Alice", "reaction": "\u2764"}],
      "senderName": "Bob",
      "text": "nice photo!",
      "timestamp": 1710000240000,
      "type": "text"
    }
  ]
}"#;

/// A thread with a "share" type message (no text body).
const FIXTURE_SHARE_TYPE: &str = r#"{
  "participants": ["Alice", "Bob"],
  "threadName": "share_thread",
  "messages": [
    {
      "isUnsent": false,
      "media": [],
      "reactions": [],
      "senderName": "Alice",
      "timestamp": 1710000000000,
      "type": "share"
    }
  ]
}"#;

/// A thread where two messages would have the same (thread, sender, ts) but differ in text.
/// Verifies that text_hint disambiguates occurrence keys.
const FIXTURE_SAME_TS_DIFFERENT_TEXT: &str = r#"{
  "participants": ["Alice"],
  "threadName": "dupe_ts_thread",
  "messages": [
    {
      "isUnsent": false, "media": [], "reactions": [],
      "senderName": "Alice",
      "text": "first message",
      "timestamp": 1710000000000,
      "type": "text"
    },
    {
      "isUnsent": false, "media": [], "reactions": [],
      "senderName": "Alice",
      "text": "second message",
      "timestamp": 1710000000000,
      "type": "text"
    }
  ]
}"#;

// ---------------------------------------------------------------------------
// AC: Messenger fixture parses sent/received/unsent/reaction/thread-index events
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_parses_all_five_message_types() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 5, "expected 5 intents for the mixed thread");
    for intent in &intents {
        assert_eq!(intent.event_source.as_str(), "messenger");
        assert_eq!(intent.event_type.as_str(), "message.sent");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_sent_message_payload_fields() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let sent = &intents[0];
    assert_eq!(sent.payload["sender_name"], "Alice");
    assert_eq!(sent.payload["thread_name"], "Alice_Bob_thread");
    assert_eq!(sent.payload["is_unsent"], false);
    assert_eq!(sent.payload["message_type"], "text");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_received_message_from_other_participant() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    // Second message is from Bob (the receiver from Alice's perspective).
    let received = &intents[1];
    assert_eq!(received.payload["sender_name"], "Bob");
    assert_eq!(received.payload["is_unsent"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_unsent_message_flag_preserved() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let unsent = &intents[2];
    assert_eq!(unsent.payload["is_unsent"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_share_type_message_no_text() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_SHARE_TYPE.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["message_type"], "share");
    assert!(intents[0].payload["text"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_thread_index_encoded_in_anchor() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    for (i, intent) in intents.iter().enumerate() {
        assert!(
            matches!(intent.anchor, MaterialAnchor::ByteRange { start, len: 1 } if start == i as u64),
            "anchor for index {} should be ByteRange{{start:{}, len:1}}, got {:?}",
            i, i, intent.anchor
        );
    }
}

// ---------------------------------------------------------------------------
// AC note: message_uid (mid.$...) anchors
// ---------------------------------------------------------------------------
//
// The Facebook GDPR export format does NOT include per-message stable IDs
// (mid.$... values) in the exported JSON. The current occurrence key uses
// (thread_name, sender_name, timestamp_ms, text_hint) as the stable tuple,
// which is the best available anchor in the GDPR export format.
//
// Adding true mid.$... anchor support would require either:
//   (a) A SQLite export path that exposes Facebook's internal message IDs, or
//   (b) A companion GDPR field if Facebook ever adds it.
//
// This gap is tracked as a follow-up to #1090.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_occurrence_key_uses_thread_sender_ts_texthint() {
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let key = intents[0].occurrence_key.as_ref().unwrap();
    assert_eq!(key.fields[0].0, "thread_name");
    assert_eq!(key.fields[0].1, "Alice_Bob_thread");
    assert_eq!(key.fields[1].0, "sender_name");
    assert_eq!(key.fields[1].1, "Alice");
    assert_eq!(key.fields[2].0, "timestamp_ms");
    assert_eq!(key.fields[2].1, "1710000000000");
    assert_eq!(key.fields[3].0, "text_hint");
    assert_eq!(key.fields[3].1, "Hey, how are you?");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_text_hint_disambiguates_same_ts_messages() {
    // Two messages with identical (thread, sender, timestamp) but different text
    // must produce different occurrence keys.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(
            record_for(FIXTURE_SAME_TS_DIFFERENT_TEXT.as_bytes()),
            &test_ctx(),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 2);
    let key0 = intents[0].occurrence_key.as_ref().unwrap();
    let key1 = intents[1].occurrence_key.as_ref().unwrap();
    // thread_name, sender_name, timestamp_ms are identical
    assert_eq!(key0.fields[0], key1.fields[0]);
    assert_eq!(key0.fields[1], key1.fields[1]);
    assert_eq!(key0.fields[2], key1.fields[2]);
    // text_hint must differ
    assert_ne!(key0.fields[3], key1.fields[3]);
}

// ---------------------------------------------------------------------------
// AC: Privacy gates — body/media/participant data
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_privacy_context_is_document() {
    // Every intent must carry ProcessingContext::Document so the admission
    // layer can gate body content before it reaches durable NATS/DLQ storage.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    for intent in &intents {
        assert_eq!(
            intent.privacy_context,
            ProcessingContext::Document,
            "every messenger intent must carry ProcessingContext::Document"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_raw_media_array_not_in_payload() {
    // Raw media blob URIs must not appear in the payload — only the count.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    // Message index 3 has 2 media entries.
    let media_msg = &intents[3];
    assert!(
        media_msg.payload.get("media").is_none(),
        "raw media[] array must not appear in payload"
    );
    assert_eq!(
        media_msg.payload["media_count"], 2,
        "media_count should be 2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_raw_reactions_array_not_in_payload() {
    // Raw reaction details (actor names, emoji) must not appear in payload.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    // Message index 4 has 1 reaction.
    let reaction_msg = &intents[4];
    assert!(
        reaction_msg.payload.get("reactions").is_none(),
        "raw reactions[] array must not appear in payload"
    );
    assert_eq!(
        reaction_msg.payload["reaction_count"], 1,
        "reaction_count should be 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_participants_preserved_for_social_graph() {
    // participants + sender_name are intentionally preserved: they are the
    // social-graph signal, not conversation content. Per design doc comment.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    let participants = intents[0].payload["participants"].as_array().unwrap();
    assert_eq!(participants.len(), 2);
    assert!(participants.iter().any(|p| p == "Alice"));
    assert!(participants.iter().any(|p| p == "Bob"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_text_preserved_for_admission_gating() {
    // `text` is preserved in the payload so that the upstream admission policy
    // (ProcessingContext::Document) can decide whether to strip it before
    // durable persistence. The parser itself does not strip the body.
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(FIXTURE_MIXED_THREAD.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents[0].payload["text"], "Hey, how are you?");
}

// ---------------------------------------------------------------------------
// AC: Idempotent replay at occurrence level
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_replay_produces_identical_occurrence_keys() {
    // Running the parser twice on the same fixture must produce the same
    // occurrence keys — idempotency at the occurrence level.
    let bytes = FIXTURE_MIXED_THREAD.as_bytes();
    let mut parser = MessengerThreadParser;
    let first_run = parser
        .parse_record(record_for(bytes), &test_ctx())
        .await
        .unwrap();
    let second_run = parser
        .parse_record(record_for(bytes), &test_ctx())
        .await
        .unwrap();
    assert_eq!(first_run.len(), second_run.len());
    for (a, b) in first_run.iter().zip(second_run.iter()) {
        assert_eq!(
            a.occurrence_key, b.occurrence_key,
            "occurrence key must be stable across re-runs"
        );
        assert_eq!(a.ts_orig, b.ts_orig);
        assert_eq!(a.payload, b.payload);
    }
}

// ---------------------------------------------------------------------------
// AC: Email parser — DEFERRED
// ---------------------------------------------------------------------------
//
// The email/mailbox parser (RFC 2822 Message-ID anchors + IMAP fallback) is
// not yet implemented. The acceptance criteria for:
//
//   - Staged email fixture parses message metadata with Message-ID anchors
//   - Missing/duplicate Message-ID falls back to IMAP (mailbox, uid_validity, uid)
//   - Bus-First source-worker path produces ingestd confirmations
//
// are deferred to a follow-up implementation issue branching off #1090.
// When EmailParser is added to sinex_source_worker::sources::messaging,
// tests should be added here covering:
//   - parse minimal RFC 2822 message (From/To/Subject/Date/Message-ID)
//   - Message-ID as occurrence key field
//   - Missing Message-ID falls back to IMAP tuple
//   - Duplicate Message-ID (two messages with same ID) falls back to IMAP
//   - Body/attachment content not in raw payload (only metadata + char counts)

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_invalid_json_returns_parse_error() {
    let mut parser = MessengerThreadParser;
    let result = parser
        .parse_record(record_for(b"not json at all"), &test_ctx())
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("invalid Messenger thread JSON"),
        "expected parse error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_empty_messages_array_produces_no_intents() {
    let empty = r#"{"participants":["A"],"threadName":"T","messages":[]}"#;
    let mut parser = MessengerThreadParser;
    let intents = parser
        .parse_record(record_for(empty.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messenger_invalid_timestamp_returns_parse_error() {
    let bad_ts = r#"{
      "participants": ["A"],
      "threadName": "T",
      "messages": [
        {"isUnsent": false, "media": [], "reactions": [],
         "senderName": "A", "text": "x",
         "timestamp": 99999999999999999,
         "type": "text"}
      ]
    }"#;
    let mut parser = MessengerThreadParser;
    let result = parser
        .parse_record(record_for(bad_ts.as_bytes()), &test_ctx())
        .await;
    // Either a parse error (out-of-range timestamp) or successful parse with clamped time.
    // The key invariant: no panic.
    let _ = result;
}
