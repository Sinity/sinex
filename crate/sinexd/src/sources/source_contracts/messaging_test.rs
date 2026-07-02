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
