use super::*;
use xtask::sandbox::prelude::sinex_test;

use sinex_primitives::Uuid;
use sinex_primitives::parser::MaterialAnchor;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("weechat"),
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
