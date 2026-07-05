use super::*;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

const JOURNAL_LINE_WITH_CURSOR: &str =
    r#"{"__CURSOR":"s=abc;i=1;b=x","MESSAGE":"hello","PRIORITY":"6"}"#;
const JOURNAL_LINE_NO_CURSOR: &str = r#"{"MESSAGE":"no cursor here","PRIORITY":"6"}"#;

#[sinex_test]
async fn test_records_from_lines_happy_path() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
    assert_eq!(records.len(), 1);
    assert!(records[0].is_ok());
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_extracts_cursor_field() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let records = records_from_journal_lines(mid, &[JOURNAL_LINE_WITH_CURSOR]);
    let record = records[0].as_ref().unwrap();

    let adapter = JournalctlStreamAdapter;
    let cursor = adapter.cursor_after(record).unwrap();
    assert_eq!(cursor.cursor, "s=abc;i=1;b=x");
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_missing_cursor_is_not_checkpointable() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let records = records_from_journal_lines(mid, &[JOURNAL_LINE_NO_CURSOR]);
    let record = records[0].as_ref().unwrap();

    let adapter = JournalctlStreamAdapter;
    let error = adapter.cursor_after(record).unwrap_err();
    assert!(format!("{error}").contains("has no __CURSOR"));
    assert!(!format!("{error}").contains("frame:"));
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_non_json_errors() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let record = SourceRecord {
        material_id: mid,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: b"not json at all".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };

    let adapter = JournalctlStreamAdapter;
    assert!(adapter.cursor_after(&record).is_err());
    Ok(())
}

#[sinex_test]
async fn test_records_skips_empty_lines() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let records = records_from_journal_lines(mid, &["", JOURNAL_LINE_WITH_CURSOR, ""]);
    assert_eq!(records.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_kind_is_subprocess() -> xtask::sandbox::TestResult<()> {
    assert_eq!(JournalctlStreamAdapter::KIND, InputShapeKind::Subprocess);
    Ok(())
}

#[sinex_test]
async fn test_multiple_lines_have_monotonic_frame_indices() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let lines = [JOURNAL_LINE_WITH_CURSOR, JOURNAL_LINE_NO_CURSOR];
    let records = records_from_journal_lines(mid, &lines);
    let indices: Vec<u64> = records
        .iter()
        .map(|r| match &r.as_ref().unwrap().anchor {
            MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
            _ => panic!("unexpected anchor"),
        })
        .collect();
    for w in indices.windows(2) {
        assert!(w[0] < w[1]);
    }
    Ok(())
}

#[sinex_test]
async fn test_cursor_serde_roundtrip() -> xtask::sandbox::TestResult<()> {
    let cursor = JournalctlCursor::new("s=abc;i=42;b=deadbeef");
    let json = serde_json::to_string(&cursor).unwrap();
    let back: JournalctlCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(cursor, back);
    Ok(())
}

#[sinex_test]
async fn journalctl_args_default_without_cursor_preserves_historical_import()
-> xtask::sandbox::TestResult<()> {
    let args = journalctl_args(
        &JournalctlStreamConfig {
            units: vec!["sinexd.service".to_string()],
            priority: Some(6),
            from_cursor: None,
            start_at_now_without_cursor: false,
        },
        None,
    );

    assert!(!args.contains(&"--since=now".to_string()));
    assert!(!args.iter().any(|arg| arg.starts_with("--after-cursor=")));
    assert!(args.contains(&"--unit=sinexd.service".to_string()));
    assert!(args.contains(&"--priority=6".to_string()));
    Ok(())
}

#[sinex_test]
async fn journalctl_args_start_at_now_when_requested() -> xtask::sandbox::TestResult<()> {
    let args = journalctl_args(
        &JournalctlStreamConfig {
            units: Vec::new(),
            priority: None,
            from_cursor: None,
            start_at_now_without_cursor: true,
        },
        None,
    );

    assert!(args.contains(&"--since=now".to_string()));
    assert!(!args.iter().any(|arg| arg.starts_with("--after-cursor=")));
    Ok(())
}

#[sinex_test]
async fn journalctl_args_resume_after_runtime_cursor() -> xtask::sandbox::TestResult<()> {
    let args = journalctl_args(
        &JournalctlStreamConfig {
            units: Vec::new(),
            priority: None,
            from_cursor: Some("config-cursor".to_string()),
            start_at_now_without_cursor: true,
        },
        Some(&JournalctlCursor::new("runtime-cursor")),
    );

    assert!(args.contains(&"--after-cursor=runtime-cursor".to_string()));
    assert!(!args.contains(&"--since=now".to_string()));
    assert!(!args.contains(&"--after-cursor=config-cursor".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_non_stream_frame_anchor_errors() -> xtask::sandbox::TestResult<()> {
    // Cover the fallback Err arm: record has no __CURSOR field AND its
    // anchor is not StreamFrame. This pins the contract that the only
    // anchors journalctl can survive without a __CURSOR are stream frames.
    let adapter = JournalctlStreamAdapter;
    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::SqliteRow {
            table: "fake".into(),
            rowid: 1,
        },
        bytes: b"{\"MESSAGE\":\"hi\"}".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let err = adapter.cursor_after(&record);
    assert!(matches!(err, Err(ParserError::Cursor(_))));
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_invalid_json_errors() -> xtask::sandbox::TestResult<()> {
    let adapter = JournalctlStreamAdapter;
    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 1,
        },
        bytes: b"not-json".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let err = adapter.cursor_after(&record);
    assert!(matches!(err, Err(ParserError::Cursor(_))));
    Ok(())
}

// =========================================================================
// SharedJournalctlStream structural tests
//
// These tests do NOT spawn a real journalctl process.  Instead, they drive
// the broadcast channel directly to verify subscriber routing semantics.
// =========================================================================

/// Build a `SourceRecord` from raw bytes — minimal helper for shared tests.
fn make_record_bytes(bytes: &[u8]) -> SourceRecord {
    SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: bytes.to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

/// Drive records into a broadcast sender and collect what a subscriber
/// with a given filter receives.
async fn drive_and_collect(
    tx: broadcast::Sender<SourceRecord>,
    subscriber: super::JournalctlSubscriber,
    records: Vec<SourceRecord>,
) -> Vec<Vec<u8>> {
    use futures::StreamExt;
    // Spawn a task that sends records then drops the tx.
    let sender_task = tokio::spawn(async move {
        for rec in records {
            let _ = tx.send(rec);
        }
        // tx dropped here — closes the channel.
    });

    // Collect from subscriber stream until it ends.
    let mut stream = std::pin::pin!(subscriber.into_stream());
    let mut received = Vec::new();
    while let Some(item) = stream.next().await {
        if let Ok(rec) = item {
            received.push(rec.bytes.clone());
        }
    }
    sender_task.await.unwrap();
    received
}

#[sinex_test]
async fn test_subscriber_filter_passes_matching_records() -> xtask::sandbox::TestResult<()> {
    let (tx, rx_primary) = broadcast::channel::<SourceRecord>(64);

    // Filter: only records whose bytes start with b"MATCH"
    let subscriber = super::JournalctlSubscriber {
        receiver: rx_primary,
        filter: Box::new(|rec: &SourceRecord| rec.bytes.starts_with(b"MATCH")),
    };

    let records = vec![
        make_record_bytes(b"MATCH_1"),
        make_record_bytes(b"SKIP_1"),
        make_record_bytes(b"MATCH_2"),
        make_record_bytes(b"SKIP_2"),
    ];

    let received = drive_and_collect(tx, subscriber, records).await;
    assert_eq!(
        received.len(),
        2,
        "expected 2 matching records, got {}",
        received.len()
    );
    assert!(received[0].starts_with(b"MATCH"));
    assert!(received[1].starts_with(b"MATCH"));
    Ok(())
}

#[sinex_test]
async fn test_two_subscribers_receive_independently() -> xtask::sandbox::TestResult<()> {
    use futures::StreamExt;

    let (tx, _) = broadcast::channel::<SourceRecord>(64);

    // subscriber A: only "A" records
    let sub_a = super::JournalctlSubscriber {
        receiver: tx.subscribe(),
        filter: Box::new(|r: &SourceRecord| r.bytes.starts_with(b"A")),
    };
    // subscriber B: only "B" records
    let sub_b = super::JournalctlSubscriber {
        receiver: tx.subscribe(),
        filter: Box::new(|r: &SourceRecord| r.bytes.starts_with(b"B")),
    };

    let records = vec![
        make_record_bytes(b"A1"),
        make_record_bytes(b"B1"),
        make_record_bytes(b"A2"),
        make_record_bytes(b"B2"),
        make_record_bytes(b"C1"), // neither
    ];

    // Collect both subscribers concurrently.
    let tx_clone = tx.clone();
    drop(tx); // release the original; subscribers hold their own receivers

    let sender_task = tokio::spawn(async move {
        for rec in records {
            let _ = tx_clone.send(rec);
        }
        // tx_clone dropped → channel closes
    });

    let stream_a = std::pin::pin!(sub_a.into_stream());
    let stream_b = std::pin::pin!(sub_b.into_stream());

    let (results_a, results_b, _) = tokio::join!(
        stream_a.collect::<Vec<_>>(),
        stream_b.collect::<Vec<_>>(),
        sender_task,
    );

    let bytes_a: Vec<_> = results_a
        .into_iter()
        .filter_map(std::result::Result::ok)
        .map(|r| r.bytes)
        .collect();
    let bytes_b: Vec<_> = results_b
        .into_iter()
        .filter_map(std::result::Result::ok)
        .map(|r| r.bytes)
        .collect();

    assert_eq!(bytes_a.len(), 2, "sub_a should get 2 records");
    assert_eq!(bytes_b.len(), 2, "sub_b should get 2 records");
    assert!(bytes_a.iter().all(|b| b.starts_with(b"A")));
    assert!(bytes_b.iter().all(|b| b.starts_with(b"B")));
    Ok(())
}

#[sinex_test]
async fn test_subscriber_kind_is_subprocess() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        super::JournalctlSubscriber::KIND,
        InputShapeKind::Subprocess
    );
    Ok(())
}

#[sinex_test]
async fn test_subscriber_cursor_after_extracts_journal_cursor() -> xtask::sandbox::TestResult<()>
{
    let (tx, rx) = broadcast::channel::<SourceRecord>(4);
    drop(tx); // no sending needed for this test
    let subscriber = super::JournalctlSubscriber {
        receiver: rx,
        filter: Box::new(|_| true),
    };

    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: JOURNAL_LINE_WITH_CURSOR.as_bytes().to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };

    let cursor = subscriber.cursor_after(&record).unwrap();
    assert_eq!(cursor.cursor, "s=abc;i=1;b=x");
    Ok(())
}

#[sinex_test]
async fn test_subscriber_open_returns_error() -> xtask::sandbox::TestResult<()> {
    let (tx, rx) = broadcast::channel::<SourceRecord>(4);
    drop(tx);
    let subscriber = super::JournalctlSubscriber {
        receiver: rx,
        filter: Box::new(|_| true),
    };
    let mid = dummy_material_id();
    let result = subscriber.open(mid, &(), None).await;
    assert!(
        result.is_err(),
        "open() must return an error — use into_stream() instead"
    );
    Ok(())
}

#[sinex_test]
async fn test_broadcast_capacity_constant_is_reasonable() -> xtask::sandbox::TestResult<()> {
    // Pin the value so changes are visible in review.
    assert_eq!(super::BROADCAST_CAPACITY, 512);
    Ok(())
}
