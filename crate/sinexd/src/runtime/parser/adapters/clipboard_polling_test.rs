use super::*;
use futures::StreamExt;
use tokio::time::{Duration, timeout};
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

fn make_adapter(snapshots: Vec<Option<String>>) -> ClipboardPollingAdapter {
    ClipboardPollingAdapter::from_backend(MockClipboardBackend::new(snapshots))
}

#[sinex_test]
async fn test_clipboard_emits_record_on_change() -> xtask::sandbox::TestResult<()> {
    let adapter = make_adapter(vec![
        Some("hello".into()),
        None, // empty → skip
        Some("world".into()),
    ]);
    let config = ClipboardPollingConfig {
        poll_interval_ms: 1,
        max_content_bytes: 1024,
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(2).collect())
        .await
        .unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].as_ref().unwrap().bytes, b"hello");
    assert_eq!(records[1].as_ref().unwrap().bytes, b"world");
    Ok(())
}

#[sinex_test]
async fn test_clipboard_deduplicates_unchanged_content() -> xtask::sandbox::TestResult<()> {
    let adapter = make_adapter(vec![
        Some("same".into()),
        Some("same".into()),
        Some("same".into()),
        Some("different".into()),
    ]);
    let config = ClipboardPollingConfig {
        poll_interval_ms: 1,
        max_content_bytes: 1024,
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(2).collect())
        .await
        .unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].as_ref().unwrap().bytes, b"same");
    assert_eq!(records[1].as_ref().unwrap().bytes, b"different");
    Ok(())
}

#[sinex_test]
async fn test_clipboard_skips_oversized_content() -> xtask::sandbox::TestResult<()> {
    let big = "x".repeat(10);
    let adapter = make_adapter(vec![Some(big.clone()), Some("small".into())]);
    let config = ClipboardPollingConfig {
        poll_interval_ms: 1,
        max_content_bytes: 5, // big will be dropped, small will pass
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(1).collect())
        .await
        .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].as_ref().unwrap().bytes, b"small");
    Ok(())
}

#[sinex_test]
async fn test_clipboard_anchor_is_stream_frame() -> xtask::sandbox::TestResult<()> {
    let adapter = make_adapter(vec![Some("text".into())]);
    let config = ClipboardPollingConfig {
        poll_interval_ms: 1,
        max_content_bytes: 1024,
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(1).collect())
        .await
        .unwrap();

    assert!(matches!(
        records[0].as_ref().unwrap().anchor,
        MaterialAnchor::StreamFrame { frame_index: 0, .. }
    ));
    Ok(())
}

#[sinex_test]
async fn test_clipboard_change_counter_monotonic() -> xtask::sandbox::TestResult<()> {
    let adapter = make_adapter(vec![Some("a".into()), Some("b".into()), Some("c".into())]);
    let config = ClipboardPollingConfig {
        poll_interval_ms: 1,
        max_content_bytes: 1024,
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = timeout(Duration::from_secs(2), stream.take(3).collect())
        .await
        .unwrap();

    let indices: Vec<u64> = records
        .iter()
        .map(|r| match &r.as_ref().unwrap().anchor {
            MaterialAnchor::StreamFrame { frame_index, .. } => *frame_index,
            _ => panic!("wrong anchor"),
        })
        .collect();

    assert_eq!(indices, vec![0, 1, 2]);
    Ok(())
}

#[sinex_test]
async fn test_clipboard_cursor_after_is_unit() -> xtask::sandbox::TestResult<()> {
    let adapter = make_adapter(vec![]);
    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index: 0,
        },
        bytes: b"x".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let cursor = adapter.cursor_after(&record).unwrap();
    assert_eq!(cursor, ClipboardPollingCursor);
    Ok(())
}

#[sinex_test]
async fn test_kind_is_polling() -> xtask::sandbox::TestResult<()> {
    assert_eq!(ClipboardPollingAdapter::KIND, InputShapeKind::Polling);
    Ok(())
}
