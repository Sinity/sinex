use super::*;
use std::io::Write;
use tempfile::NamedTempFile;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

#[sinex_test]
async fn test_append_only_yields_one_record_per_line() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "line1").unwrap();
    writeln!(f, "line2").unwrap();
    writeln!(f, "line3").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path,
        skip_empty: false,
    };
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    assert_eq!(records.len(), 3);
    assert_eq!(records[0].as_ref().unwrap().bytes, b"line1");
    assert_eq!(records[1].as_ref().unwrap().bytes, b"line2");
    assert_eq!(records[2].as_ref().unwrap().bytes, b"line3");
    Ok(())
}

#[sinex_test]
async fn test_append_only_skip_empty_lines() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "first").unwrap();
    writeln!(f).unwrap();
    writeln!(f, "second").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path,
        skip_empty: true,
    };
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    assert_eq!(records.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_append_only_resume_from_cursor() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "line1").unwrap();
    writeln!(f, "line2").unwrap();
    writeln!(f, "line3").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path: path.clone(),
        skip_empty: false,
    };

    // First pass to get cursor for line 2
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;
    let cursor_after_line2 = adapter.cursor_after(records[1].as_ref().unwrap()).unwrap();

    // Resume: should only yield line3
    let stream2 = adapter
        .open(dummy_material_id(), &config, Some(cursor_after_line2))
        .await
        .unwrap();
    let records2: Vec<_> = stream2.collect().await;

    assert_eq!(records2.len(), 1);
    assert_eq!(records2[0].as_ref().unwrap().bytes, b"line3");
    Ok(())
}

#[sinex_test]
async fn test_append_only_line_anchor() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "hello").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path,
        skip_empty: false,
    };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let record = stream.next().await.unwrap().unwrap();

    assert!(matches!(
        record.anchor,
        MaterialAnchor::Line { line: 1, .. }
    ));
    Ok(())
}

#[sinex_test]
async fn test_append_only_cursor_after_wrong_anchor_errors() -> xtask::sandbox::TestResult<()> {
    let adapter = AppendOnlyFileAdapter;
    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::ByteRange { start: 0, len: 10 },
        bytes: b"x".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    assert!(adapter.cursor_after(&record).is_err());
    Ok(())
}

#[sinex_test]
async fn test_append_only_missing_file_returns_error() -> xtask::sandbox::TestResult<()> {
    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path: "/nonexistent/file.log".into(),
        skip_empty: false,
    };
    let stream = adapter.open(dummy_material_id(), &config, None).await?;
    let records: Vec<_> = stream.collect().await;
    assert!(
        records.is_empty(),
        "missing append-only files are optional sources and should yield no records"
    );
    Ok(())
}

#[sinex_test]
async fn test_append_only_records_carry_inode_when_unix() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "x").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path,
        skip_empty: false,
    };
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    let rec = records[0].as_ref().unwrap();
    if cfg!(unix) {
        // Inode should be embedded in metadata on every record so
        // cursor_after can round-trip it.
        let ino = rec
            .metadata
            .get(ADAPTER_INODE_KEY)
            .and_then(serde_json::Value::as_u64);
        assert!(ino.is_some(), "expected inode in metadata on unix");
    }
    Ok(())
}

#[sinex_test]
async fn test_append_only_rotation_resets_offsets() -> xtask::sandbox::TestResult<()> {
    // Two distinct temp files act as "before rotation" and "after rotation".
    // The cursor returned from scanning the first file carries the first
    // file's inode; supplying it to a scan of the second file (different
    // inode) MUST cause the adapter to reset to offset 0 and tag the first
    // emitted record with rotation metadata.
    let mut f1 = NamedTempFile::new().unwrap();
    writeln!(f1, "old-line-1").unwrap();
    writeln!(f1, "old-line-2").unwrap();
    let path1 = f1.path().to_str().unwrap().to_string();

    let mut f2 = NamedTempFile::new().unwrap();
    writeln!(f2, "new-line-1").unwrap();
    writeln!(f2, "new-line-2").unwrap();
    let path2 = f2.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;

    // Scan f1 fully, capture cursor.
    let cfg1 = AppendOnlyFileConfig {
        path: path1,
        skip_empty: false,
    };
    let stream1 = adapter
        .open(dummy_material_id(), &cfg1, None)
        .await
        .unwrap();
    let records1: Vec<_> = stream1.collect().await;
    let cursor1 = adapter
        .cursor_after(records1.last().unwrap().as_ref().unwrap())
        .unwrap();
    if cfg!(unix) {
        assert!(cursor1.inode.is_some(), "f1 cursor must capture inode");
    }

    // Resume against f2 using f1's cursor (offsets non-zero, inode different).
    let cfg2 = AppendOnlyFileConfig {
        path: path2,
        skip_empty: false,
    };
    let stream2 = adapter
        .open(dummy_material_id(), &cfg2, Some(cursor1.clone()))
        .await
        .unwrap();
    let records2: Vec<_> = stream2.collect().await;

    // On unix the rotation is detected: both new lines are emitted from
    // offset 0 with rotation metadata on the first. Without unix support
    // (no inode), the adapter falls back to inheriting offsets — which
    // would emit zero records because f2 is shorter than cursor1.offset.
    if cfg!(unix) {
        assert_eq!(
            records2.len(),
            2,
            "rotation should reset and emit all of f2"
        );
        let first_meta = &records2[0].as_ref().unwrap().metadata;
        assert_eq!(
            first_meta
                .get("rotation_detected")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "first post-rotation record must carry rotation_detected: true"
        );
        assert!(
            first_meta
                .get("previous_inode")
                .and_then(serde_json::Value::as_u64)
                .is_some(),
            "rotation marker must include previous_inode"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_append_only_empty_file_yields_no_records() -> xtask::sandbox::TestResult<()> {
    let f = NamedTempFile::new().unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = AppendOnlyFileAdapter;
    let config = AppendOnlyFileConfig {
        path,
        skip_empty: false,
    };
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    assert!(records.is_empty());
    Ok(())
}
