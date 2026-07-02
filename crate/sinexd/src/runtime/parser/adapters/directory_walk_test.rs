use super::*;
use futures::StreamExt;
use std::io::Write;
use tempfile::TempDir;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

fn simple_config(roots: Vec<Utf8PathBuf>) -> DirectoryWalkConfig {
    DirectoryWalkConfig {
        roots,
        globs: vec![],
        follow_symlinks: false,
        max_depth: None,
    }
}

async fn collect_records(
    adapter: &DirectoryWalkAdapter,
    config: &DirectoryWalkConfig,
    cursor: Option<DirectoryWalkCursor>,
) -> Vec<SourceRecord> {
    let stream = adapter
        .open(dummy_material_id(), config, cursor)
        .await
        .unwrap();
    stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect()
}

// -------------------------------------------------------------------------

#[sinex_test]
async fn test_empty_directory_yields_zero_records() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let records = collect_records(&adapter, &config, None).await;
    assert_eq!(records.len(), 0);
    Ok(())
}

#[sinex_test]
async fn test_walk_emits_record_per_file() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

    for name in &["a.txt", "b.txt", "c.txt"] {
        let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
        write!(f, "content of {name}").unwrap();
    }

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let records = collect_records(&adapter, &config, None).await;

    assert_eq!(records.len(), 3);
    // Records are emitted in sorted path order.
    let paths: Vec<String> = records
        .iter()
        .map(|r| {
            r.logical_path
                .as_ref()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(paths, vec!["a.txt", "b.txt", "c.txt"]);
    Ok(())
}

#[sinex_test]
async fn test_cursor_based_dedup_skips_unchanged_files() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

    let file_path = dir.path().join("file.txt");
    let mut f = std::fs::File::create(&file_path).unwrap();
    write!(f, "initial").unwrap();
    drop(f);

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root.clone()]);

    // First walk: file is new, so it is emitted.
    let records = collect_records(&adapter, &config, None).await;
    assert_eq!(records.len(), 1);

    // Build a cursor that matches the current fingerprint.
    let meta = std::fs::metadata(&file_path).unwrap();
    let fp = DirectoryWalkAdapter::fingerprint(&meta);
    let utf8_path = Utf8PathBuf::from_path_buf(file_path.clone()).unwrap();
    let mut cursor = DirectoryWalkCursor::default();
    cursor.insert(utf8_path.clone(), fp);

    // Second walk with matching cursor: file should be skipped.
    let records2 = collect_records(&adapter, &config, Some(cursor)).await;
    assert_eq!(records2.len(), 0, "unchanged file should be deduped");

    // Modify the file (change content to change size).
    let mut f2 = std::fs::File::create(&file_path).unwrap();
    write!(f2, "modified content that is longer").unwrap();
    drop(f2);

    // Build cursor with old fingerprint (size mismatch now).
    let mut stale_cursor = DirectoryWalkCursor::default();
    stale_cursor.insert(utf8_path, fp);

    // Third walk: fingerprint changed, file should be re-emitted.
    let records3 = collect_records(&adapter, &config, Some(stale_cursor)).await;
    assert_eq!(records3.len(), 1, "modified file should be re-emitted");
    Ok(())
}

#[sinex_test]
async fn test_glob_filter_restricts_emission() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

    for name in &["doc.md", "data.json", "script.sh"] {
        let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
        write!(f, "x").unwrap();
    }

    let adapter = DirectoryWalkAdapter;
    let config = DirectoryWalkConfig {
        roots: vec![root],
        globs: vec!["**/*.md".into()],
        follow_symlinks: false,
        max_depth: None,
    };

    let records = collect_records(&adapter, &config, None).await;
    assert_eq!(records.len(), 1);
    assert!(
        records[0]
            .logical_path
            .as_ref()
            .unwrap()
            .as_str()
            .ends_with("doc.md")
    );
    Ok(())
}

#[sinex_test]
async fn test_max_depth_bounds_recursion() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

    // Create: root/top.txt, root/sub/nested.txt
    let mut f = std::fs::File::create(dir.path().join("top.txt")).unwrap();
    write!(f, "top").unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let mut f2 = std::fs::File::create(sub.join("nested.txt")).unwrap();
    write!(f2, "nested").unwrap();

    let adapter = DirectoryWalkAdapter;

    // max_depth=0 → only files directly in root (no recursion into sub/).
    let config_shallow = DirectoryWalkConfig {
        roots: vec![root.clone()],
        globs: vec![],
        follow_symlinks: false,
        max_depth: Some(0),
    };
    let records_shallow = collect_records(&adapter, &config_shallow, None).await;
    assert_eq!(records_shallow.len(), 1, "only top.txt at depth 0");
    assert!(
        records_shallow[0]
            .logical_path
            .as_ref()
            .unwrap()
            .as_str()
            .ends_with("top.txt")
    );

    // max_depth=1 → includes sub/nested.txt.
    let config_deep = DirectoryWalkConfig {
        roots: vec![root],
        globs: vec![],
        follow_symlinks: false,
        max_depth: Some(1),
    };
    let records_deep = collect_records(&adapter, &config_deep, None).await;
    assert_eq!(
        records_deep.len(),
        2,
        "both top.txt and nested.txt at depth 1"
    );
    Ok(())
}

#[sinex_test]
async fn test_input_fingerprint_reports_directory_manifest_shape()
-> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();

    let mut csv = std::fs::File::create(dir.path().join("events.csv")).unwrap();
    write!(csv, "id,name\n1,Alice").unwrap();
    let mut json = std::fs::File::create(sub.join("profile.JSON")).unwrap();
    write!(json, "{{\"id\":1}}").unwrap();
    let mut jsonl = std::fs::File::create(sub.join("events.jsonl")).unwrap();
    writeln!(jsonl, "{{\"event_id\":1}}").unwrap();

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let fingerprint = adapter.input_fingerprint(&config)?.unwrap();

    assert_eq!(fingerprint.format, "directory_manifest");
    assert_eq!(
        fingerprint.keys,
        vec!["events.csv", "sub/events.jsonl", "sub/profile.JSON"]
    );
    assert!(
        fingerprint
            .type_map
            .get("events.csv")
            .is_some_and(|kind| kind.starts_with("extension:csv;shape:"))
    );
    assert!(
        fingerprint
            .type_map
            .get("sub/profile.JSON")
            .is_some_and(|kind| kind.starts_with("extension:json;shape:"))
    );
    assert!(
        fingerprint
            .type_map
            .get("sub/events.jsonl")
            .is_some_and(|kind| kind.starts_with("extension:jsonl;shape:"))
    );
    Ok(())
}

#[sinex_test]
async fn test_input_fingerprint_hash_changes_when_file_set_changes()
-> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let mut first = std::fs::File::create(dir.path().join("events.csv")).unwrap();
    write!(first, "id,name\n1,Alice").unwrap();

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let before = adapter.input_fingerprint(&config)?.unwrap();

    let mut second = std::fs::File::create(dir.path().join("events.json")).unwrap();
    write!(second, "{{\"id\":1}}").unwrap();
    let after = adapter.input_fingerprint(&config)?.unwrap();

    assert_ne!(before.hash(), after.hash());
    assert!(after.keys.contains(&"events.csv".to_string()));
    assert!(after.keys.contains(&"events.json".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_input_fingerprint_hash_changes_when_structured_child_shape_changes()
-> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let csv_path = dir.path().join("events.csv");
    let mut first = std::fs::File::create(&csv_path).unwrap();
    write!(first, "id,name\n1,Alice").unwrap();
    drop(first);

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let before = adapter.input_fingerprint(&config)?.unwrap();

    let mut second = std::fs::File::create(&csv_path).unwrap();
    write!(second, "id,display_name,active\n1,Alice,true").unwrap();
    drop(second);
    let after = adapter.input_fingerprint(&config)?.unwrap();

    assert_eq!(before.keys, after.keys);
    assert_ne!(before.hash(), after.hash());
    assert_ne!(before.type_map["events.csv"], after.type_map["events.csv"]);
    Ok(())
}

#[sinex_test]
async fn test_anchor_is_directory_entry() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let mut f = std::fs::File::create(dir.path().join("file.txt")).unwrap();
    write!(f, "hello").unwrap();

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let records = collect_records(&adapter, &config, None).await;

    assert_eq!(records.len(), 1);
    assert!(matches!(
        &records[0].anchor,
        MaterialAnchor::DirectoryEntry {
            path: _,
            content_hash: None
        }
    ));
    Ok(())
}

#[sinex_test]
async fn test_non_existent_root_is_silently_skipped() -> xtask::sandbox::TestResult<()> {
    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![Utf8PathBuf::from(
        "/nonexistent/dir/that/does/not/exist",
    )]);
    let records = collect_records(&adapter, &config, None).await;
    assert_eq!(records.len(), 0);
    Ok(())
}
