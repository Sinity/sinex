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

/// Exercises the actual runtime cursor path: `open()` a directory, run every
/// emitted record's anchor through `cursor_after()` (as the runtime does per
/// record), persist and merge those per-file cursors exactly like
/// `merge_cursor_update` does, then `open()` again with the persisted
/// checkpoint.
///
/// This is the path `test_cursor_based_dedup_skips_unchanged_files` above
/// does NOT cover: that test hand-builds a cursor via
/// `DirectoryWalkAdapter::fingerprint()` on live metadata, never via
/// `cursor_after()`. Reverting the `cursor_after()` fix (persisting a
/// `modified_ms: 0` sentinel again) makes this test fail: the persisted
/// cursor would never match the live fingerprint, so the second `open()`
/// would re-emit every unchanged file instead of skipping it.
#[sinex_test]
async fn test_persisted_cursor_after_skips_unchanged_files_on_reopen()
-> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();

    for name in &["a.txt", "b.txt"] {
        let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
        write!(f, "content of {name}").unwrap();
    }

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);

    // First open(): both files are new, both are emitted. Build the
    // persisted checkpoint the way the runtime does: fold each record's
    // `cursor_after()` output into the accumulated cursor (per-path merge,
    // matching `merge_cursor_json_update`'s object-key overlay).
    let stream = adapter.open(dummy_material_id(), &config, None).await?;
    let records: Vec<SourceRecord> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert_eq!(records.len(), 2, "both files are new on the first walk");

    let mut persisted = DirectoryWalkCursor::default();
    for record in &records {
        let per_file_cursor = adapter.cursor_after(record)?;
        for (path, fp) in per_file_cursor.0 {
            persisted.insert(path, fp);
        }
    }

    // Second open() with the persisted checkpoint: nothing changed on disk,
    // so both files must be skipped.
    let records2 = collect_records(&adapter, &config, Some(persisted.clone())).await;
    assert_eq!(
        records2.len(),
        0,
        "unchanged files must be skipped when reopened with a cursor built \
         from cursor_after() — a modified_ms:0 sentinel would make every \
         fingerprint disagree with the live mtime and defeat dedup"
    );

    // Sanity: a genuinely modified file is still re-emitted through the same
    // persisted-cursor path.
    let mut f = std::fs::File::create(dir.path().join("a.txt")).unwrap();
    write!(f, "modified content that is longer than before").unwrap();
    drop(f);

    let records3 = collect_records(&adapter, &config, Some(persisted)).await;
    assert_eq!(
        records3.len(),
        1,
        "modified file should be re-emitted even via the persisted-cursor path"
    );
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
async fn test_input_fingerprint_reports_directory_manifest_shape() -> xtask::sandbox::TestResult<()>
{
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

/// sinex-xtmp bug 4: `collect_paths` has no visited-node guard for symlink
/// cycles when `follow_symlinks=true` -- a cyclic symlink tree causes the
/// same real directory to be re-entered at every depth level, silently
/// over-collecting the same file once per cycle traversal rather than once.
/// Bounded with `max_depth` here (a genuinely unbounded cycle would hang
/// the test runner) -- even bounded, cycle-safe traversal should still find
/// `target.txt` exactly once, not once per depth level.
#[sinex_test]
async fn test_symlink_cycle_does_not_over_collect_the_same_file() -> xtask::sandbox::TestResult<()>
{
    let dir = TempDir::new().unwrap();
    let loop_dir = dir.path().join("loop");
    std::fs::create_dir(&loop_dir).unwrap();
    std::fs::write(loop_dir.join("target.txt"), b"content").unwrap();
    // A symlink inside `loop/` pointing back at `loop/` itself -- following
    // it re-enters the same directory contents indefinitely.
    std::os::unix::fs::symlink(&loop_dir, loop_dir.join("self")).unwrap();

    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let adapter = DirectoryWalkAdapter;
    let config = DirectoryWalkConfig {
        roots: vec![root],
        globs: vec![],
        follow_symlinks: true,
        max_depth: Some(5),
    };
    let records = collect_records(&adapter, &config, None).await;

    let target_hits = records
        .iter()
        .filter(|r| {
            r.logical_path
                .as_ref()
                .is_some_and(|p| p.file_name() == Some("target.txt"))
        })
        .count();

    assert_eq!(
        target_hits, 1,
        "a cycle-safe walk must find target.txt exactly once regardless of \
         max_depth; found it {target_hits} times, meaning the symlink cycle \
         caused the same file to be rediscovered once per depth level"
    );
    Ok(())
}

/// sinex-6qef: `MaterialAnchor::DirectoryEntry.content_hash` is hardcoded
/// `None` at emission time (see the `open()` record-construction site), even
/// though `library.rs`'s `docs-library-index` source contract declares
/// `occurrence_identity = Uuid5From("(source, path, content_hash)")` for
/// exactly this anchor -- change detection via content hash is fiction, the
/// field is always empty.
#[sinex_test]
#[ignore = "sinex-6qef open: DirectoryWalkAdapter hardcodes content_hash: None, declared change-detection is fiction"]
async fn walked_records_carry_a_real_content_hash_sinex_6qef() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let root = Utf8PathBuf::from_path_buf(dir.path().to_path_buf()).unwrap();
    let mut file = std::fs::File::create(dir.path().join("doc.txt")).unwrap();
    file.write_all(b"hello content hash").unwrap();

    let adapter = DirectoryWalkAdapter;
    let config = simple_config(vec![root]);
    let records = collect_records(&adapter, &config, None).await;
    assert_eq!(records.len(), 1);

    let MaterialAnchor::DirectoryEntry { content_hash, .. } = &records[0].anchor else {
        panic!("expected a DirectoryEntry anchor");
    };
    assert!(
        content_hash.is_some(),
        "sinex-6qef: DirectoryWalkAdapter emits content_hash: None unconditionally, even though \
         docs-library-index declares occurrence_identity = (source, path, content_hash) -- the \
         declared change-detection field is never populated"
    );
    Ok(())
}
