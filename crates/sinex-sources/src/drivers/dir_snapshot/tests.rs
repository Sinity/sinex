use super::*;

fn vault() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-dir-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(d.join("sub")).unwrap();
    std::fs::write(d.join("a.md"), "alpha").unwrap();
    std::fs::write(d.join("sub/b.md"), "beta").unwrap();
    std::fs::write(d.join("skip.png"), "x").unwrap();
    d
}

fn driver(root: PathBuf, part_bytes: usize) -> DirSnapshot {
    DirSnapshot {
        root,
        host: "h".into(),
        select: |p| p.ends_with(".md"),
        part_bytes,
    }
}

fn paths(a: &Acquired) -> Vec<String> {
    a.material.as_ref().map_or(vec![], |m| {
        m.manifest["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["path"].as_str().unwrap().to_owned())
            .collect()
    })
}

#[test]
fn reads_changed_files_and_records_vanished_ones() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(paths(&first), vec!["a.md", "sub/b.md"]);
    let m = first.material.as_ref().unwrap();
    assert_eq!(record(m, 1), b"beta");
    let cursor = (m.cursor_at)(m.records.last().unwrap().1);

    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());

    std::fs::write(root.join("a.md"), "alpha 2").unwrap();
    std::fs::remove_file(root.join("sub/b.md")).unwrap();
    let next = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(paths(&next), vec!["a.md", "sub/b.md"]);
    let m = next.material.as_ref().unwrap();
    assert_eq!(record(m, 1), &vanished_marker("sub/b.md")[..]);
    assert!(m.manifest["members"][1].get("vanished_at_ns").is_some());
    assert_eq!(m.manifest["records"].as_array().unwrap().len(), 2);
}

#[test]
fn a_walk_cut_by_the_part_size_does_not_record_vanished_files() {
    let root = vault();
    let mut state = DirState::default();
    let small = driver(root.clone(), 6);
    let first = small.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(paths(&first), vec!["a.md"]);
    assert_eq!(
        first.material.as_ref().unwrap().manifest["scan_complete"],
        json!(false)
    );
    let m = first.material.as_ref().unwrap();
    let second = small
        .poll(&(m.cursor_at)(m.records.last().unwrap().1), &mut state)
        .unwrap();
    assert_eq!(paths(&second), vec!["sub/b.md"]);
}

#[test]
fn a_touched_but_unchanged_file_is_skipped() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1);
    let f = std::fs::File::options()
        .append(true)
        .open(root.join("a.md"))
        .unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());
}

fn record(m: &NewMaterial, index: usize) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let Some(StoredBytes::FramedFile(file)) = &m.stored else {
        panic!("not staged")
    };
    let (start, end) = m.records[index];
    let mut bytes = vec![0; end - start];
    file.read_exact_at(&mut bytes, start as u64).unwrap();
    bytes
}

struct Refuse;
impl super::super::MaterialSink for Refuse {
    fn put(&self, _: &[u8]) -> Result<String, String> {
        Err("synthetic storage failure".into())
    }
    fn put_file(&self, _: &Path) -> Result<String, String> {
        Err("synthetic storage failure".into())
    }
}

#[test]
fn failed_storage_retries_the_same_unnamed_capture_before_new_source_changes() {
    use super::super::prepare;
    use crate::{Limits, sources::notes};
    use sinex_model::{BatchKind, RunId};
    use std::os::unix::fs::MetadataExt;
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let source = notes::source();
    let mut state = DirState::default();
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let id = m.id;
    let Some(StoredBytes::FramedFile(file)) = &m.stored else {
        panic!()
    };
    assert_eq!(
        file.metadata().unwrap().nlink(),
        0,
        "staging has no crash-leaking name"
    );
    assert!(m.bytes.is_empty());
    assert!(
        prepare(
            &source,
            "b",
            RunId::new(),
            acquired,
            Value::Null,
            &Refuse,
            Limits::default()
        )
        .is_err()
    );
    assert!(state.files.is_empty(), "acquisition is not admission");
    std::fs::write(root.join("a.md"), "new source bytes").unwrap();
    let retry = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, id);
    assert_eq!(record(retry.material.as_ref().unwrap(), 0), b"alpha");
    let blobs = tempfile::tempdir().unwrap();
    let sink = super::super::producer::Store::open(blobs.path()).unwrap();
    let prepared = prepare(
        &source,
        "b",
        RunId::new(),
        retry,
        Value::Null,
        &sink,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(prepared.material_len(), Some(11));
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    assert_eq!(
        progress.material.unwrap().blob,
        format!("blake3:{}", blake3::hash(b"\0alpha\0beta").to_hex())
    );
    let next = d.poll(&progress.cursor, &mut state).unwrap();
    assert_ne!(next.material.as_ref().unwrap().id, id);
    assert_eq!(
        record(next.material.as_ref().unwrap(), 0),
        b"new source bytes"
    );
}

#[test]
fn partial_admission_resumes_original_member_anchors_and_only_final_cursor_updates_cache() {
    use super::super::prepare;
    use crate::{Limits, sources::notes};
    use sinex_model::{BatchKind, RecordOutcome, RunId};
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let source = notes::source();
    let mut state = DirState::default();
    let blobs = tempfile::tempdir().unwrap();
    let sink = super::super::producer::Store::open(blobs.path()).unwrap();
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let id = acquired.material.as_ref().unwrap().id;
    let prepared = prepare(
        &source,
        "b",
        RunId::new(),
        acquired,
        Value::Null,
        &sink,
        Limits {
            max_events: 1,
            max_bytes: 1000,
        },
    )
    .unwrap();
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    assert_eq!(progress.cursor["through"], 6);
    assert!(progress.cursor["last_complete_scan_ns"].is_null());
    drop(prepared);
    let acquired = d.poll(&progress.cursor, &mut state).unwrap();
    let m = acquired.material.as_ref().unwrap();
    assert_eq!(m.id, id);
    assert_eq!(m.records, vec![(7, 11)]);
    assert_eq!(m.manifest["records"], json!([[1, 6], [7, 11]]));
    assert!(state.files.is_empty());
    let prepared = prepare(
        &source,
        "b",
        RunId::new(),
        acquired,
        progress.carry,
        &sink,
        Limits::default(),
    )
    .unwrap();
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let RecordOutcome::Events { location, events } = &batch.records[0] else {
        panic!()
    };
    assert_eq!(location.unwrap().anchor, 7);
    assert_eq!(events[0].payload["path"], "sub/b.md");
    assert_eq!(events[0].payload["text"], "beta");
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    assert_eq!(progress.cursor["through"], 11);
    assert!(
        d.poll(&progress.cursor, &mut state)
            .unwrap()
            .material
            .is_none()
    );
    assert_eq!(state.files.len(), 2);
}

#[test]
fn bundle_staging_does_not_hold_whole_file_bodies() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let block = [b'x'; 64 * 1024];
    for n in 0..6 {
        let mut file = File::create(dir.path().join(format!("{n}.md"))).unwrap();
        for _ in 0..64 {
            file.write_all(&block).unwrap();
        }
    }
    let peak = || {
        let text = std::fs::read_to_string("/proc/self/status").unwrap();
        text.lines()
            .find_map(|s| s.strip_prefix("VmHWM:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    };
    let before = peak();
    let mut state = DirState::default();
    let acquired = driver(dir.path().to_owned(), 32 << 20)
        .poll(&Value::Null, &mut state)
        .unwrap();
    let m = acquired.material.unwrap();
    assert!(m.bytes.is_empty());
    assert_eq!(m.records.len(), 6);
    let Some(StoredBytes::FramedFile(file)) = m.stored else {
        panic!()
    };
    assert_eq!(file.metadata().unwrap().len(), (24 << 20) + 6);
    assert!(
        peak().saturating_sub(before) < 8 << 20,
        "acquisition retained the bundle body"
    );
}

#[test]
fn missing_directory_is_unavailable_and_accessible_empty_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let d = driver(dir.path().join("missing"), 1024);
    let missing = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    assert!(missing.observation.unavailable.is_some());
    assert!(missing.material.is_none());
    let d = driver(dir.path().to_owned(), 1024);
    let empty = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    assert!(empty.observation.unavailable.is_none());
    assert!(empty.material.is_none());
    assert!(empty.cursor_unchanged["last_complete_scan_ns"].is_number());
}
