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
        m.manifest.members.iter().map(|x| x.path.clone()).collect()
    })
}

#[devkit::test]
fn reads_changed_files_and_records_removed_ones() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(paths(&first), vec!["a.md", "sub/b.md"]);
    let m = first.material.as_ref().unwrap();
    assert_eq!(record(m, 1), b"beta");
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();

    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());

    std::fs::write(root.join("a.md"), "alpha 2").unwrap();
    std::fs::remove_file(root.join("sub/b.md")).unwrap();
    let next = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(paths(&next), vec!["a.md", "sub/b.md"]);
    let m = next.material.as_ref().unwrap();
    assert_eq!(record(m, 1), &removed_marker("sub/b.md")[..]);
    assert!(m.manifest.members[1].details.contains_key("removed_at_ns"));
    assert_eq!(m.manifest.records.as_ref().unwrap().len(), 2);
}

#[devkit::test]
fn a_walk_cut_by_the_part_size_does_not_record_removed_files() {
    let root = vault();
    let mut state = DirState::default();
    let small = driver(root.clone(), 6);
    let first = small.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(paths(&first), vec!["a.md"]);
    assert_eq!(
        first
            .material
            .as_ref()
            .unwrap()
            .manifest
            .detail("scan_complete"),
        &json!(false)
    );
    let m = first.material.as_ref().unwrap();
    let second = small
        .poll(
            &(m.cursor_at)(m.records.last().unwrap().1).unwrap(),
            &mut state,
        )
        .unwrap();
    assert_eq!(paths(&second), vec!["sub/b.md"]);
}

#[devkit::test]
fn a_touched_but_unchanged_file_is_skipped() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
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
        panic!("not in a temporary file")
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

#[devkit::test]
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
        "the temporary bundle has no name a crash could leave behind"
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
    let sink = FixtureSink::open(blobs.path()).unwrap();
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

#[devkit::test]
fn partial_admission_resumes_original_member_anchors_and_only_final_cursor_updates_cache() {
    use super::super::prepare;
    use crate::{Limits, sources::notes};
    use sinex_model::{BatchKind, RecordOutcome, RunId};
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let source = notes::source();
    let mut state = DirState::default();
    let blobs = tempfile::tempdir().unwrap();
    let sink = FixtureSink::open(blobs.path()).unwrap();
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
    assert_eq!(m.manifest.records, Some(vec![(1, 6), (7, 11)]));
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

#[devkit::test]
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
    assert!(empty.cursor_without_material["last_complete_scan_ns"].is_number());
}

#[devkit::test]
fn empty_and_nonempty_members_keep_exact_body_ranges_in_every_order() {
    use crate::{Limits, drain::drain, sources::notes};
    use sinex_model::{Class, ReadingOrder, RecordOutcome};
    use std::os::unix::fs::FileExt;
    for bodies in [
        vec!["", "", "gamma"],
        vec!["gamma", "", ""],
        vec!["", "gamma", ""],
        vec![""],
        vec!["", ""],
    ] {
        let dir = tempfile::tempdir().unwrap();
        for (index, body) in bodies.iter().enumerate() {
            std::fs::write(dir.path().join(format!("{index}.md")), body).unwrap();
        }
        let acquired = driver(dir.path().to_owned(), 1 << 20)
            .poll(&Value::Null, &mut DirState::default())
            .unwrap();
        let m = acquired.material.unwrap();
        assert!(m.records.windows(2).all(|ranges| ranges[0].1 < ranges[1].1));
        let Some(StoredBytes::FramedFile(file)) = &m.stored else {
            panic!()
        };
        for ((start, end), body) in m.records.iter().zip(&bodies) {
            assert_eq!(end - start, body.len());
            let mut prefix = [1];
            file.read_exact_at(&mut prefix, (start - 1) as u64).unwrap();
            assert_eq!(prefix, [0]);
        }
        let chunks = drain(
            &notes::NotesParser,
            crate::DrainInput {
                source: "synthetic",
                class: Class::Open,
                material: m.id,
                bytes: m.record_bytes(),
                records: &m.records,
                carry: Value::Null,
                reading_order: &|_| ReadingOrder(0),
                limits: Limits {
                    max_events: 1,
                    max_bytes: 1,
                },
                manifest: &m.manifest,
            },
        )
        .unwrap();
        let outcomes: Vec<_> = chunks.into_iter().flat_map(|c| c.outcomes).collect();
        assert_eq!(outcomes.len(), bodies.len());
        for (index, (outcome, body)) in outcomes.iter().zip(&bodies).enumerate() {
            let RecordOutcome::Events { location, events } = outcome else {
                panic!("{outcome:?}")
            };
            assert_eq!(location.unwrap().anchor, m.records[index].0 as u64);
            assert_eq!(events[0].payload["path"], format!("{index}.md"));
            assert_eq!(events[0].payload["text"], *body);
            assert_eq!(
                &events[0].record_hash,
                blake3::hash(body.as_bytes()).as_bytes()
            );
        }
    }
}

#[devkit::test]
fn a_restart_remembers_the_files_of_admitted_records_only() {
    let root = vault();
    std::fs::write(root.join("a.md"), b"").unwrap();
    let d = driver(root.clone(), 1 << 20);
    let mut fresh = DirState::default();
    let acquired = d.poll(&Value::Null, &mut fresh).unwrap();
    let material = acquired.material.as_ref().unwrap();
    let mut restored = DirState::default();
    restored
        .remember_members(&material.manifest, material.records[0].1 as u64)
        .unwrap();
    let empty = Some((
        0,
        restored.files["a.md"].unwrap().1,
        *blake3::hash(b"").as_bytes(),
    ));
    assert_eq!(restored.files.get("a.md"), Some(&empty));
    assert!(!restored.files.contains_key("sub/b.md"));
    let mut malformed = material.manifest.clone();
    malformed.members[0]
        .details
        .insert("removed_at_ns".into(), Value::Null);
    assert!(
        DirState::default()
            .remember_members(&malformed, u64::MAX)
            .is_err()
    );
    restored.finish_remembering();
    restored
        .remember_members(&material.manifest, u64::MAX)
        .unwrap();
    assert_eq!(
        restored.files["sub/b.md"].map(|(size, _, hash)| (size, hash)),
        Some((4, *blake3::hash(b"beta").as_bytes()))
    );
    assert!(
        d.poll(&Value::Null, &mut restored)
            .unwrap()
            .material
            .is_none(),
        "remembered files that did not change are not stored again"
    );
}

#[devkit::test]
fn a_newer_bundle_decides_a_path_before_an_older_one() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let older = d.poll(&Value::Null, &mut state).unwrap();
    let older = older.material.unwrap();
    let cursor = (older.cursor_at)(older.records.last().unwrap().1).unwrap();
    let _ = d.poll(&cursor, &mut state).unwrap();
    std::fs::remove_file(root.join("sub/b.md")).unwrap();
    let newer = d.poll(&cursor, &mut state).unwrap();
    let newer = newer.material.unwrap();
    let mut restored = DirState::default();
    restored
        .remember_members(&newer.manifest, u64::MAX)
        .unwrap();
    restored
        .remember_members(&older.manifest, u64::MAX)
        .unwrap();
    assert!(
        !restored.files.contains_key("sub/b.md"),
        "the newer bundle records the file as removed"
    );
}

#[devkit::test]
fn failed_disappearance_is_retried_before_a_reappeared_file() {
    use super::super::prepare;
    use crate::{Limits, sources::notes};
    use sinex_model::{BatchKind, RunId};
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut state = DirState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());
    std::fs::remove_file(root.join("a.md")).unwrap();
    let gone = d.poll(&cursor, &mut state).unwrap();
    let id = gone.material.as_ref().unwrap().id;
    let source = notes::source();
    assert!(
        prepare(
            &source,
            "b",
            RunId::new(),
            gone,
            Value::Null,
            &Refuse,
            Limits::default()
        )
        .is_err()
    );
    assert!(state.files.contains_key("a.md"));
    std::fs::write(root.join("a.md"), "reappeared").unwrap();
    let retry = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, id);
    assert_eq!(
        record(retry.material.as_ref().unwrap(), 0),
        removed_marker("a.md")
    );
    let dir = tempfile::tempdir().unwrap();
    let sink = FixtureSink::open(dir.path()).unwrap();
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
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    let newer = d.poll(&progress.cursor, &mut state).unwrap();
    assert_ne!(newer.material.as_ref().unwrap().id, id);
    assert_eq!(record(newer.material.as_ref().unwrap(), 0), b"reappeared");
}

struct FixtureSink(hashed_blobs::Store);
impl FixtureSink {
    fn open(root: &std::path::Path) -> Result<Self, hashed_blobs::Error> {
        hashed_blobs::Store::open(root, vec![], 0o600).map(Self)
    }
}
impl super::super::MaterialSink for FixtureSink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.0
            .put(bytes, 3)
            .map(|h| format!("blake3:{}", h.hex()))
            .map_err(|e| e.to_string())
    }
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        self.0
            .put(file, 3)
            .map(|h| format!("blake3:{}", h.hex()))
            .map_err(|e| e.to_string())
    }
}

#[devkit::test]
fn saved_memory_round_trips_and_newer_bundles_win() {
    let files: HashMap<String, Option<(u64, i128, [u8; 32])>> = HashMap::from([
        ("a.md".to_owned(), Some((5, -3, [1; 32]))),
        (
            "sub/é.md".to_owned(),
            Some((0, 1_700_000_000_000_000_000, [2; 32])),
        ),
        ("unknown.md".to_owned(), None),
    ]);
    let memory = restore::memory_of(&files);
    let mut state = DirState::default();
    // A newer bundle already decided a.md.
    state.remembered.insert("a.md".to_owned());
    state.files.insert("a.md".to_owned(), Some((9, 9, [9; 32])));
    state.remember_saved(&memory).unwrap();
    assert_eq!(state.files["a.md"], Some((9, 9, [9; 32])));
    assert_eq!(state.files["sub/é.md"], files["sub/é.md"]);
    assert_eq!(state.files["unknown.md"], None);
    assert!(
        DirState::default()
            .remember_saved(&memory[..memory.len() - 1])
            .is_err()
    );
}
