mod restore_tar;

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
        removed_at_source: false,
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
fn metadata_changes_are_captured_even_when_content_stays_equal() {
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
    let updated = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(paths(&updated), ["a.md"]);
    assert_eq!(record(updated.material.as_ref().unwrap(), 0), b"alpha");
}

fn record(m: &NewMaterial, index: usize) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let Some(StoredBytes::FramedFile(file)) = &m.stored else {
        panic!("not in a temporary file")
    };
    let (start, end) = m.records[index];
    let mut bytes = vec![0; end - start];
    file.read_exact_at(&mut bytes, start as u64).unwrap();
    format::decode(&m.manifest, start, bytes.into())
        .unwrap()
        .into_owned()
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
    use crate::Limits;
    use crate::test_parser as notes;
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
    let expected_length = file.metadata().unwrap().len();
    let mut expected_bytes = vec![0; expected_length as usize];
    std::os::unix::fs::FileExt::read_exact_at(&**file, &mut expected_bytes, 0).unwrap();
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
    assert_eq!(prepared.material_len(), Some(expected_length as usize));
    let batch = prepared.batches().unwrap().next().unwrap().unwrap();
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    assert_eq!(
        progress.material.unwrap().blob,
        format!("blake3:{}", blake3::hash(&expected_bytes).to_hex())
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
    use crate::Limits;
    use crate::test_parser as notes;
    use sinex_model::{BatchKind, RecordOutcome, RunId};
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let source = notes::source();
    let mut state = DirState::default();
    let blobs = tempfile::tempdir().unwrap();
    let sink = FixtureSink::open(blobs.path()).unwrap();
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let id = acquired.material.as_ref().unwrap().id;
    let original_records = acquired.material.as_ref().unwrap().records.clone();
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
    assert_eq!(progress.cursor["through"], original_records[0].1);
    assert!(progress.cursor["last_complete_scan_ns"].is_null());
    drop(prepared);
    let acquired = d.poll(&progress.cursor, &mut state).unwrap();
    let m = acquired.material.as_ref().unwrap();
    assert_eq!(m.id, id);
    assert_eq!(m.records, original_records[1..]);
    assert_eq!(m.manifest.records, Some(original_records.clone()));
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
    assert_eq!(location.unwrap().anchor, original_records[1].0 as u64);
    assert_eq!(events[0].payload["path"], "sub/b.md");
    assert_eq!(events[0].payload["text"], "beta");
    let BatchKind::Source(progress) = batch.kind else {
        panic!()
    };
    assert_eq!(progress.cursor["through"], original_records[1].1);
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
    use crate::test_parser as notes;
    use crate::{Limits, drain::drain};
    use sinex_model::{Class, ReadingOrder, RecordOutcome};
    for bodies in [
        vec!["", "", "gamma"],
        vec!["gamma", "", ""],
        vec!["", "gamma", ""],
        vec![""],
        vec!["", ""],
        vec![" ", "\n", "  \n"],
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
        for ((start, end), body) in m.records.iter().zip(&bodies) {
            assert_eq!(end - start, body.len());
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
        let hashes: std::collections::BTreeSet<_> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                RecordOutcome::Events { events, .. } => Some(events[0].record_hash),
                _ => None,
            })
            .collect();
        assert_eq!(
            hashes.len(),
            bodies.len(),
            "distinct paths keep distinct readings even with equal bytes"
        );
        for (index, (outcome, body)) in outcomes.iter().zip(&bodies).enumerate() {
            let RecordOutcome::Events { location, events } = outcome else {
                panic!("{outcome:?}")
            };
            assert_eq!(location.unwrap().anchor, m.records[index].0 as u64);
            assert_eq!(events[0].payload["path"], format!("{index}.md"));
            assert_eq!(events[0].payload["text"], *body);
            assert_eq!(record(&m, index), body.as_bytes());
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
        restored.files[Path::new("a.md")].unwrap().1,
        *blake3::hash(b"").as_bytes(),
    ));
    assert_eq!(restored.files.get(Path::new("a.md")), Some(&empty));
    assert!(!restored.files.contains_key(Path::new("sub/b.md")));
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
        restored.files[Path::new("sub/b.md")].map(|(size, _, hash)| (size, hash)),
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
        !restored.files.contains_key(Path::new("sub/b.md")),
        "the newer bundle records the file as removed"
    );
}

#[devkit::test]
fn failed_disappearance_is_retried_before_a_reappeared_file() {
    use super::super::prepare;
    use crate::Limits;
    use crate::test_parser as notes;
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
    assert!(state.files.contains_key(Path::new("a.md")));
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
    let files: FileMemory = HashMap::from([
        (PathBuf::from("a.md"), Some((5, -3, [1; 32]))),
        (
            PathBuf::from("sub/é.md"),
            Some((0, 1_700_000_000_000_000_000, [2; 32])),
        ),
        (PathBuf::from("unknown.md"), None),
    ]);
    let memory = restore::memory_of(&files, &HashMap::new());
    let mut state = DirState::default();
    // A newer bundle already decided a.md.
    state.remembered.insert(PathBuf::from("a.md"));
    state
        .files
        .insert(PathBuf::from("a.md"), Some((9, 9, [9; 32])));
    state.remember_saved(&memory).unwrap();
    assert_eq!(state.files[Path::new("a.md")], Some((9, 9, [9; 32])));
    assert_eq!(
        state.files[Path::new("sub/é.md")],
        files[Path::new("sub/é.md")]
    );
    assert_eq!(state.files[Path::new("unknown.md")], None);
    assert!(
        DirState::default()
            .remember_saved(&memory[..memory.len() - 1])
            .is_err()
    );
}

#[devkit::test]
fn native_names_survive_capture_restart_changes_and_removal() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    let dir = tempfile::tempdir().unwrap();
    let names = [
        PathBuf::from(OsString::from_vec(b"bad-\xff.md".to_vec())),
        PathBuf::from(OsString::from_vec(b"bad-\xfe.md".to_vec())),
        PathBuf::from("bad-�.md"),
        PathBuf::from("bad-%FF.md"),
        PathBuf::from("é.md"),
        PathBuf::from("e\u{301}.md"),
    ];
    for (index, name) in names.iter().enumerate() {
        std::fs::write(dir.path().join(name), index.to_string()).unwrap();
    }
    let d = driver(dir.path().to_owned(), 1 << 20);
    let first = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    let material = first.material.unwrap();
    assert_eq!(material.records.len(), names.len());
    let displays: HashSet<_> = material
        .manifest
        .members
        .iter()
        .map(|member| member.path.as_str())
        .collect();
    assert!(displays.contains("bad-%FF.md"));
    assert!(
        material
            .manifest
            .members
            .iter()
            .filter(|member| member.details.contains_key("native_path_bytes"))
            .count()
            == 2
    );
    assert!(displays.contains("é.md"));
    assert!(displays.contains("e\u{301}.md"));
    for (index, member) in material.manifest.members.iter().enumerate() {
        let name = paths::native(member).unwrap();
        let expected = names
            .iter()
            .position(|candidate| candidate == &name)
            .unwrap();
        assert_eq!(record(&material, index), expected.to_string().as_bytes());
    }
    let cursor = (material.cursor_at)(material.records.last().unwrap().1).unwrap();
    for saved in [false, true] {
        let mut state = DirState::default();
        if saved {
            state
                .remember_saved(material.memory.as_ref().unwrap())
                .unwrap();
        } else {
            state
                .remember_members(&material.manifest, u64::MAX)
                .unwrap();
        }
        state.finish_remembering();
        assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());
        for name in &names {
            assert!(state.files.contains_key(name));
        }
    }
    let mut state = DirState::default();
    state
        .remember_saved(material.memory.as_ref().unwrap())
        .unwrap();
    state.finish_remembering();
    std::fs::write(dir.path().join(&names[0]), "changed bytes").unwrap();
    std::fs::remove_file(dir.path().join(&names[1])).unwrap();
    let changed = d.poll(&cursor, &mut state).unwrap();
    let material = changed.material.unwrap();
    assert_eq!(material.records.len(), 2);
    assert_eq!(record(&material, 0), b"changed bytes");
    assert_eq!(material.manifest.members[0].path, "bad-%FF.md");
    assert_eq!(record(&material, 1), removed_marker("bad-%FE.md"));
}

#[devkit::test]
fn an_unreadable_entry_fails_only_its_record_and_is_retried() {
    use crate::{Limits, drain::drain, test_parser::NotesParser};
    use sinex_model::{Class, ReadingOrder, RecordOutcome};
    use std::sync::Mutex;
    static REMOVED: Mutex<Option<PathBuf>> = Mutex::new(None);
    fn select(path: &str) -> bool {
        if path == "a.md" {
            let path = REMOVED.lock().unwrap().take().unwrap();
            std::fs::remove_file(path).unwrap();
        }
        true
    }
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("a.md");
    std::fs::write(&bad, "unreadable").unwrap();
    std::fs::write(dir.path().join("b.md"), "readable").unwrap();
    *REMOVED.lock().unwrap() = Some(bad.clone());
    let mut d = driver(dir.path().to_owned(), 1);
    d.select = select;
    let mut state = DirState::default();
    let material = d.poll(&Value::Null, &mut state).unwrap().material.unwrap();
    assert_eq!(material.records.len(), 2);
    let chunks = drain(
        &NotesParser,
        crate::DrainInput {
            source: "synthetic",
            class: Class::Open,
            material: material.id,
            bytes: material.record_bytes(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    assert_eq!(chunks[0].failures, 1);
    assert!(
        matches!(&chunks[0].outcomes[0], RecordOutcome::Failure {reason, ..} if reason.starts_with("a.md:"))
    );
    assert!(
        matches!(&chunks[0].outcomes[1], RecordOutcome::Events {events, ..} if events[0].payload["text"] == "readable")
    );
    let mut restored = DirState::default();
    restored
        .remember_members(&material.manifest, u64::MAX)
        .unwrap();
    assert!(!restored.files.contains_key(Path::new("a.md")));
    let cursor = (material.cursor_at)(material.records.last().unwrap().1).unwrap();
    std::fs::write(&bad, "readable now").unwrap();
    d.select = |_| true;
    let retried = d.poll(&cursor, &mut state).unwrap().material.unwrap();
    assert_eq!(retried.records.len(), 1);
    assert_eq!(retried.manifest.members[0].path, "a.md");
    assert_eq!(record(&retried, 0), b"readable now");
}

#[devkit::test]
fn path_display_round_trips_every_native_byte() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    let path = PathBuf::from(OsString::from_vec((1..=255).collect()));
    let mut member = json!({"path": paths::display(&path), "stream_offset": 1, "length": 0});
    paths::add_native(&mut member, &path);
    let member = serde_json::from_value(member).unwrap();
    assert_eq!(paths::native(&member).unwrap(), path);
    for text in ["%", "%X1", "%1", "%💡", "100% done.md", "foo%20bar.md"] {
        let member =
            serde_json::from_value(json!({"path": text, "stream_offset": 1, "length": 0})).unwrap();
        assert_eq!(paths::display(Path::new(text)), text);
        assert_eq!(paths::native(&member).unwrap(), PathBuf::from(text));
    }
}

#[devkit::test]
fn open_and_read_errors_belong_to_the_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut bundle = tar::Archive::new(tempfile::tempfile().unwrap());
    assert!(
        tar::capture(
            &dir.path().join("missing"),
            Path::new("missing"),
            &mut bundle,
            0,
            0
        )
        .unwrap()
        .is_err()
    );
    // On Unix a directory can be opened but cannot be read as file bytes.
    assert!(
        tar::capture(
            dir.path(),
            Path::new("directory"),
            &mut bundle,
            dir.path().metadata().unwrap().len(),
            tar::change_time(&dir.path().metadata().unwrap())
        )
        .unwrap()
        .is_err()
    );
}

fn final_cursor(acquired: &Acquired) -> Value {
    let material = acquired.material.as_ref().unwrap();
    (material.cursor_at)(material.records.last().unwrap().1).unwrap()
}

fn poll_entries(
    d: &DirSnapshot,
    cursor: &Value,
    state: &mut DirState,
    entries: BTreeMap<PathBuf, EntryFacts>,
) -> Acquired {
    // Apply the admitted held material through the same entry point as a poll.
    if let Some(held) = state.held.take() {
        assert!(held.material.admitted(cursor));
        state.files = held.files;
        state.failures = held.failures;
        state.retries = held.retries;
    }
    let previous = if cursor.is_null() {
        Cursor::default()
    } else {
        serde_json::from_value(cursor.clone()).unwrap()
    };
    d.poll_entries(
        cursor,
        state,
        previous,
        file_facts(&d.root).unwrap(),
        entries,
    )
    .unwrap()
}

#[devkit::test]
fn tar_members_with_verbatim_percent_names_restore_without_new_records() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["100% done.md", "foo%20bar.md"] {
        std::fs::write(dir.path().join(name), "text").unwrap();
    }
    let d = driver(dir.path().to_owned(), 1 << 20);
    let first = d.poll(&Value::Null, &mut DirState::default()).unwrap();
    let material = first.material.as_ref().unwrap();
    let mut state = DirState::default();
    state
        .remember_members(
            &material.manifest,
            material.records.last().unwrap().1 as u64,
        )
        .unwrap();
    state.finish_remembering();
    assert!(d.poll(&Value::Null, &mut state).unwrap().material.is_none());
    assert!(state.files.contains_key(Path::new("100% done.md")));
    assert!(state.files.contains_key(Path::new("foo%20bar.md")));
    assert_eq!(paths(&first), ["100% done.md", "foo%20bar.md"]);
    assert!(
        material
            .manifest
            .members
            .iter()
            .all(|member| !member.details.contains_key("native_path_bytes"))
    );
}

#[devkit::test]
fn failures_are_recorded_on_change_and_after_recovery_only() {
    let dir = tempfile::tempdir().unwrap();
    let d = driver(dir.path().to_owned(), 1024);
    let mut state = DirState::default();
    let failed = |reason: &str| BTreeMap::from([(PathBuf::from("a.md"), Err(reason.to_owned()))]);
    let first = poll_entries(&d, &Value::Null, &mut state, failed("denied"));
    let cursor = final_cursor(&first);
    // Before admission the identical material is offered again.
    assert_eq!(
        d.poll(&Value::Null, &mut state)
            .unwrap()
            .material
            .unwrap()
            .id,
        first.material.unwrap().id
    );
    for _ in 0..3 {
        assert!(
            poll_entries(&d, &cursor, &mut state, failed("denied"))
                .material
                .is_none()
        );
    }
    let changed = poll_entries(&d, &cursor, &mut state, failed("missing"));
    let cursor = final_cursor(&changed);
    assert_eq!(
        changed.material.unwrap().manifest.members[0].details["read_error"],
        "missing"
    );
    std::fs::write(dir.path().join("a.md"), "readable").unwrap();
    let recovered = d.poll(&cursor, &mut state).unwrap();
    let cursor = final_cursor(&recovered);
    let again = poll_entries(&d, &cursor, &mut state, failed("missing"));
    assert!(again.material.is_some());
}

#[devkit::test]
fn failed_subtrees_preserve_only_their_own_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = driver(dir.path().to_owned(), 1024);
    let mut state = DirState::default();
    for name in [
        "blocked/inside.md",
        "blocked-extra/removed.md",
        "outside.md",
    ] {
        state.files.insert(PathBuf::from(name), None);
    }
    let failed = BTreeMap::from([(PathBuf::from("blocked"), Err("denied".into()))]);
    let first = poll_entries(&d, &Value::Null, &mut state, failed.clone());
    assert_eq!(
        paths(&first),
        vec!["blocked", "blocked-extra/removed.md", "outside.md"]
    );
    let cursor = final_cursor(&first);
    assert!(
        poll_entries(&d, &cursor, &mut state, failed)
            .material
            .is_none()
    );
    assert_eq!(
        state.files.keys().collect::<Vec<_>>(),
        vec![&PathBuf::from("blocked/inside.md")]
    );
    let recovered = poll_entries(&d, &cursor, &mut state, BTreeMap::new());
    assert_eq!(paths(&recovered), vec!["blocked/inside.md"]);
}

#[devkit::test]
fn changing_files_are_skipped_without_failures_and_retried() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.md");
    std::fs::write(&path, "before").unwrap();
    let before = path.metadata().unwrap();
    std::fs::write(&path, "after with more bytes").unwrap();
    let d = driver(dir.path().to_owned(), 1024);
    let mut state = DirState::default();
    let changed = poll_entries(
        &d,
        &Value::Null,
        &mut state,
        BTreeMap::from([(
            PathBuf::from("a.md"),
            Ok((before.len(), tar::change_time(&before))),
        )]),
    );
    assert!(changed.material.is_none());
    assert!(state.files.is_empty());
    assert!(state.failures.is_empty());
    let retried = d
        .poll(&changed.cursor_without_material, &mut state)
        .unwrap();
    assert_eq!(paths(&retried), vec!["a.md"]);
    assert_eq!(
        record(retried.material.as_ref().unwrap(), 0),
        b"after with more bytes"
    );
}

#[devkit::test]
fn a_part_cut_keeps_failures_of_files_not_yet_retried() {
    let dir = tempfile::tempdir().unwrap();
    let d = driver(dir.path().to_owned(), 1);
    let mut state = DirState::default();
    let missing = (PathBuf::from("z.md"), Ok((1, 0)));
    let failed = poll_entries(
        &d,
        &Value::Null,
        &mut state,
        BTreeMap::from([missing.clone()]),
    );
    let cursor = final_cursor(&failed);
    std::fs::write(dir.path().join("a.md"), "a").unwrap();
    let facts = dir.path().join("a.md").metadata().unwrap();
    let entries = BTreeMap::from([
        (
            PathBuf::from("a.md"),
            Ok((facts.len(), tar::change_time(&facts))),
        ),
        missing,
    ]);
    let cut = poll_entries(&d, &cursor, &mut state, entries.clone());
    assert_eq!(paths(&cut), vec!["a.md"]);
    let cursor = final_cursor(&cut);
    // The missing file is actually retried now, with the same read error.
    assert!(
        poll_entries(&d, &cursor, &mut state, entries)
            .material
            .is_none()
    );
    assert!(state.failures.contains_key(Path::new("z.md")));
}

#[devkit::test]
fn changing_files_retry_even_when_their_facts_return_to_the_cached_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.md");
    std::fs::write(&path, "after").unwrap();
    let facts = path.metadata().unwrap();
    let size = facts.len();
    let mtime = tar::change_time(&facts);
    let mut state = DirState::default();
    state.files.insert(
        PathBuf::from("a.md"),
        Some((size, mtime, *blake3::hash(b"prior").as_bytes())),
    );
    let d = driver(dir.path().to_owned(), 1024);
    // Enumeration saw an intermediate size; capture sees the cached size again.
    let changed = poll_entries(
        &d,
        &Value::Null,
        &mut state,
        BTreeMap::from([(PathBuf::from("a.md"), Ok((size + 1, mtime)))]),
    );
    assert!(changed.material.is_none());
    assert!(state.failures.is_empty());
    let retry = d
        .poll(&changed.cursor_without_material, &mut state)
        .unwrap();
    assert_eq!(record(retry.material.as_ref().unwrap(), 0), b"after");
}

#[devkit::test]
fn failed_files_retry_even_when_their_facts_match_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.md");
    std::fs::write(&path, "after").unwrap();
    let facts = path.metadata().unwrap();
    let mut state = DirState::default();
    state.files.insert(
        PathBuf::from("a.md"),
        Some((
            facts.len(),
            tar::change_time(&facts),
            *blake3::hash(b"prior").as_bytes(),
        )),
    );
    state
        .failures
        .insert(PathBuf::from("a.md"), "denied".into());
    let d = driver(dir.path().to_owned(), 1024);
    let retry = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(record(retry.material.as_ref().unwrap(), 0), b"after");
    let cursor = final_cursor(&retry);
    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());
    assert!(state.failures.is_empty());
}

#[devkit::test]
fn an_unstable_file_with_unchanged_content_retains_its_new_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.md");
    std::fs::write(&path, "same").unwrap();
    let facts = path.metadata().unwrap();
    let size = facts.len();
    let mtime = tar::change_time(&facts);
    let mut state = DirState::default();
    state.files.insert(
        PathBuf::from("a.md"),
        Some((size, mtime, *blake3::hash(b"same").as_bytes())),
    );
    let d = driver(dir.path().to_owned(), 1024);
    let changed = poll_entries(
        &d,
        &Value::Null,
        &mut state,
        BTreeMap::from([(PathBuf::from("a.md"), Ok((size + 1, mtime)))]),
    );
    assert!(changed.material.is_none());
    assert!(state.retries.contains(Path::new("a.md")));
    let retry = d
        .poll(&changed.cursor_without_material, &mut state)
        .unwrap();
    assert_eq!(record(retry.material.as_ref().unwrap(), 0), b"same");
    let cursor = final_cursor(&retry);
    assert!(d.poll(&cursor, &mut state).unwrap().material.is_none());
    assert!(state.retries.is_empty());
}
