use super::*;
use serde_json::Value;
use sinex_model::{ReadingOrder, RecordOutcome};
use sinex_source_core::DeclaredSource;
use sinex_source_core::drivers::dir_snapshot::DirState;
use sinex_source_core::{DrainInput, Limits, drain};
use std::path::Path;
pub(super) fn parse(acquired: &sinex_source_core::Acquired) -> Vec<sinex_model::Event> {
    let m = acquired.material.as_ref().unwrap();
    drain(
        &Stateless(DocumentParser),
        DrainInput {
            source: SPEC.name,
            class: Class::Guarded,
            material: m.id,
            bytes: m.record_bytes(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &m.manifest,
        },
    )
    .unwrap()
    .into_iter()
    .flat_map(|b| b.outcomes)
    .flat_map(|outcome| match outcome {
        RecordOutcome::Events { events, .. } => events,
        RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
    })
    .collect()
}
#[devkit::test]
fn binary_empty_and_large_documents_replay_after_originals_are_gone() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let large = vec![0xff; (2 << 20) + 1];
    std::fs::write(root.join("a.bin"), &large).unwrap();
    std::fs::write(root.join("b.empty"), []).unwrap();
    std::fs::write(root.join("c.pdf"), b"%PDF-1.4\nexample").unwrap();
    std::fs::write(root.join("note.md"), b"# Notes").unwrap();
    std::fs::write(root.join("z.whitespace"), b" \n\t").unwrap();
    let driver = driver(root.to_path_buf(), "example-host".into());
    let acquired = driver.poll(&Value::Null, &mut DirState::default()).unwrap();
    let first = parse(&acquired);
    assert_eq!(first.len(), 5);
    assert_eq!(first[0].payload["media_type"], "application/octet-stream");
    assert_eq!(first[0].payload["size"], large.len());
    assert_eq!(
        first[0].payload["content"],
        format!("blake3:{}", blake3::hash(&large).to_hex())
    );
    assert_eq!(first[1].payload["size"], 0);
    assert_eq!(first[2].payload["media_type"], "application/pdf");
    assert_ne!(first[0].origin, first[1].origin);
    std::fs::remove_dir_all(root).unwrap();
    let replay = parse(&acquired);
    assert_eq!(
        first.iter().map(|e| &e.payload).collect::<Vec<_>>(),
        replay.iter().map(|e| &e.payload).collect::<Vec<_>>()
    );
    assert!(
        driver
            .poll(&Value::Null, &mut DirState::default())
            .unwrap()
            .observation
            .unavailable
            .is_some()
    );
}
#[devkit::test]
fn unchanged_changed_removed_and_missing_roots_are_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let driver = driver(temp.path().into(), "example-host".into());
    let mut state = DirState::default();
    assert!(
        driver
            .poll(&Value::Null, &mut state)
            .unwrap()
            .material
            .is_none()
    );
    std::fs::write(temp.path().join("a.txt"), b"one").unwrap();
    let first = driver.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    assert!(driver.poll(&cursor, &mut state).unwrap().material.is_none());
    std::fs::write(temp.path().join("a.txt"), b"changed").unwrap();
    let second = driver.poll(&cursor, &mut state).unwrap();
    assert_ne!(parse(&first)[0].origin, parse(&second)[0].origin);
    assert!(parse(&first)[0].equivalence_key.is_none());
    assert!(parse(&second)[0].equivalence_key.is_none());
    let m = second.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    std::fs::remove_file(temp.path().join("a.txt")).unwrap();
    let removed = parse(&driver.poll(&cursor, &mut state).unwrap());
    assert_eq!(removed[0].record_state, sinex_model::RecordState::Removed);
    assert!(removed[0].payload["content"].is_null());
    assert!(removed[0].payload["media_type"].is_null());
    assert!(removed[0].payload["size"].is_null());
}
#[devkit::test]
fn reader_restart_rebuilds_file_facts_and_keeps_held_bytes() {
    use sinex_model::{BatchKind, RunId};
    use sinex_source_core::drivers::prepare;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("a.txt"), b"captured").unwrap();
    let sinex_source_core::Reading::Polled(mut reader) =
        sinex_source_core::binding::walked(driver(temp.path().into(), "example-host".into()))
    else {
        panic!()
    };
    let acquired = reader.poll(&Value::Null).unwrap();
    let id = acquired.material.as_ref().unwrap().id;
    std::fs::write(temp.path().join("a.txt"), b"changed since reading").unwrap();
    let retry = reader.poll(&Value::Null).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, id);
    let first = parse(&retry);
    assert_eq!(first[0].payload["size"], 8);
    let sink = Sink::default();
    let source = DeclaredSource {
        spec: SPEC,
        parser: Stateless(DocumentParser),
    };
    let prepared = prepare(
        &source,
        "example-binding",
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
    let reference = progress.material.unwrap();
    let bytes = sink.0.lock().unwrap().get(&reference.blob).unwrap().clone();
    let sinex_source_core::Reading::Polled(mut restarted) =
        sinex_source_core::binding::walked(driver(temp.path().into(), "example-host".into()))
    else {
        panic!()
    };
    assert_eq!(restarted.history_from(&progress.cursor), Some(id));
    restarted
        .restore(
            &progress.cursor,
            sinex_source_core::StoredMaterial {
                admitted: sinex_source_core::drivers::cursor_material(&progress.cursor)
                    .and_then(|(_, through)| through),
                reference,
                bytes: std::sync::Arc::new(Bytes(bytes)),
                same_revision: true,
            },
        )
        .unwrap();
    restarted.restored().unwrap();
    let changed = restarted.poll(&progress.cursor).unwrap();
    assert_eq!(parse(&changed)[0].payload["size"], 21);
}

#[derive(Default)]
struct Sink(std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>);
impl sinex_source_core::MaterialSink for Sink {
    fn put_file(&self, path: &Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
        self.0.lock().unwrap().insert(name.clone(), bytes.into());
        Ok(name)
    }
}
struct Bytes(Vec<u8>);
impl sinex_source_core::drain::RecordRead for Bytes {
    fn length(&self) -> u64 {
        self.0.len() as u64
    }
    fn read_exact_at(&self, start: u64, bytes: &mut [u8]) -> std::io::Result<()> {
        let start = usize::try_from(start).map_err(std::io::Error::other)?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("range overflow"))?;
        bytes.copy_from_slice(
            self.0
                .get(start..end)
                .ok_or_else(|| std::io::Error::other("range beyond bytes"))?,
        );
        Ok(())
    }
}

#[devkit::test]
fn document_discovery_excludes_owned_subtrees_and_reports_symlink_roots() {
    use sinex_source_core::discovery::DiscoveryContext;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("documents");
    let notes = root.join("notes");
    let library = root.join("library");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::create_dir(&library).unwrap();
    std::fs::write(root.join(".hidden.md"), b"hidden").unwrap();
    std::fs::write(root.join("readme.markdown"), b"document").unwrap();
    std::fs::write(notes.join("note.md"), b"owned").unwrap();
    std::fs::write(library.join("book.pdf"), b"owned").unwrap();
    let alias = temp.path().join("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let mut context = DiscoveryContext {
        host: "example".into(),
        ..Default::default()
    };
    context.input_roots.insert("documents".into(), vec![alias]);
    context.input_roots.insert("notes".into(), vec![notes]);
    context.inputs.insert(
        super::super::library::FORMAT.name.into(),
        vec![library.clone()],
    );
    for source in [
        super::super::notes::SPEC.name,
        super::super::library::SCAN.name,
    ] {
        context.exclude.insert(
            source.into(),
            Some(sinex_source_core::discovery::Exclusion {
                reason: "separate reader".into(),
            }),
        );
    }
    let found = discover(&FORMAT, &context);
    let input = found.iter().find(|i| !i.bindings.is_empty()).unwrap();
    assert!(input.reason.as_ref().unwrap().contains("symbolic-link"));
    let sinex_source_core::Reading::Polled(mut reader) =
        (FORMAT.reader.unwrap())(&input.bindings[0]).unwrap()
    else {
        panic!()
    };
    let events = parse(&reader.poll(&Value::Null).unwrap());
    assert_eq!(events.len(), 2);
    assert!(events.iter().any(|e| e.payload["path"] == ".hidden.md"));
}
#[devkit::test]
fn raw_byte_document_names_stay_distinct_from_utf8() {
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path()
            .join(std::ffi::OsString::from_vec(b"file-\xff.bin".to_vec())),
        b"raw",
    )
    .unwrap();
    std::fs::write(temp.path().join("file-%FF.bin"), b"utf8").unwrap();
    let events = parse(
        &driver(temp.path().into(), "example".into())
            .poll(&Value::Null, &mut DirState::default())
            .unwrap(),
    );
    assert_eq!(events.len(), 2);
    assert_ne!(events[0].origin, events[1].origin);
    assert_ne!(
        events[0].payload["native_path_bytes"],
        events[1].payload["native_path_bytes"]
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.payload["native_path_bytes"].is_string())
            .count(),
        1
    );
}

#[devkit::test]
fn typed_document_options_preserve_excluded_roots_and_refuse_json_strings() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = sinex_source_core::Settings::new("host");
    settings.options = serde_json::json!({"excluded_roots": [root.path().join("notes")]});
    let binding = (FORMAT.plan.unwrap())(root.path(), &settings)
        .unwrap()
        .remove(0);
    assert!((FORMAT.reader.unwrap())(&binding).is_ok());
    for options in [
        serde_json::json!("{}"),
        serde_json::json!({"excluded_roots": "notes"}),
        serde_json::json!({"capture_documents": true}),
    ] {
        settings.options = options;
        assert!((FORMAT.plan.unwrap())(root.path(), &settings).is_err());
    }
}

fn located_driver(root: PathBuf) -> DirSnapshot {
    let mut driver = driver(root, "example-host".into());
    driver.locate = Some(locate_audio_video);
    driver
}

fn media_fixture() -> Vec<u8> {
    let mut bytes = b"\0\0\0\x20ftypisom".to_vec();
    bytes.resize((2 << 20) + 7, 0x93);
    bytes
}

fn accepted_cursor(acquired: &sinex_source_core::Acquired) -> Value {
    let material = acquired.material.as_ref().unwrap();
    (material.cursor_at)(material.records.last().unwrap().1).unwrap()
}

#[devkit::test]
fn located_media_is_recorded_without_bytes_and_other_files_keep_their_bytes() {
    let root = tempfile::tempdir().unwrap();
    let media = media_fixture();
    // Classification follows bytes, including when the extension is misleading.
    std::fs::write(root.path().join("movie.bin"), &media).unwrap();
    std::fs::write(root.path().join("note.txt"), b"retained note").unwrap();
    let driver = located_driver(root.path().into());
    let mut state = DirState::default();
    let first = driver.poll(&Value::Null, &mut state).unwrap();
    let material = first.material.as_ref().unwrap();
    let listed = material
        .manifest
        .members
        .iter()
        .find(|m| m.path == "movie.bin")
        .unwrap();
    assert_eq!(listed.length, 0);
    assert_eq!(listed.details["located"], true);
    assert_eq!(listed.node.as_ref().unwrap().size, media.len() as u64);
    assert!(listed.node.as_ref().unwrap().sparse.is_none());
    let sinex_source_core::drivers::StoredBytes::FramedFile(stored) =
        material.stored.as_ref().unwrap()
    else {
        panic!()
    };
    assert!(
        stored.metadata().unwrap().len() < 16384,
        "media payload was copied into the listing"
    );
    // An independent tar reader sees the facts in the original listing bytes.
    // The evidence is not only a reconstruction from database manifest fields.
    let mut listing = stored.try_clone().unwrap();
    std::io::Seek::rewind(&mut listing).unwrap();
    let mut archive = tar::Archive::new(listing);
    let mut entries = archive.entries().unwrap();
    let mut media_entry = entries.next().unwrap().unwrap();
    assert_eq!(media_entry.size(), 0);
    let facts: std::collections::BTreeMap<_, _> = media_entry
        .pax_extensions()
        .unwrap()
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.key_bytes().to_vec(), entry.value_bytes().to_vec())
        })
        .collect();
    assert_eq!(facts[b"SINEX.located".as_slice()], b"true");
    assert_eq!(
        facts[b"SINEX.content_hash".as_slice()],
        blake3::hash(&media).to_hex().as_bytes()
    );
    assert_eq!(
        facts[b"SINEX.size".as_slice()],
        media.len().to_string().as_bytes()
    );
    assert_eq!(facts[b"SINEX.media_type".as_slice()], b"video/mp4");
    let events = parse(&first);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["located"], true);
    assert_eq!(events[0].payload["media_type"], "video/mp4");
    assert_eq!(events[0].payload["size"], media.len());
    assert_eq!(
        events[0].payload["content"],
        format!("blake3:{}", blake3::hash(&media).to_hex())
    );
    assert_eq!(events[1].payload["located"], false);
    assert_eq!(
        events[1].payload["content"],
        format!("blake3:{}", blake3::hash(b"retained note").to_hex())
    );
    assert!(
        driver
            .poll(&accepted_cursor(&first), &mut state)
            .unwrap()
            .material
            .is_none()
    );
    std::fs::remove_file(root.path().join("movie.bin")).unwrap();
    // Replaying the listing needs no read of the external original.
    assert_eq!(parse(&first)[0].payload, events[0].payload);
}

#[devkit::test]
fn a_moved_located_file_keeps_its_hash_and_records_the_old_path_removed() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("before.mp4"), media_fixture()).unwrap();
    let driver = located_driver(root.path().into());
    let mut state = DirState::default();
    let first = driver.poll(&Value::Null, &mut state).unwrap();
    let original = parse(&first).remove(0);
    std::fs::rename(
        root.path().join("before.mp4"),
        root.path().join("after.mp4"),
    )
    .unwrap();
    let moved = driver.poll(&accepted_cursor(&first), &mut state).unwrap();
    let events = parse(&moved);
    let present = events
        .iter()
        .find(|e| e.payload["path"] == "after.mp4")
        .unwrap();
    let removed = events
        .iter()
        .find(|e| e.payload["path"] == "before.mp4")
        .unwrap();
    assert_eq!(present.payload["content"], original.payload["content"]);
    assert_eq!(present.payload["located"], true);
    assert_eq!(removed.record_state, sinex_model::RecordState::Removed);
    assert_ne!(
        present.record_hash, original.record_hash,
        "path remains part of record identity"
    );
}

#[devkit::test]
fn located_record_hash_follows_content_even_at_the_same_path_and_size() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("movie.mp4");
    let mut bytes = media_fixture();
    std::fs::write(&path, &bytes).unwrap();
    let driver = located_driver(root.path().into());
    let mut state = DirState::default();
    let first = driver.poll(&Value::Null, &mut state).unwrap();
    bytes[8192] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let changed = driver.poll(&accepted_cursor(&first), &mut state).unwrap();
    let old = parse(&first).remove(0);
    let new = parse(&changed).remove(0);
    assert_eq!(old.payload["size"], new.payload["size"]);
    assert_ne!(old.payload["content"], new.payload["content"]);
    assert_ne!(
        old.record_hash, new.record_hash,
        "empty listing bytes must not mark changed media as repeated"
    );
}

#[devkit::test]
fn only_discovered_download_roots_get_a_fresh_located_binding() {
    let root = tempfile::tempdir().unwrap();
    let documents = root.path().join("documents");
    let downloads = root.path().join("downloads");
    std::fs::create_dir(&documents).unwrap();
    std::fs::create_dir(&downloads).unwrap();
    let mut context = sinex_source_core::discovery::DiscoveryContext {
        host: "example-host".into(),
        ..Default::default()
    };
    context
        .input_roots
        .insert("documents".into(), vec![documents.clone()]);
    context
        .input_roots
        .insert("downloads".into(), vec![downloads.clone()]);
    let found = discover(&FORMAT, &context);
    let binding = |path: &Path| {
        found
            .iter()
            .find(|i| i.input.as_deref() == Some(path))
            .unwrap()
            .bindings
            .first()
            .unwrap()
    };
    assert!(binding(&downloads).name.contains("#located@"));
    assert!(
        Options::from_settings(&binding(&downloads).settings)
            .unwrap()
            .locate_media
    );
    assert!(binding(&documents).name.contains("#directory@"));
    assert!(
        !Options::from_settings(&binding(&documents).settings)
            .unwrap()
            .locate_media
    );
}
