use super::*;
use crate::drivers::dir_snapshot::DirState;
use crate::{DrainInput, Limits, drain};
use sinex_model::{ReadingOrder, RecordOutcome};
pub(super) fn parse(acquired: &crate::Acquired) -> Vec<sinex_model::Event> {
    let m = acquired.material.as_ref().unwrap();
    drain(
        &DocumentParser,
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
    let driver = driver(root.to_path_buf(), "example-host".into());
    let acquired = driver.poll(&Value::Null, &mut DirState::default()).unwrap();
    let first = parse(&acquired);
    assert_eq!(first.len(), 3);
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
    assert_eq!(
        parse(&first)[0].equivalence_key,
        parse(&second)[0].equivalence_key
    );
    let m = second.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    std::fs::remove_file(temp.path().join("a.txt")).unwrap();
    let removed = parse(&driver.poll(&cursor, &mut state).unwrap());
    assert_eq!(removed[0].record_state, sinex_model::RecordState::Removed);
    assert!(removed[0].payload["content"].is_null());
}
#[devkit::test]
fn acquisition_requires_scope_and_rejects_notes_library_and_symlink_overlap() {
    let temp = tempfile::tempdir().unwrap();
    let mut settings = Settings::new("example-host");
    assert!(validate(temp.path(), &settings).is_err());
    settings.options = serde_json::json!({"capture_documents":true,"existing_roots":[]});
    assert!(validate(temp.path(), &settings).is_ok());
    settings.options["existing_roots"] = serde_json::json!([temp.path().join("library")]);
    assert!(validate(temp.path(), &settings).is_err());
    settings.options["existing_roots"] = serde_json::json!([]);
    std::os::unix::fs::symlink(temp.path(), temp.path().join("link")).unwrap();
    assert!(validate(&temp.path().join("link"), &settings).is_err());
    assert!(!select("note.md"));
    assert!(!select("notes.MARKDOWN"));
    assert!(!select(".git/config"));
}

#[devkit::test]
fn reader_restart_rebuilds_file_facts_and_keeps_held_bytes() {
    use crate::drivers::prepare;
    use sinex_model::{BatchKind, RunId};
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("a.txt"), b"captured").unwrap();
    let crate::Reading::Polled(mut reader) =
        crate::binding::walked(driver(temp.path().into(), "example-host".into()))
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
        parser: DocumentParser,
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
    let crate::Reading::Polled(mut restarted) =
        crate::binding::walked(driver(temp.path().into(), "example-host".into()))
    else {
        panic!()
    };
    assert_eq!(restarted.history_from(&progress.cursor), Some(id));
    restarted
        .restore(
            &progress.cursor,
            crate::StoredMaterial {
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
impl crate::MaterialSink for Sink {
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
impl crate::drain::RecordRead for Bytes {
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
