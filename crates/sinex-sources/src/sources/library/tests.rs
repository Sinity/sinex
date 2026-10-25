use super::*;
use crate::{DrainInput, Limits, drain};
use sinex_model::event_types::library::{Level, Target};
use sinex_model::{RecordOutcome, RecordState};

struct MemorySink(std::sync::Mutex<HashMap<String, Vec<u8>>>);
impl MaterialSink for MemorySink {
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
        self.0.lock().unwrap().insert(name.clone(), bytes.to_vec());
        Ok(name)
    }
}

fn tmp() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-library-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn png(w: u32, h: u32) -> Vec<u8> {
    let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    b.extend_from_slice(&w.to_be_bytes());
    b.extend_from_slice(&h.to_be_bytes());
    b.extend_from_slice(&[8, 6, 0, 0, 0, 0, 0, 0, 0]);
    b
}

fn read(a: &Acquired, parser: &ScanParser) -> Vec<Acquisition> {
    let m = a.material.as_ref().expect("a material");
    let out = drain(
        parser,
        DrainInput {
            source: "library.scan",
            class: Class::Open,
            material: m.id,
            bytes: m.bytes.as_slice().into(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &*m.reading_order,
            limits: Limits::default(),
            manifest: &m.manifest,
        },
    )
    .unwrap();
    out.into_iter()
        .flat_map(|c| c.outcomes)
        .flat_map(|o| match o {
            RecordOutcome::Events { events, .. } => events,
            RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
        .map(|e| {
            assert_eq!(e.event_type, "library.acquisition");
            e.payload_as::<Acquisition>().unwrap()
        })
        .collect()
}

fn scan(root: &Path) -> LibraryScan {
    LibraryScan {
        root: root.to_path_buf(),
        host: "h".into(),
        keep_bytes: true,
        probe: false,
        bytes_per_poll: 1 << 30,
        only: None,
    }
}

#[devkit::test]
fn sidecars_named_by_stem_are_not_items() {
    let root = tmp();
    std::fs::write(root.join("pic.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("pic.json"), b"{}").unwrap();
    std::fs::write(root.join("pic.png.txt"), b"tag").unwrap();
    std::fs::write(root.join("notes.json"), b"{}").unwrap();
    let sink = MemorySink(Default::default());
    let mut mem = ScanState::default();
    let first = scan(&root).poll(&Value::Null, &mut mem, &sink).unwrap();
    let mut paths: Vec<String> = read(&first, &ScanParser)
        .into_iter()
        .map(|a| a.path.rsplit('/').next().unwrap().to_owned())
        .collect();
    paths.sort();
    // `notes.json` describes no file beside it, so it is an item.
    assert_eq!(paths, ["notes.json", "pic.png"]);
}

#[devkit::test]
fn a_single_file_scan_reads_and_misses_nothing_else() {
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("two.png"), png(20, 20)).unwrap();
    let sink = MemorySink(Default::default());
    let mut mem = ScanState::default();
    let all = scan(&root).poll(&Value::Null, &mut mem, &sink).unwrap();
    assert_eq!(read(&all, &ScanParser).len(), 2);

    std::fs::write(root.join("one.png"), png(30, 30)).unwrap();
    std::fs::write(root.join("three.png"), png(40, 40)).unwrap();
    let only = LibraryScan {
        only: Some("one.png".into()),
        ..scan(&root)
    };
    let seen = read(
        &only.poll(&Value::Null, &mut mem, &sink).unwrap(),
        &ScanParser,
    );
    assert_eq!(seen.len(), 1, "only the named file: {seen:?}");
    assert!(seen[0].path.ends_with("one.png"));
    assert!(seen[0].vanished_at.is_none());
}

#[devkit::test]
fn scan_describes_stores_and_follows_renames_edits_and_removals() {
    let root = tmp();
    std::fs::create_dir_all(root.join("a")).unwrap();
    std::fs::write(root.join("a/one.png"), png(640, 480)).unwrap();
    std::fs::write(root.join("two.bin"), b"\0\x01\x02 not media").unwrap();
    std::fs::write(root.join(".hidden"), b"x").unwrap();
    let sink = MemorySink(Default::default());
    let s = scan(&root);
    let mut mem = ScanState::default();
    let parser = ScanParser;

    let first = s.poll(&Value::Null, &mut mem, &sink).unwrap();
    let seen = read(&first, &parser);
    assert_eq!(seen.len(), 2);
    let one = seen.iter().find(|a| a.path.ends_with("one.png")).unwrap();
    assert_eq!(one.media_type, "image/png");
    assert_eq!((one.width, one.height), (Some(640), Some(480)));
    assert!(one.stored && one.content.starts_with("blake3:"));
    assert_eq!(one.sha256.len(), 64);
    assert_eq!(one.md5.len(), 32);
    assert!(sink.0.lock().unwrap().contains_key(&one.content));
    let other = seen.iter().find(|a| a.path.ends_with("two.bin")).unwrap();
    assert_eq!(other.media_type, "application/octet-stream");

    // Nothing changed: no material.
    let again = s.poll(&Value::Null, &mut mem, &sink).unwrap();
    assert!(again.material.is_none());

    // Rename one.png, edit two.bin in place, delete nothing else.
    std::fs::rename(root.join("a/one.png"), root.join("renamed.png")).unwrap();
    std::fs::write(root.join("two.bin"), b"\0\x01\x02 edited").unwrap();
    let third = s.poll(&Value::Null, &mut mem, &sink).unwrap();
    let seen = read(&third, &parser);
    let moved = seen
        .iter()
        .find(|a| a.path.ends_with("renamed.png"))
        .unwrap();
    assert!(moved.moved_from.as_deref().unwrap().ends_with("a/one.png"));
    let edited = seen.iter().find(|a| a.path.ends_with("two.bin")).unwrap();
    assert_eq!(
        edited.previous_content.as_deref(),
        Some(other.content.as_str())
    );
    // The old path's copy is gone, after the line that names the move.
    let gone: Vec<_> = seen.iter().filter(|a| a.vanished_at.is_some()).collect();
    assert_eq!(gone.len(), 1);
    assert!(gone[0].path.ends_with("a/one.png"));
    let moved_at = seen
        .iter()
        .position(|a| a.path.ends_with("renamed.png"))
        .unwrap();
    let gone_at = seen.iter().position(|a| a.vanished_at.is_some()).unwrap();
    assert!(moved_at < gone_at);

    std::fs::remove_file(root.join("renamed.png")).unwrap();
    let fourth = s.poll(&Value::Null, &mut mem, &sink).unwrap();
    let seen = read(&fourth, &parser);
    assert_eq!(seen.len(), 1);
    assert!(seen[0].vanished_at.is_some());
    std::fs::remove_dir_all(&root).unwrap();
}

#[devkit::test]
fn equal_bytes_hash_equal_wherever_they_are() {
    let a = json!({"host": "h", "root": "/r", "path": "/r/a", "content": "blake3:x", "size": 1,
        "sha256": "s", "md5": "m", "media_type": "image/png", "stored": true});
    let b = json!({"host": "h", "root": "/q", "path": "/q/b", "content": "blake3:x", "size": 1,
        "sha256": "s", "md5": "m", "media_type": "image/png", "stored": false});
    let hash = |v: &Value| {
        <Map<String, Value> as crate::RecordShape>::hash(
            v.to_string().as_bytes(),
            ScanParser::VOLATILE,
        )
    };
    assert_eq!(hash(&a), hash(&b));
}

#[devkit::test]
fn sniffs_types_from_bytes_not_names() {
    assert_eq!(sniff(b"\xFF\xD8\xFF\xE0"), "image/jpeg");
    assert_eq!(sniff(b"GIF89a.."), "image/gif");
    assert_eq!(sniff(b"RIFF\0\0\0\0WEBPVP8 "), "image/webp");
    assert_eq!(sniff(b"\0\0\0\x20ftypisom"), "video/mp4");
    assert_eq!(sniff(b"\x1A\x45\xDF\xA3 webm"), "video/webm");
    assert_eq!(sniff(b"%PDF-1.7"), "application/pdf");
    assert_eq!(sniff(b"hello"), "text/plain");
    let gif = b"GIF89a\x40\x01\xF0\x00";
    assert_eq!(dimensions(gif, "image/gif").0, Some(320));
    assert_eq!(dimensions(gif, "image/gif").1, Some(240));
}

#[devkit::test]
fn requests_take_author_and_time_from_the_manifest() {
    let item = Item {
        item: "i1".into(),
        revision: 1,
        replaces: None,
        content: "blake3:x".into(),
        media_type: "image/png".into(),
        acquisitions: vec!["h:/r/a.png".into()],
        label: Some("a.png".into()),
        by: "forged".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: None,
    };
    let tag = Assignment {
        assignment: "as1".into(),
        target: Target {
            item: "i1".into(),
            ..Target::default()
        },
        field: "tag".into(),
        value: Some("place:croatia".into()),
        level: Level::Override,
        source_ref: None,
        by: "forged".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: Some(Timestamp::from_unix_secs(2).unwrap()),
    };
    let a = request_material(
        &[LibraryRecord::Item(item), LibraryRecord::Assignment(tag)],
        "operator",
        "h",
        "test",
    );
    let m = a.material.as_ref().unwrap();
    let out = drain(
        &RequestParser,
        DrainInput {
            source: "library.request",
            class: Class::Open,
            material: m.id,
            bytes: m.bytes.as_slice().into(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &*m.reading_order,
            limits: Limits::default(),
            manifest: &m.manifest,
        },
    )
    .unwrap();
    let events: Vec<_> = out
        .into_iter()
        .flat_map(|c| c.outcomes)
        .flat_map(|o| match o {
            RecordOutcome::Events { events, .. } => events,
            RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
        .collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "library.item");
    assert_eq!(events[0].payload["by"], "operator");
    assert_ne!(events[0].payload["at"], json!("1970-01-01T00:00:01Z"));
    assert_eq!(events[1].event_type, "library.assignment");
    assert_eq!(events[1].record_state, RecordState::Removed);
}

#[devkit::test]
fn request_citation_facts_preserve_payload_identity_and_refuse_supplied_invalid_evidence() {
    let blob = format!("blake3:{}", blake3::hash(b"synthetic preview"));
    let record = LibraryRecord::Rendition(Rendition {
        rendition: "r".into(),
        item: "i".into(),
        revision: 1,
        role: sinex_model::event_types::library::RenditionRole::Preview,
        content: blob.clone(),
        media_type: "image/webp".into(),
        width: Some(1),
        height: Some(1),
        duration_ms: None,
        made_from: None,
        made_with: Default::default(),
        by: String::new(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: None,
    });
    let acquired = request_material(&[record], "operator", "h", "preview");
    let material = acquired.material.unwrap();
    let outcomes = |manifest: &Value| {
        drain(
            &RequestParser,
            DrainInput {
                source: REQUEST.name,
                class: Class::Open,
                material: material.id,
                bytes: material.bytes.as_slice().into(),
                records: &material.records,
                carry: Value::Null,
                reading_order: &*material.reading_order,
                limits: Limits::default(),
                manifest,
            },
        )
        .unwrap()
        .into_iter()
        .flat_map(|chunk| chunk.outcomes)
        .collect::<Vec<_>>()
    };
    let original = outcomes(&material.manifest);
    let RecordOutcome::Events { events: old, .. } = &original[0] else {
        panic!()
    };
    assert!(
        old[0].cites.is_empty(),
        "old absent facts do not invent media ownership"
    );
    let citation = sinex_model::Cited {
        blob: blob.clone(),
        size: 17,
    };
    let mut manifest = material.manifest.clone();
    manifest["cited_blobs"] = json!([citation]);
    let current = outcomes(&manifest);
    let RecordOutcome::Events { events: new, .. } = &current[0] else {
        panic!()
    };
    assert_eq!(new[0].cites, vec![citation]);
    assert_eq!(new[0].payload, old[0].payload);
    assert_eq!(new[0].equivalence_key, old[0].equivalence_key);
    assert_eq!(new[0].record_hash, old[0].record_hash);
    assert_eq!(new[0].reading_order, old[0].reading_order);
    for invalid in [
        Value::Null,
        json!([]),
        json!([{}]),
        json!("opaque"),
        json!([{"blob":blob,"size":17},{"blob":blob,"size":17}]),
        json!([{"blob":"blake3:invalid","size":17}]),
        json!([{"blob":format!("blake3:{}",blake3::hash(b"different")),"size":17}]),
    ] {
        manifest["cited_blobs"] = invalid;
        assert!(matches!(
            &outcomes(&manifest)[0],
            RecordOutcome::Failure { .. }
        ));
    }
}
