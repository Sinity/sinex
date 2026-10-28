use super::files::sniff;
use super::*;
use serde_json::{Map, Value, json};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{Assignment, FileCopy, LibraryRecord, Preview};
use sinex_model::event_types::library::{Level, Target};
use sinex_model::{RecordOutcome, RecordState};
use sinex_source_core::Parser;
use sinex_source_core::drivers::Acquired;
use sinex_source_core::{DrainInput, Limits, drain};
use std::path::{Path, PathBuf};

fn tmp() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-library-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub(super) fn png(w: u32, h: u32) -> Vec<u8> {
    let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    b.extend_from_slice(&w.to_be_bytes());
    b.extend_from_slice(&h.to_be_bytes());
    b.extend_from_slice(&[8, 6, 0, 0, 0, 0, 0, 0, 0]);
    b
}

fn read_events(a: &Acquired, parser: &ScanParser) -> Vec<sinex_model::Event> {
    let m = a.material.as_ref().expect("a material");
    let out = drain(
        parser,
        DrainInput {
            source: "library.scan",
            class: Class::Open,
            material: m.id,
            bytes: m.record_bytes(),
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
        .collect()
}

pub(super) fn read(a: &Acquired, parser: &ScanParser) -> Vec<FileCopy> {
    read_events(a, parser)
        .into_iter()
        .map(|event| event.payload_as().unwrap())
        .collect()
}

pub(super) fn scan(root: &Path) -> LibraryScan {
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
    let mut mem = ScanState::default();
    let first = scan(&root).poll(&Value::Null, &mut mem).unwrap();
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
    let mut mem = ScanState::default();
    let all = scan(&root).poll(&Value::Null, &mut mem).unwrap();
    assert_eq!(read(&all, &ScanParser).len(), 2);
    assert!(
        scan(&root)
            .poll(&Value::Null, &mut mem)
            .unwrap()
            .material
            .is_none()
    );

    std::fs::write(root.join("one.png"), png(30, 30)).unwrap();
    std::fs::write(root.join("three.png"), png(40, 40)).unwrap();
    let only = LibraryScan {
        only: Some("one.png".into()),
        ..scan(&root)
    };
    let seen = read(&only.poll(&Value::Null, &mut mem).unwrap(), &ScanParser);
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
    let s = scan(&root);
    let mut mem = ScanState::default();
    let parser = ScanParser;

    let first = s.poll(&Value::Null, &mut mem).unwrap();
    let seen = read(&first, &parser);
    assert_eq!(seen.len(), 2);
    let one = seen.iter().find(|a| a.path.ends_with("one.png")).unwrap();
    assert_eq!(one.media_type, "image/png");
    assert_eq!((one.width, one.height), (Some(640), Some(480)));
    assert!(one.stored && one.content.starts_with("blake3:"));
    assert_eq!(one.sha256.len(), 64);
    assert_eq!(one.md5.len(), 32);
    let owned = &first.material.as_ref().unwrap().owned_files;
    assert!(
        owned
            .iter()
            .any(|f| f.blob == one.content && f.path.ends_with("a/one.png"))
    );
    let other = seen.iter().find(|a| a.path.ends_with("two.bin")).unwrap();
    assert_eq!(other.media_type, "application/octet-stream");

    // Nothing changed: no material.
    let again = s.poll(&Value::Null, &mut mem).unwrap();
    assert!(again.material.is_none());

    // Rename one.png, edit two.bin in place, delete nothing else.
    std::fs::rename(root.join("a/one.png"), root.join("renamed.png")).unwrap();
    std::fs::write(root.join("two.bin"), b"\0\x01\x02 edited").unwrap();
    let third = s.poll(&Value::Null, &mut mem).unwrap();
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

    assert!(s.poll(&Value::Null, &mut mem).unwrap().material.is_none());
    std::fs::remove_file(root.join("renamed.png")).unwrap();
    let fourth = s.poll(&Value::Null, &mut mem).unwrap();
    let seen = read(&fourth, &parser);
    assert_eq!(seen.len(), 1);
    assert!(seen[0].vanished_at.is_some());
    std::fs::remove_dir_all(&root).unwrap();
}

struct Body(Vec<u8>);
impl sinex_source_core::drain::RecordRead for Body {
    fn length(&self) -> u64 {
        self.0.len() as u64
    }
    fn read_exact_at(&self, start: u64, out: &mut [u8]) -> std::io::Result<()> {
        out.copy_from_slice(&self.0[start as usize..start as usize + out.len()]);
        Ok(())
    }
}

/// What the store holds for the material `acquired` read, and the cursor
/// once all of it is admitted.
pub(super) fn stored(acquired: &Acquired) -> (sinex_source_core::binding::StoredMaterial, Value) {
    let m = acquired.material.as_ref().unwrap();
    let length = m.records.last().unwrap().1;
    let bytes = m.record_bytes().record(0, length).unwrap().into_owned();
    let stored = sinex_source_core::binding::StoredMaterial {
        reference: sinex_model::MaterialRef {
            id: m.id,
            input: None,
            blob: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
            length: length as u64,
            part_index: m.part_index,
            previous_part: m.previous_part,
            parser: "library-scan".into(),
            parser_version: 1,
            manifest: m.manifest.clone(),
        },
        bytes: std::sync::Arc::new(Body(bytes)),
        same_revision: true,
    };
    (stored, (m.cursor_at)(length).unwrap())
}

#[devkit::test]
fn a_restarted_scan_keeps_item_keys_and_reads_only_changes() {
    use sinex_source_core::binding::Reader;
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("two.png"), png(20, 20)).unwrap();
    let mut first = LibraryReader::new(scan(&root));
    let a = first.poll(&Value::Null).unwrap();
    let (part_a, cursor_a) = stored(&a);
    let memory = a.material.as_ref().unwrap().memory.clone();
    assert!(memory.is_some(), "the first part saves what the scan knows");
    assert!(first.poll(&cursor_a).unwrap().material.is_none());

    std::fs::rename(root.join("one.png"), root.join("moved.png")).unwrap();
    let b = first.poll(&cursor_a).unwrap();
    let (part_b, cursor_b) = stored(&b);

    // A restart from both parts knows every file: nothing to read.
    let mut again = LibraryReader::new(scan(&root));
    assert_eq!(again.history_from(&cursor_b), Some(part_b.reference.id));
    assert!(again.restore(&cursor_b, part_b).unwrap());
    assert!(again.restore(&cursor_b, part_a).unwrap());
    again.restored().unwrap();
    assert!(again.poll(&cursor_b).unwrap().material.is_none());

    // From the newest part and the memory saved at the first one.
    let (part_b, _) = stored(&b);
    let mut saved = LibraryReader::new(scan(&root));
    saved.restore(&cursor_b, part_b).unwrap();
    saved.restore_memory(&memory.unwrap()).unwrap();
    saved.restored().unwrap();
    std::fs::write(root.join("two.png"), png(30, 30)).unwrap();
    let c = saved.poll(&cursor_b).unwrap();
    let seen = read(&c, &ScanParser);
    assert_eq!(
        seen.len(),
        1,
        "only the changed file is scan evidence: {seen:?}"
    );
    assert!(seen.iter().any(|copy| copy.path.ends_with("two.png")));
    std::fs::remove_dir_all(&root).unwrap();
}

#[devkit::test]
fn equal_bytes_hash_equal_wherever_they_are() {
    let a = json!({"host": "h", "root": "/r", "path": "/r/a", "content": "blake3:x", "size": 1,
        "sha256": "s", "md5": "m", "media_type": "image/png", "stored": true});
    let b = json!({"host": "h", "root": "/q", "path": "/q/b", "content": "blake3:x", "size": 1,
        "sha256": "s", "md5": "m", "media_type": "image/png", "stored": false});
    let hash = |v: &Value| {
        <Map<String, Value> as sinex_source_core::RecordShape>::hash(
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
    let a = request_material(&[LibraryRecord::Assignment(tag)], "operator", "h", "test");
    let m = a.material.as_ref().unwrap();
    let out = drain(
        &RequestParser,
        DrainInput {
            source: "library.request",
            class: Class::Open,
            material: m.id,
            bytes: m.record_bytes(),
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
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "library.assignment");
    assert_eq!(events[0].payload["by"], "operator");
    assert_ne!(events[0].payload["at"], json!("1970-01-01T00:00:01Z"));
    assert_eq!(events[0].record_state, RecordState::Removed);
}

#[devkit::test]
fn request_citation_facts_preserve_payload_identity_and_refuse_supplied_invalid_evidence() {
    let blob = format!("blake3:{}", blake3::hash(b"synthetic preview"));
    let record = LibraryRecord::Preview(Preview {
        rendition: "r".into(),
        item: "i".into(),
        revision: "blake3:x".into(),
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
    let outcomes = |manifest: &sinex_model::Manifest| {
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
        old[0].owned_blobs.is_empty(),
        "old absent facts do not invent media ownership"
    );
    let citation = sinex_model::OwnedBlob {
        blob: blob.clone(),
        size: 17,
    };
    let mut manifest = material.manifest.clone();
    manifest
        .details
        .insert("owned_blobs".into(), json!([citation]));
    let current = outcomes(&manifest);
    let RecordOutcome::Events { events: new, .. } = &current[0] else {
        panic!()
    };
    assert_eq!(new[0].owned_blobs, vec![citation]);
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
        manifest.details.insert("owned_blobs".into(), invalid);
        assert!(matches!(
            &outcomes(&manifest)[0],
            RecordOutcome::Failure { .. }
        ));
    }
}

#[devkit::test]
fn library_format_plans_one_file_without_reading_its_neighbours() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("selected.txt"), b"synthetic selected file").unwrap();
    std::fs::write(dir.path().join("other.txt"), b"synthetic other file").unwrap();
    let unrelated = dir.path().join("unrelated");
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::set_permissions(&unrelated, std::fs::Permissions::from_mode(0o000)).unwrap();
    let mut settings = sinex_source_core::Settings::new("synthetic");
    settings.options = serde_json::json!({"reference": true});
    let bindings = (FORMAT.plan.unwrap())(&dir.path().join("selected.txt"), &settings).unwrap();
    assert_eq!(bindings.len(), 1);
    let binding = &bindings[0];
    assert_eq!(binding.source, SCAN.name);
    let mut reader = (FORMAT.reader.unwrap())(binding)
        .unwrap()
        .into_polled()
        .unwrap();
    let acquired = reader.poll(&serde_json::Value::Null);
    std::fs::set_permissions(&unrelated, std::fs::Permissions::from_mode(0o700)).unwrap();
    let acquired = acquired.unwrap();
    let material = acquired.material.unwrap();
    assert_eq!(material.records.len(), 2);
    assert!(material.owned_files.is_empty());
    let bytes = crate::test_support::body(&material);
    let line: serde_json::Value =
        serde_json::from_slice(&bytes[material.records[0].0..material.records[0].1]).unwrap();
    assert!(line["path"].as_str().unwrap().ends_with("selected.txt"));
    assert_eq!(line["stored"], false);
    let (start, end) = material.records[1];
    let completion: Value = serde_json::from_slice(&bytes[start..end]).unwrap();
    assert_eq!(completion["record"], "complete");
    assert_eq!(completion["only"], "selected.txt");
}

#[devkit::test]
fn equal_content_moves_prefer_each_files_device_and_inode() {
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("two.png"), png(10, 10)).unwrap();
    let scanner = scan(&root);
    let mut state = ScanState::default();
    let first = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    let one = first
        .iter()
        .find(|copy| copy.path.ends_with("one.png"))
        .unwrap();
    let two = first
        .iter()
        .find(|copy| copy.path.ends_with("two.png"))
        .unwrap();
    assert_ne!(one.item, two.item);
    assert!(
        scanner
            .poll(&Value::Null, &mut state)
            .unwrap()
            .material
            .is_none()
    );
    std::fs::rename(root.join("one.png"), root.join("z.png")).unwrap();
    std::fs::rename(root.join("two.png"), root.join("a.png")).unwrap();
    let moved = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    assert_eq!(
        moved
            .iter()
            .find(|copy| copy.path.ends_with("z.png"))
            .unwrap()
            .item,
        one.item
    );
    assert_eq!(
        moved
            .iter()
            .find(|copy| copy.path.ends_with("a.png"))
            .unwrap()
            .item,
        two.item
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn a_copy_then_removal_keeps_identity_without_the_same_inode() {
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    let scanner = scan(&root);
    let mut state = ScanState::default();
    let first = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    assert!(
        scanner
            .poll(&Value::Null, &mut state)
            .unwrap()
            .material
            .is_none()
    );
    std::fs::copy(root.join("one.png"), root.join("two.png")).unwrap();
    std::fs::remove_file(root.join("one.png")).unwrap();
    let moved = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    let present = moved
        .iter()
        .find(|copy| copy.vanished_at.is_none())
        .unwrap();
    assert_eq!(present.item, first[0].item);
    assert_eq!(present.item_key, first[0].item_key);
    assert_ne!(present.inode, first[0].inode);
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn three_unchanged_polls_write_zero_copy_events_and_reuse_hashes_after_restart() {
    use sinex_source_core::binding::Reader;
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("two.png"), png(20, 20)).unwrap();
    let mut reader = LibraryReader::new(scan(&root));
    let first = reader.poll(&Value::Null).unwrap();
    let (_, cursor) = stored(&first);
    let memory = first.material.as_ref().unwrap().memory.clone().unwrap();
    assert_eq!(read(&first, &ScanParser).len(), 2);
    assert_eq!(reader.state.files_hashed(), 2);
    for _ in 0..3 {
        let unchanged = reader.poll(&cursor).unwrap();
        assert!(
            unchanged.material.is_none(),
            "an unchanged poll writes zero copies"
        );
    }
    assert_eq!(reader.state.files_hashed(), 2);

    let mut restarted = LibraryReader::new(scan(&root));
    restarted.restore_memory(&memory).unwrap();
    restarted.restored().unwrap();
    assert!(restarted.poll(&cursor).unwrap().material.is_none());
    assert_eq!(restarted.state.files_hashed(), 0);
    let mut changed = png(30, 30);
    changed.push(0);
    std::fs::write(root.join("two.png"), changed).unwrap();
    assert_eq!(
        read(&restarted.poll(&cursor).unwrap(), &ScanParser).len(),
        1
    );
    assert_eq!(restarted.state.files_hashed(), 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn a_scan_records_completion_once_after_all_parts() {
    let root = tmp();
    std::fs::write(root.join("one.png"), png(10, 10)).unwrap();
    std::fs::write(root.join("two.png"), png(20, 20)).unwrap();
    let scanner = LibraryScan {
        bytes_per_poll: 1,
        ..scan(&root)
    };
    let mut state = ScanState::default();
    let mut cursor = Value::Null;
    let mut copies = 0;
    let mut completions = 0;
    for expected_complete in [false, true] {
        let acquired = scanner.poll(&cursor, &mut state).unwrap();
        let material = acquired.material.as_ref().unwrap();
        assert_eq!(
            material.manifest.details["walk_complete"],
            expected_complete
        );
        copies += read(&acquired, &ScanParser).len();
        for &(start, end) in &material.records {
            if matches!(
                serde_json::from_slice::<ScanLine>(&material.bytes[start..end]).unwrap(),
                ScanLine::Complete { .. }
            ) {
                completions += 1;
            }
        }
        cursor = (material.cursor_at)(material.bytes.len()).unwrap();
    }
    assert_eq!((copies, completions), (2, 1));
    for _ in 0..3 {
        assert!(
            scanner
                .poll(&cursor, &mut state)
                .unwrap()
                .material
                .is_none()
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn ambiguous_equal_content_moves_create_new_items() {
    let root = tmp();
    for path in ["one.png", "two.png"] {
        std::fs::write(root.join(path), png(10, 10)).unwrap();
    }
    let scanner = scan(&root);
    let mut state = ScanState::default();
    let first = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    for path in ["one.png", "two.png"] {
        state.files.get_mut(path).unwrap().inode = None;
    }
    std::fs::rename(root.join("one.png"), root.join("a.png")).unwrap();
    std::fs::rename(root.join("two.png"), root.join("b.png")).unwrap();
    let moved = read(
        &scanner.poll(&Value::Null, &mut state).unwrap(),
        &ScanParser,
    );
    let present: Vec<_> = moved
        .iter()
        .filter(|copy| copy.vanished_at.is_none())
        .collect();
    assert_eq!(present.len(), 2);
    for copy in present {
        assert!(copy.moved_from.is_none());
        assert!(first.iter().all(|old| old.item != copy.item));
    }
    assert_eq!(
        moved
            .iter()
            .filter(|copy| copy.vanished_at.is_some())
            .count(),
        2
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn scans_with_equal_timestamps_are_independent_evidence() {
    let root = tmp();
    std::fs::write(root.join("one.txt"), "one").unwrap();
    let scanner = scan(&root);
    let mut state = ScanState::default();
    let mut first = scanner.poll(&Value::Null, &mut state).unwrap();
    let (_, cursor) = stored(&first);
    assert!(
        scanner
            .poll(&cursor, &mut state)
            .unwrap()
            .material
            .is_none()
    );
    std::fs::write(root.join("one.txt"), "changed").unwrap();
    let mut second = scanner.poll(&cursor, &mut state).unwrap();
    for acquired in [&mut first, &mut second] {
        acquired.material.as_mut().unwrap().manifest.read_at_ns = Some(1_000_000_000);
    }

    let first = read_events(&first, &ScanParser);
    let second = read_events(&second, &ScanParser);
    assert_eq!(
        first[0].payload["scanned_at"],
        second[0].payload["scanned_at"]
    );
    assert_ne!(first[0].record_hash, second[0].record_hash);
    assert_ne!(first[0].equivalence_key, second[0].equivalence_key);
    assert_ne!(first[0].payload["scan"], second[0].payload["scan"]);
    assert_eq!(first[0].payload["part_index"], 0);
    assert_eq!(second[0].payload["part_index"], 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn completion_records_do_not_advance_the_file_memory_checkpoint() {
    use sinex_source_core::binding::Reader;
    let root = tmp();
    for index in 0..12 {
        std::fs::write(root.join(format!("{index}.bin")), "before").unwrap();
    }
    let mut reader = LibraryReader::new(scan(&root));
    let first = reader.poll(&Value::Null).unwrap();
    assert!(first.material.as_ref().unwrap().memory.is_some());
    let (_, mut cursor) = stored(&first);
    for index in 0..3 {
        std::fs::write(
            root.join(format!("{index}.bin")),
            format!("after edit {index}"),
        )
        .unwrap();
        let acquired = reader.poll(&cursor).unwrap();
        assert_eq!(read(&acquired, &ScanParser).len(), 1);
        assert_eq!(
            acquired.material.as_ref().unwrap().memory.is_some(),
            index == 2,
            "save memory after a quarter of the twelve files changed"
        );
        (_, cursor) = stored(&acquired);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn a_reused_path_gets_a_separate_key_while_the_moved_item_is_present() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.png"), png(10, 10)).unwrap();
    let scanner = scan(dir.path());
    let mut state = ScanState::default();
    let first = scanner.poll(&Value::Null, &mut state).unwrap();
    let old = read(&first, &ScanParser).remove(0);
    let (_, cursor) = stored(&first);
    std::fs::rename(dir.path().join("a.png"), dir.path().join("b.png")).unwrap();
    let moved = scanner.poll(&cursor, &mut state).unwrap();
    let (_, cursor) = stored(&moved);
    std::fs::write(dir.path().join("a.png"), png(20, 20)).unwrap();
    let acquired = scanner.poll(&cursor, &mut state).unwrap();
    let new = read(&acquired, &ScanParser).remove(0);
    assert_ne!(new.item, old.item);
    assert_eq!(state.files["b.png"].item_key, old.item_key);
    assert!(
        new.item_key
            .ends_with(&acquired.material.as_ref().unwrap().id.to_string())
    );
    let mut restarted = ScanState::default();
    let (bytes, cursor) = stored(&acquired);
    restarted
        .remember_part(dir.path(), bytes.bytes.as_ref(), bytes.reference.length)
        .unwrap();
    let (bytes, _) = stored(&moved);
    restarted
        .remember_part(dir.path(), bytes.bytes.as_ref(), bytes.reference.length)
        .unwrap();
    restarted.remembered.clear();
    assert_eq!(restarted.files["a.png"].item_key, new.item_key);
    assert_eq!(restarted.files["b.png"].item_key, old.item_key);
    assert!(
        scanner
            .poll(&cursor, &mut restarted)
            .unwrap()
            .material
            .is_none()
    );
}

#[devkit::test]
fn one_missing_path_and_several_new_equal_copies_report_ambiguity_across_parts() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = png(10, 10);
    std::fs::write(dir.path().join("old.png"), &bytes).unwrap();
    let mut scanner = scan(dir.path());
    let mut state = ScanState::default();
    let first = scanner.poll(&Value::Null, &mut state).unwrap();
    let old = read(&first, &ScanParser).remove(0);
    let (_, mut cursor) = stored(&first);
    // Remove identity evidence, as when a file is moved between file systems.
    state.files.get_mut("old.png").unwrap().inode = None;
    std::fs::remove_file(dir.path().join("old.png")).unwrap();
    for name in ["a.png", "b.png"] {
        std::fs::write(dir.path().join(name), &bytes).unwrap();
    }
    scanner.bytes_per_poll = bytes.len() as u64;
    let mut copies = Vec::new();
    for _ in 0..2 {
        let part = scanner.poll(&cursor, &mut state).unwrap();
        copies.extend(read(&part, &ScanParser));
        cursor = stored(&part).1;
    }
    let present: Vec<_> = copies
        .iter()
        .filter(|copy| copy.vanished_at.is_none())
        .collect();
    assert_eq!(present.len(), 2);
    assert_ne!(present[0].item, present[1].item);
    for copy in present {
        assert_ne!(copy.item, old.item);
        assert!(copy.moved_from.is_none());
        assert_eq!(copy.move_candidates.len(), 3);
        assert!(copy.move_candidates.contains(&old.path));
    }
    assert_eq!(state.files_hashed(), 3, "each new file was hashed once");
}
