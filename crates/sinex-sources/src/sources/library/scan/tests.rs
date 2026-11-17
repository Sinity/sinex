use super::*;
use crate::sources::library::tests::{png, read, scan, stored, tmp};
use sinex_model::{Class, RecordOutcome};
use sinex_source_core::{DrainInput, Limits, drain};

#[devkit::test]
fn an_unreadable_directory_reports_partial_coverage_and_other_files_continue() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(nested.join("image.png"), png(10, 10)).unwrap();
    let scanner = scan(dir.path());
    let mut state = ScanState::default();
    let acquired = scanner.poll(&Value::Null, &mut state).unwrap();
    let (_, cursor) = stored(&acquired);
    let known = state.files["nested/image.png"].clone();
    std::fs::write(dir.path().join("new.png"), png(20, 20)).unwrap();
    std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o000)).unwrap();
    let partial = scanner.poll(&cursor, &mut state);
    std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o700)).unwrap();
    let partial = partial.unwrap();
    let material = partial.material.as_ref().unwrap();
    let result = drain(
        &sinex_source_core::Stateless(ScanParser),
        DrainInput {
            source: SCAN.name,
            class: Class::Open,
            material: material.id,
            bytes: material.record_bytes(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &*material.reading_order,
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    let outcomes: Vec<_> = result
        .into_iter()
        .flat_map(|batch| batch.outcomes)
        .collect();
    let failures: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            RecordOutcome::Failure { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("partial library scan"));
    assert!(failures[0].contains("nested"));
    let copies: Vec<FileCopy> = outcomes
        .into_iter()
        .flat_map(|outcome| match outcome {
            RecordOutcome::Events { events, .. } => events,
            _ => vec![],
        })
        .map(|event| event.payload_as().unwrap())
        .collect();
    assert_eq!(copies.len(), 1);
    assert!(copies[0].path.ends_with("new.png"));
    assert_eq!(state.files["nested/image.png"], known);
    let (_, next) = stored(&partial);
    assert_eq!(
        next["last_complete_walk_ns"],
        cursor["last_complete_walk_ns"]
    );
    // The successful retry discovers changes under the unreadable directory.
    std::fs::write(nested.join("second.png"), png(30, 30)).unwrap();
    let retry = read(
        &scanner.poll(&next, &mut state).unwrap(),
        &sinex_source_core::Stateless(ScanParser),
    );
    assert_eq!(retry.len(), 1);
    assert!(retry[0].path.ends_with("nested/second.png"));
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
        copies += read(&acquired, &sinex_source_core::Stateless(ScanParser)).len();
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
fn a_reused_path_gets_a_separate_key_while_the_moved_item_is_present() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.png"), png(10, 10)).unwrap();
    let scanner = scan(dir.path());
    let mut state = ScanState::default();
    let first = scanner.poll(&Value::Null, &mut state).unwrap();
    let old = read(&first, &sinex_source_core::Stateless(ScanParser)).remove(0);
    let (_, cursor) = stored(&first);
    std::fs::rename(dir.path().join("a.png"), dir.path().join("b.png")).unwrap();
    let moved = scanner.poll(&cursor, &mut state).unwrap();
    let (_, cursor) = stored(&moved);
    std::fs::write(dir.path().join("a.png"), png(20, 20)).unwrap();
    let acquired = scanner.poll(&cursor, &mut state).unwrap();
    let new = read(&acquired, &sinex_source_core::Stateless(ScanParser)).remove(0);
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
fn ollama_metadata_does_not_read_weights_and_other_model_files_stay_full() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let blob = format!("model/ollama/blobs/sha256-{}", "a".repeat(64));
    let weight = root.path().join(&blob);
    std::fs::create_dir_all(weight.parent().unwrap()).unwrap();
    let file = std::fs::File::create(&weight).unwrap();
    file.set_len(16_796_011_072).unwrap();
    drop(file);
    // Stat is permitted; reading contents is not. This fails if capture hashes
    // the weight, including a move preflight or a fallback content read.
    std::fs::set_permissions(&weight, std::fs::Permissions::from_mode(0o0)).unwrap();
    let full = [
        "model/control-vector/authored.gguf".to_owned(),
        "model/unclassified/retained.bin".to_owned(),
        "model/ollama/blobs/unclassified.bin".to_owned(),
        format!("model/ollama/blobs/nested/sha256-{}", "b".repeat(64)),
        format!("model/ollama/blobs/sha256-{}", "A".repeat(64)),
    ];
    for name in &full {
        let path = root.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"authored or unclassified bytes").unwrap();
    }
    let mut scanner = scan(root.path());
    scanner.ollama_blobs_metadata = vec!["model/ollama/blobs".into()];
    let mut state = ScanState::default();
    let acquired = scanner.poll(&Value::Null, &mut state).unwrap();
    let material = acquired.material.as_ref().unwrap();
    let events = crate::sources::library::tests::read_events(
        &acquired,
        &sinex_source_core::Stateless(ScanParser),
    );
    let metadata = events
        .iter()
        .find(|e| e.event_type == "document.captured")
        .unwrap();
    assert_eq!(metadata.payload["root"], root.path().display().to_string());
    assert_eq!(metadata.payload["path"], blob);
    assert_eq!(metadata.payload["size"], 16_796_011_072_u64);
    assert_eq!(metadata.payload["located"], true);
    assert!(metadata.payload["content"].is_null());
    assert!(metadata.payload["media_type"].is_null());
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "library.copy")
            .count(),
        full.len()
    );
    assert_eq!(state.files_hashed(), full.len());
    let owned: std::collections::BTreeSet<_> = material
        .owned_files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(owned, full.iter().map(|p| root.path().join(p)).collect());
    for event in events.iter().filter(|e| e.event_type == "library.copy") {
        let copy: FileCopy = event.payload_as().unwrap();
        assert!(copy.stored);
        assert_eq!(
            copy.content,
            format!("blake3:{}", blake3::hash(b"authored or unclassified bytes"))
        );
    }
    let (retained, cursor) = stored(&acquired);
    let memory = state.memory();
    let mut restarted = ScanState::default();
    restarted.remember_saved(&memory).unwrap();
    assert!(
        scanner
            .poll(&cursor, &mut restarted)
            .unwrap()
            .material
            .is_none()
    );
    assert_eq!(restarted.files_hashed(), 0);
    assert!(restarted.files[&blob].content.is_none());
    assert!(restarted.files[&blob].sha256.is_none());
    assert!(restarted.files[&blob].md5.is_none());
    let mut replayed = ScanState::default();
    replayed
        .remember_part(
            root.path(),
            retained.bytes.as_ref(),
            retained.admitted.unwrap(),
        )
        .unwrap();
    assert_eq!(replayed.files[&blob], state.files[&blob]);
    std::fs::remove_file(&weight).unwrap();
    let removed = scanner.poll(&cursor, &mut restarted).unwrap();
    let events = crate::sources::library::tests::read_events(
        &removed,
        &sinex_source_core::Stateless(ScanParser),
    );
    let event = events
        .iter()
        .find(|e| e.event_type == "document.captured")
        .unwrap();
    assert_eq!(event.payload["path"], blob);
    assert!(!event.payload["removed_at"].is_null());
    assert!(event.payload["content"].is_null());
    assert!(removed.material.as_ref().unwrap().owned_files.is_empty());
}

#[devkit::test]
fn selecting_metadata_keeps_existing_full_copy_facts_and_never_prepares_a_move() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let blob = format!("model/ollama/blobs/sha256-{}", "c".repeat(64));
    let weight = root.path().join(&blob);
    std::fs::create_dir_all(weight.parent().unwrap()).unwrap();
    std::fs::write(&weight, b"previously captured weight").unwrap();
    std::fs::write(
        root.path().join("before.bin"),
        b"previously captured weight",
    )
    .unwrap();
    let mut scanner = scan(root.path());
    let mut state = ScanState::default();
    let previous = scanner.poll(&Value::Null, &mut state).unwrap();
    let copies = read(&previous, &sinex_source_core::Stateless(ScanParser));
    assert!(
        copies
            .iter()
            .any(|c| c.path == weight.display().to_string() && c.stored)
    );
    let old = state.files[&blob].content.clone().unwrap();
    let (_, cursor) = stored(&previous);
    std::fs::rename(
        root.path().join("before.bin"),
        root.path().join("moved.bin"),
    )
    .unwrap();
    scanner.ollama_blobs_metadata = vec!["model/ollama/blobs".into()];
    let new_weight = root
        .path()
        .join(format!("model/ollama/blobs/sha256-{}", "d".repeat(64)));
    std::fs::write(&new_weight, b"previously captured weight").unwrap();
    std::fs::set_permissions(&weight, std::fs::Permissions::from_mode(0o0)).unwrap();
    std::fs::set_permissions(&new_weight, std::fs::Permissions::from_mode(0o0)).unwrap();
    let next = scanner.poll(&cursor, &mut state).unwrap();
    assert!(
        !next
            .material
            .as_ref()
            .unwrap()
            .owned_files
            .iter()
            .any(|f| f.path == weight || f.path == new_weight)
    );
    let events = crate::sources::library::tests::read_events(
        &next,
        &sinex_source_core::Stateless(ScanParser),
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "document.captured")
            .count(),
        2
    );
    assert_eq!(
        state.files_hashed(),
        3,
        "only the full moved copy is prepared"
    );
    assert!(copies.iter().any(|c| c.content == old && c.stored));
    // Existing saved full-copy strings deserialize as known digests, without
    // rereading or discarding the admitted bytes during policy adoption.
    let mut saved = serde_json::to_value(&state.files["moved.bin"]).unwrap();
    saved.as_object_mut().unwrap().remove("metadata");
    let known: Known = serde_json::from_value(saved).unwrap();
    assert!(known.content.is_some());
}
