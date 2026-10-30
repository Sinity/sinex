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
        &ScanParser,
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
    let retry = read(&scanner.poll(&next, &mut state).unwrap(), &ScanParser);
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
