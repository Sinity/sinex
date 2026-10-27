use super::*;
#[devkit::test]
fn selected_file_keeps_held_bytes_and_cursor_hash_across_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("selected.bin");
    std::fs::write(&path, b"example").unwrap();
    let mut reader = FileReader::new(path.clone(), "example-host".into());
    let a = reader.poll(&Value::Null).unwrap();
    let m = a.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records[0].1).unwrap();
    std::fs::write(&path, b"changed").unwrap();
    let retry = reader.poll(&Value::Null).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, m.id);
    let mut restarted = FileReader::new(path.clone(), "example-host".into());
    let changed = restarted.poll(&cursor).unwrap();
    let m = changed.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records[0].1).unwrap();
    assert!(restarted.poll(&cursor).unwrap().material.is_none());
    std::fs::remove_file(&path).unwrap();
    let removed = restarted.poll(&cursor).unwrap();
    let m = removed.material.as_ref().unwrap();
    assert!(m.manifest.members[0].details.get("removed_at_ns").is_some());
    let cursor = (m.cursor_at)(m.records[0].1).unwrap();
    assert!(restarted.poll(&cursor).unwrap().material.is_none());
}
#[devkit::test]
fn empty_selected_file_is_a_record_and_initial_absence_is_unavailable() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("empty.bin");
    let mut reader = FileReader::new(path.clone(), "example-host".into());
    assert!(
        reader
            .poll(&Value::Null)
            .unwrap()
            .observation
            .unavailable
            .is_some()
    );
    std::fs::write(&path, []).unwrap();
    let acquired = reader.poll(&Value::Null).unwrap();
    let events = super::super::tests::parse(&acquired);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["size"], 0);
}
