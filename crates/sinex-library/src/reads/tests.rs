use super::*;
use sinex_model::Timestamp;

fn copy(part_index: u32, content: &str) -> serde_json::Value {
    serde_json::to_value(FileCopy {
        scan: format!("scan-{part_index}"),
        part_index,
        item_key: "key".into(),
        item: "item".into(),
        scanned_at: Timestamp::from_unix_secs(1).unwrap(),
        host: "h".into(),
        root: "/r".into(),
        path: "/r/a".into(),
        content: content.into(),
        size: 3,
        sha256: String::new(),
        md5: String::new(),
        media_type: "text/plain".into(),
        width: None,
        height: None,
        duration_ms: None,
        stored: false,
        device: None,
        inode: None,
        modified_at: None,
        moved_from: None,
        move_candidates: Vec::new(),
        previous_content: None,
        vanished_at: None,
    })
    .unwrap()
}

#[devkit::test]
fn a_replayed_library_scan_does_not_replace_newer_copy_facts() {
    let copies = latest_copies(vec![copy(1, "new"), copy(0, "old")]).unwrap();
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0].content, "new");
}

#[devkit::test]
fn revision_history_keeps_all_content_transitions_in_capture_order() {
    let current = Item {
        item: "item".into(),
        revision: "a".into(),
        replaces: Some("b".into()),
        content: "a".into(),
        media_type: "text/plain".into(),
        copies: vec![],
        label: None,
        by: "library.items".into(),
        at: Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: None,
    };
    let copies = [copy(3, "a"), copy(1, "a"), copy(2, "b"), copy(4, "a")]
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect();
    let history = revision_history(current, copies);
    assert_eq!(
        history
            .iter()
            .map(|item| item.revision.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "a"]
    );
    assert_eq!(history[0].replaces, None);
    assert_eq!(history[1].replaces.as_deref(), Some("a"));
    assert_eq!(history[2].replaces.as_deref(), Some("b"));
}
