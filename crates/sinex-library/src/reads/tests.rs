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
