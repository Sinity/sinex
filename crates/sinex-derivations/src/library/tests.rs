use super::*;
use sinex_model::event_types::library::{item_id, item_key};
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

fn copy(path: &str, content: &str, seconds: i64, original: &str) -> Event {
    let key = item_key("h", "/r", original);
    let payload = FileCopy {
        item: item_id(&key),
        item_key: key,
        revision: if content == "blake3:b" { 2 } else { 1 },
        scanned_at: Timestamp::from_unix_secs(seconds).unwrap(),
        host: "h".into(),
        root: "/r".into(),
        path: path.into(),
        content: content.into(),
        size: 3,
        sha256: String::new(),
        md5: String::new(),
        media_type: "image/png".into(),
        width: Some(2),
        height: Some(2),
        duration_ms: None,
        stored: true,
        device: None,
        inode: None,
        modified_at: None,
        moved_from: None,
        previous_content: None,
        vanished_at: None,
    };
    Event::build(
        &payload,
        "library.scan",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(seconds),
        *blake3::hash(serde_json::to_string(&payload).unwrap().as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn compute(inputs: &[Event]) -> Vec<Event> {
    Keyed::compute(
        &LibraryItems,
        &inputs
            .first()
            .map(|event| event.payload["item"].clone())
            .unwrap_or(Value::Null),
        inputs,
        &Env::at(Timestamp::from_unix_secs(100).unwrap()),
    )
    .outputs
}

fn item(outputs: &[Event]) -> Item {
    outputs
        .iter()
        .find(|event| event.event_type == "library.item")
        .unwrap()
        .payload_as()
        .unwrap()
}

#[devkit::test]
fn rescans_and_rebuilds_keep_ids_and_equal_bytes_at_other_paths_stay_separate() {
    let first = copy("/r/a.png", "blake3:a", 1, "a.png");
    let again = copy("/r/a.png", "blake3:a", 2, "a.png");
    let one = compute(std::slice::from_ref(&first));
    let two = compute(&[first.clone(), again]);
    assert_eq!(item(&one).item, item(&two).item);
    assert_eq!(one[0].payload["rendition"], two[0].payload["rendition"]);
    assert_eq!(item(&two).revision, 1);
    let other = compute(&[copy("/r/b.png", "blake3:a", 1, "b.png")]);
    assert_ne!(item(&one).item, item(&other).item);
    let rebuilt = compute(&[first]);
    assert_eq!(one[0].payload, rebuilt[0].payload);
}

#[devkit::test]
fn edits_are_revisions_even_when_the_file_was_replaced() {
    let inputs = [
        copy("/r/a.png", "blake3:a", 1, "a.png"),
        copy("/r/a.png", "blake3:b", 2, "a.png"),
    ];
    let outputs = compute(&inputs);
    let latest = item(&outputs);
    assert_eq!(latest.item, inputs[0].payload["item"].as_str().unwrap());
    assert_eq!(latest.revision, 2);
    assert_eq!(latest.replaces, Some(1));
    assert_eq!(item(&compute(&inputs[1..])).revision, 2);
    assert_eq!(
        outputs
            .iter()
            .filter(|event| event.event_type == "library.rendition")
            .count(),
        2
    );
}

#[devkit::test]
fn a_move_keeps_the_id_even_when_its_first_scan_is_hidden() {
    let first = copy("/r/a.png", "blake3:a", 1, "a.png");
    let moved = copy("/r/b.png", "blake3:a", 2, "a.png");
    let output = compute(&[first.clone(), moved.clone()]);
    let visible = compute(&[moved]);
    assert_eq!(item(&output).item, item(&visible).item);
    assert_eq!(item(&visible).copies, ["h:/r/b.png"]);
    assert_eq!(item(&visible).item, first.payload["item"].as_str().unwrap());
}

#[devkit::test]
fn hiding_the_only_scan_removes_every_output_and_another_scan_keeps_them() {
    let first = copy("/r/a.png", "blake3:a", 1, "a.png");
    let second = copy("/r/a.png", "blake3:a", 2, "a.png");
    assert_eq!(compute(&[first.clone(), second.clone()]).len(), 2);
    assert_eq!(compute(&[second]).len(), 2);
    assert_eq!(compute(&[first]).len(), 2);
    assert!(compute(&[]).is_empty());
}
