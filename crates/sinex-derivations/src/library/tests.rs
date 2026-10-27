use super::*;
use sinex_model::event_types::library::{item_id, item_key};
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

fn copy(path: &str, content: &str, seconds: i64, original: &str) -> Event {
    copy_at(
        path,
        content,
        u32::try_from(seconds).unwrap(),
        seconds,
        original,
    )
}

fn copy_at(path: &str, content: &str, part_index: u32, seconds: i64, original: &str) -> Event {
    let material = MaterialId::new();
    let key = item_key("h", "/r", original);
    let payload = FileCopy {
        scan: material.to_string(),
        part_index,
        item: item_id(&key),
        item_key: key,
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
        move_candidates: Vec::new(),
        previous_content: None,
        vanished_at: None,
    };
    Event::build(
        &payload,
        "library.scan",
        Origin::Record {
            location: RecordLocation {
                material,
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
    assert_eq!(item(&two).revision, "blake3:a");
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
    assert_eq!(latest.revision, "blake3:b");
    assert_eq!(latest.replaces.as_deref(), Some("blake3:a"));
    assert_eq!(item(&compute(&inputs[1..])).revision, "blake3:b");
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

#[devkit::test]
fn returning_to_the_same_bytes_reuses_the_revision_and_original() {
    let inputs = [
        copy("/r/a.png", "blake3:a", 1, "a.png"),
        copy("/r/a.png", "blake3:b", 2, "a.png"),
        copy("/r/a.png", "blake3:a", 3, "a.png"),
    ];
    let output = compute(&inputs);
    assert_eq!(item(&output).revision, "blake3:a");
    assert_eq!(item(&output).replaces.as_deref(), Some("blake3:b"));
    assert_eq!(
        output
            .iter()
            .filter(|event| event.event_type == "library.rendition")
            .count(),
        2
    );
}

#[devkit::test]
fn clock_corrections_do_not_change_capture_order() {
    let first = copy_at("/r/a.png", "blake3:a", 0, 2, "a.png");
    let second = copy_at("/r/a.png", "blake3:b", 1, 1, "a.png");
    let output = compute(&[second, first]);
    assert_eq!(item(&output).revision, "blake3:b");
    assert_eq!(item(&output).replaces.as_deref(), Some("blake3:a"));
}

#[devkit::test]
fn preview_evidence_derives_a_rendition_with_both_parents_without_external_tools() {
    use sinex_model::event_types::library::Preview;
    let original = copy("/r/a.png", "blake3:a", 1, "a.png");
    let payload = Preview {
        rendition: "preview-a".into(),
        item: original.payload["item"].as_str().unwrap().into(),
        revision: "blake3:a".into(),
        role: RenditionRole::Preview,
        content: "blake3:preview".into(),
        media_type: "image/webp".into(),
        width: Some(1),
        height: Some(1),
        duration_ms: None,
        made_from: Some(format!(
            "{}:blake3:a",
            original.payload["item"].as_str().unwrap()
        )),
        made_with: serde_json::Map::from_iter([("tool".into(), Value::from("synthetic"))]),
        by: "operator".into(),
        at: Timestamp::from_unix_secs(2).unwrap(),
        withdrawn_at: None,
    };
    let preview = Event::build(
        &payload,
        "library.request",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(2),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    let output = compute(&[original.clone(), preview.clone()]);
    let derived = output
        .iter()
        .find(|event| event.payload["rendition"] == "preview-a")
        .unwrap();
    let rendition: Rendition = derived.payload_as().unwrap();
    assert_eq!(rendition.part_index, 1);
    assert_eq!(rendition.content, payload.content);
    assert_eq!(rendition.made_from, payload.made_from);
    assert_eq!(rendition.made_with, payload.made_with);
    assert_eq!(rendition.by, "operator");
    let Origin::Derived { parents } = &derived.origin else {
        panic!("derived rendition");
    };
    assert_eq!(parents.len(), 2);
    assert!(parents.contains(&original.id));
    assert!(parents.contains(&preview.id));
    assert!(
        compute(&[preview]).is_empty(),
        "a preview alone cannot supply a revision"
    );
    assert_eq!(
        compute(&[original]).len(),
        2,
        "removing the preview leaves the original and item"
    );
}
