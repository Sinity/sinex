use super::*;

fn acq(path: &str, content: &str) -> Acquisition {
    Acquisition {
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
        device: Some(1),
        inode: Some(9),
        modified_at: None,
        moved_from: None,
        previous_content: None,
        vanished_at: None,
    }
}

fn at() -> Timestamp {
    Timestamp::from_unix_secs(100).unwrap()
}

fn items(r: &[LibraryRecord]) -> Vec<&Item> {
    r.iter()
        .filter_map(|x| match x {
            LibraryRecord::Item(i) => Some(i),
            _ => None,
        })
        .collect()
}

#[test]
fn a_new_copy_is_a_new_item_with_its_original() {
    let r = records_for(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
    assert_eq!(r.len(), 2);
    let i = items(&r)[0];
    assert_eq!(i.revision, 1);
    assert_eq!(i.acquisitions, ["h:/r/a.png"]);
    assert_eq!(i.label.as_deref(), Some("a.png"));
    assert!(
        matches!(&r[1], LibraryRecord::Rendition(x) if x.role == RenditionRole::Original && x.content == "blake3:a")
    );
}

#[test]
fn equal_bytes_at_an_unrelated_path_are_another_item() {
    let first = records_for(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
    let existing = items(&first)[0].clone();
    let r = records_for(
        &acq("/r/copy.png", "blake3:a"),
        |k| (k == "h:/r/a.png").then(|| existing.clone()),
        "intake",
        at(),
    );
    assert_ne!(items(&r)[0].item, existing.item);
}

#[test]
fn a_rename_moves_the_acquisition_and_an_edit_makes_a_revision() {
    let first = records_for(&acq("/r/a.png", "blake3:a"), |_| None, "intake", at());
    let existing = items(&first)[0].clone();
    let mut moved = acq("/r/b.png", "blake3:a");
    moved.moved_from = Some("/r/a.png".into());
    let r = records_for(
        &moved,
        |k| (k == "h:/r/a.png").then(|| existing.clone()),
        "intake",
        at(),
    );
    let i = items(&r)[0];
    assert_eq!(i.item, existing.item);
    assert_eq!(i.acquisitions, ["h:/r/b.png"]);
    assert_eq!(i.revision, 1);

    let mut edited = acq("/r/a.png", "blake3:a2");
    edited.previous_content = Some("blake3:a".into());
    let r = records_for(
        &edited,
        |k| (k == "h:/r/a.png").then(|| existing.clone()),
        "intake",
        at(),
    );
    let i = items(&r)[0];
    assert_eq!(
        (i.item.as_str(), i.revision, i.replaces),
        (existing.item.as_str(), 2, Some(1))
    );
    assert_eq!(i.content, "blake3:a2");

    // Replaced by an unrelated file at the same path: no continuity.
    let r = records_for(
        &acq("/r/a.png", "blake3:z"),
        |k| (k == "h:/r/a.png").then(|| existing.clone()),
        "intake",
        at(),
    );
    let is = items(&r);
    assert_eq!(is[0].item, existing.item);
    assert!(is[0].acquisitions.is_empty());
    assert_ne!(is[1].item, existing.item);

    // Unchanged and vanished copies change nothing.
    assert!(
        records_for(
            &acq("/r/a.png", "blake3:a"),
            |_| Some(existing.clone()),
            "intake",
            at()
        )
        .is_empty()
    );
    let mut gone = acq("/r/a.png", "blake3:a");
    gone.vanished_at = Some(at());
    assert!(records_for(&gone, |_| None, "intake", at()).is_empty());
}
