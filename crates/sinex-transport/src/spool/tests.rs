use super::*;

fn name(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

#[devkit::test]
fn chunks_survive_reopen_and_conflicting_or_overflowing_resends_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"exact bytes across a restart";
    let (blob, total) = (name(bytes), bytes.len() as u64);
    let receive =
        |offset, data: &[u8]| receive(root.path(), "node", "frame", &blob, total, offset, data);
    assert!(!receive(0, &bytes[..7]).unwrap());
    let partial = directory(root.path(), "node", "frame").join(format!("{}.partial", &blob[7..]));
    assert_eq!(std::fs::read(&partial).unwrap(), &bytes[..7]);
    assert!(receive(0, b"changed").is_err());
    assert!(receive(8, b"x").is_err());
    assert!(receive(u64::MAX, b"xx").is_err());
    assert!(snapshot(root.path(), "node", "frame", &blob, total).is_err());
    assert!(receive(4, &bytes[4..]).unwrap());
    assert!(receive(0, &bytes[..7]).unwrap());
    assert!(receive(0, b"changed").is_err());
    let mut copy = snapshot(root.path(), "node", "frame", &blob, total).unwrap();
    std::fs::write(
        directory(root.path(), "node", "frame").join(&blob[7..]),
        b"producer changed it",
    )
    .unwrap();
    let mut read = Vec::new();
    copy.read_to_end(&mut read).unwrap();
    assert_eq!(read, bytes, "the receiver's copy is its own");
    assert!(snapshot(root.path(), "node", "frame", &blob, total).is_err());
    discard(root.path(), "node", "frame").unwrap();
    discard(root.path(), "node", "frame").unwrap();
    assert!(!directory(root.path(), "node", "frame").exists());
}

#[devkit::test]
fn frames_and_producers_have_separate_directories() {
    let root = tempfile::tempdir().unwrap();
    let body = b"material bytes";
    let blob = name(body);
    let length = body.len() as u64;
    stage(root.path(), "node", "one", &blob, length, body.as_slice()).unwrap();
    stage(root.path(), "node", "two", &blob, length, body.as_slice()).unwrap();
    stage(root.path(), "other", "one", &blob, length, body.as_slice()).unwrap();
    discard(root.path(), "node", "one").unwrap();
    assert!(snapshot(root.path(), "node", "one", &blob, length).is_err());
    assert!(snapshot(root.path(), "node", "two", &blob, length).is_ok());
    assert!(snapshot(root.path(), "other", "one", &blob, length).is_ok());
}

#[devkit::test]
fn staged_bytes_must_match_their_name_and_length() {
    let root = tempfile::tempdir().unwrap();
    let body = b"right";
    assert!(
        stage(
            root.path(),
            "node",
            "frame",
            &name(body),
            5,
            b"wrong".as_slice()
        )
        .is_err()
    );
    assert!(
        stage(
            root.path(),
            "node",
            "frame",
            &name(body),
            4,
            body.as_slice()
        )
        .is_err()
    );
    assert!(
        stage(
            root.path(),
            "node",
            "frame",
            "blake3:../../x",
            5,
            body.as_slice()
        )
        .is_err()
    );
    assert!(
        stage(
            root.path(),
            "node",
            "frame",
            &name(body),
            5,
            body.as_slice()
        )
        .is_ok()
    );
}

#[devkit::test]
fn empty_and_wrong_uploads_leave_no_complete_file() {
    let root = tempfile::tempdir().unwrap();
    assert!(receive(root.path(), "node", "empty", &name(b""), 0, 0, b"").unwrap());
    let right = name(b"right");
    assert!(receive(root.path(), "node", "wrong", &right, 5, 0, b"wrong").is_err());
    assert!(
        !directory(root.path(), "node", "wrong")
            .join(&right[7..])
            .exists()
    );
    assert!(receive(root.path(), "node", "wrong", &right, 5, 0, b"right").unwrap());
}

#[devkit::test]
fn discard_leaves_files_it_did_not_put_there() {
    let root = tempfile::tempdir().unwrap();
    let body = b"bytes";
    stage(
        root.path(),
        "node",
        "frame",
        &name(body),
        5,
        body.as_slice(),
    )
    .unwrap();
    let dir = directory(root.path(), "node", "frame");
    std::fs::write(dir.join("unrelated"), b"kept").unwrap();
    assert!(discard(root.path(), "node", "frame").is_ok());
    assert_eq!(std::fs::read(dir.join("unrelated")).unwrap(), b"kept");
    assert!(!dir.join(&name(body)[7..]).exists());
}

mod group;
