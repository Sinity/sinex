use super::*;

#[devkit::test]
fn segments_are_stored_one_per_poll_in_order_and_removed_after() {
    let dir = std::env::temp_dir().join(format!("sinex-media-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    for (stem, body) in [
        ("000002-000003.screen", b"bb".as_slice()),
        ("000001-000002.screen", b"a"),
    ] {
        let partial = dir.join(format!("{stem}.jpg.partial"));
        std::fs::write(&partial, body).unwrap();
        publish_segment(&dir, &partial, stem, "jpg", &json!({"kind": "screen"})).unwrap();
    }
    // A segment still being written is not complete.
    std::fs::write(dir.join("000003-000004.screen.jpg.partial"), b"c").unwrap();
    let mut media = Media {
        spool: dir.clone(),
        input: InputRef {
            kind: "media".into(),
            locator: "screen".into(),
            host: "h".into(),
        },
    };
    let first = media.poll(&Value::Null).unwrap().material.unwrap();
    assert_eq!(first.bytes, b"a");
    assert_eq!(first.records, vec![(0, 1)]);
    let cursor = (first.cursor_at)(1).unwrap();
    let second = media.poll(&cursor).unwrap().material.unwrap();
    assert_eq!(second.bytes, b"bb");
    assert_eq!(second.part_index, 1);
    assert!(
        media
            .poll(&(second.cursor_at)(2).unwrap())
            .unwrap()
            .material
            .is_none()
    );
    assert_eq!(media.committed(&cursor).unwrap(), 1);
    assert_eq!(complete(&dir).unwrap().len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[devkit::test]
fn place_moves_whole_bytes_and_syncs_the_directory_after_renaming() {
    let dir = tempfile::tempdir().unwrap();
    let from = dir.path().join("segment.jpg.partial");
    let to = dir.path().join("segment.jpg");
    std::fs::write(&from, b"whole").unwrap();
    place(dir.path(), &from, &to).unwrap();
    assert_eq!(std::fs::read(&to).unwrap(), b"whole");
    assert!(!from.exists());

    // The directory is synced after the rename: a directory that cannot be
    // opened fails the call even though the rename itself succeeded.
    std::fs::write(&from, b"again").unwrap();
    let missing = dir.path().join("missing");
    assert!(place(&missing, &from, &to).is_err());
    assert_eq!(std::fs::read(&to).unwrap(), b"again");
}

#[devkit::test]
fn publish_segment_syncs_before_rename() {
    let dir = tempfile::tempdir().unwrap();
    let partial = dir.path().join("segment.audio.ogg.partial");
    // This readable file cannot be synced. Its final name must never appear
    // when syncing fails, even though a rename of the link would succeed.
    std::os::unix::fs::symlink("/dev/null", &partial).unwrap();
    let error = publish_segment(
        dir.path(),
        &partial,
        "segment.audio",
        "ogg",
        &json!({"kind": "audio"}),
    )
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(22));
    assert_eq!(
        std::fs::read_link(&partial).unwrap(),
        PathBuf::from("/dev/null")
    );
    assert_eq!(complete(dir.path()).unwrap(), Vec::new());
    assert_eq!(
        serde_json::from_slice::<Value>(
            &std::fs::read(dir.path().join("segment.audio.json")).unwrap()
        )
        .unwrap(),
        json!({"kind": "audio"})
    );
}
