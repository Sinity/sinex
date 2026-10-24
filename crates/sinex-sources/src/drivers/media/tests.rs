use super::*;

#[test]
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
    let media = Media {
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
    assert_eq!(media.remove_passed(&cursor).unwrap(), 1);
    assert_eq!(complete(&dir).unwrap().len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}
