use super::*;
fn name(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}
fn encoding(root: &Path, bytes: &[u8]) -> (EncodedBlob, Vec<u8>) {
    let prepared = Preparation::new(root).unwrap().plain(bytes, 3).unwrap();
    let descriptor = prepared.descriptor();
    let mut data = Vec::new();
    prepared.into_file().read_to_end(&mut data).unwrap();
    (descriptor, data)
}
#[devkit::test]
fn chunks_survive_reopen_and_conflicting_or_overflowing_resends_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"exact bytes across a restart";
    let blob = name(bytes);
    let (descriptor, encoded) = encoding(root.path(), bytes);
    let receive =
        |offset, data: &[u8]| receive(root.path(), "node", "frame", &descriptor, offset, data);
    assert!(!receive(0, &encoded[..7]).unwrap());
    let partial = directory(root.path(), "node", "frame").join(format!("{}.partial", &blob[7..]));
    assert_eq!(std::fs::read(&partial).unwrap(), &encoded[..7]);
    assert!(receive(0, b"changed").is_err());
    assert!(receive(8, b"x").is_err());
    assert!(receive(u64::MAX, b"xx").is_err());
    assert!(reader(root.path(), "node", "frame", &blob, bytes.len() as u64).is_err());
    assert!(receive(4, &encoded[4..]).unwrap());
    assert!(receive(0, &encoded[..7]).unwrap());
    assert!(receive(0, b"changed").is_err());
    let mut copy = reader(root.path(), "node", "frame", &blob, bytes.len() as u64).unwrap();
    std::fs::write(
        directory(root.path(), "node", "frame").join(&blob[7..]),
        b"producer changed it",
    )
    .unwrap();
    let mut actual = Vec::new();
    copy.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, bytes, "the receiver owns an encoded snapshot");
    assert!(reader(root.path(), "node", "frame", &blob, bytes.len() as u64).is_err());
    discard(root.path(), "node", "frame").unwrap();
    discard(root.path(), "node", "frame").unwrap();
    assert!(!directory(root.path(), "node", "frame").exists());
}
#[devkit::test]
fn frames_and_producers_have_separate_encoded_directories() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"material bytes";
    let blob = name(bytes);
    let length = bytes.len() as u64;
    for (producer, frame) in [("node", "one"), ("node", "two"), ("other", "one")] {
        write(
            root.path(),
            producer,
            frame,
            &blob,
            length,
            bytes.as_slice(),
        )
        .unwrap();
    }
    discard(root.path(), "node", "one").unwrap();
    assert!(reader(root.path(), "node", "one", &blob, length).is_err());
    assert!(reader(root.path(), "node", "two", &blob, length).is_ok());
    assert!(reader(root.path(), "other", "one", &blob, length).is_ok());
}
#[devkit::test]
fn written_bytes_must_match_their_plain_name_and_length() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"right";
    assert!(
        write(
            root.path(),
            "node",
            "frame",
            &name(bytes),
            5,
            b"wrong".as_slice()
        )
        .is_err()
    );
    assert!(
        write(
            root.path(),
            "node",
            "frame",
            &name(bytes),
            4,
            bytes.as_slice()
        )
        .is_err()
    );
    assert!(
        write(
            root.path(),
            "node",
            "frame",
            "blake3:../../x",
            5,
            bytes.as_slice()
        )
        .is_err()
    );
    assert!(
        write(
            root.path(),
            "node",
            "frame",
            &name(bytes),
            5,
            bytes.as_slice()
        )
        .is_ok()
    );
}
#[devkit::test]
fn empty_and_wrong_encoded_uploads_leave_no_false_complete_file() {
    let root = tempfile::tempdir().unwrap();
    let (descriptor, encoded) = encoding(root.path(), b"");
    assert!(!encoded.is_empty());
    assert!(receive(root.path(), "node", "empty", &descriptor, 0, &encoded).unwrap());
    assert_eq!(
        reader(root.path(), "node", "empty", &name(b""), 0)
            .unwrap()
            .read(&mut [0])
            .unwrap(),
        0
    );
    let (descriptor, encoded) = encoding(root.path(), b"right");
    let mut wrong = encoded.clone();
    wrong[0] ^= 1;
    assert!(receive(root.path(), "node", "wrong", &descriptor, 0, &wrong).is_err());
    assert!(
        !directory(root.path(), "node", "wrong")
            .join(&name(b"right")[7..])
            .exists()
    );
    assert!(receive(root.path(), "node", "wrong", &descriptor, 0, &encoded).unwrap());
}
#[devkit::test]
fn discard_leaves_files_it_did_not_put_there() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"bytes";
    write(
        root.path(),
        "node",
        "frame",
        &name(bytes),
        5,
        bytes.as_slice(),
    )
    .unwrap();
    let dir = directory(root.path(), "node", "frame");
    std::fs::write(dir.join("unrelated"), b"kept").unwrap();
    discard(root.path(), "node", "frame").unwrap();
    assert_eq!(std::fs::read(dir.join("unrelated")).unwrap(), b"kept");
    assert!(!dir.join(&name(bytes)[7..]).exists());
    assert!(!descriptor_path(&dir, &name(bytes)[7..]).exists());
}
#[devkit::test]
fn storage_faults_and_invalid_encodings_have_different_errors() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"synthetic";
    let blob = name(bytes);
    let (descriptor, encoded) = encoding(root.path(), bytes);
    assert!(matches!(
        reader(root.path(), "node", "missing", &blob, bytes.len() as u64),
        Err(Error::Storage(_))
    ));
    let blocked = root.path().join("not-a-directory");
    std::fs::write(&blocked, b"file").unwrap();
    assert!(matches!(
        receive(&blocked, "node", "frame", &descriptor, 0, &encoded),
        Err(Error::Storage(_))
    ));
    assert!(matches!(
        write(
            root.path(),
            "node",
            "frame",
            &blob,
            bytes.len() as u64,
            b"different".as_slice()
        ),
        Err(Error::Mismatch(_))
    ));
}
#[devkit::test]
fn locally_linked_and_remotely_transferred_custody_write_only_shared_encoding() {
    let root = tempfile::tempdir().unwrap();
    let sender = tempfile::tempdir().unwrap();
    let bytes = b"{\"text\":\"synthetic repeated payload\"}\n".repeat(40_000);
    let temporary = tempfile::NamedTempFile::new_in(sender.path()).unwrap();
    let prepared = Preparation::new(sender.path())
        .unwrap()
        .plain_into(temporary.reopen().unwrap(), bytes.as_slice(), 3)
        .unwrap();
    let descriptor = prepared.descriptor();
    let source = sender.path().join("owned");
    drop(prepared);
    temporary.as_file().sync_all().unwrap();
    temporary.persist(&source).unwrap();
    link_encoded(root.path(), "node", "local", &descriptor, &source).unwrap();
    let (local, file) =
        encoded(root.path(), "node", "local", &descriptor.hash.to_string()).unwrap();
    assert_eq!(local, descriptor);
    assert_eq!(
        file.metadata().unwrap().ino(),
        std::fs::metadata(&source).unwrap().ino()
    );
    assert!(descriptor.stored_len + (descriptor.encode().len() as u64) < bytes.len() as u64 / 20);
    let mut raw = std::fs::File::open(&source).unwrap();
    let mut buffer = [0; 31];
    let mut offset = 0;
    loop {
        let n = raw.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        receive(
            root.path(),
            "remote",
            "network",
            &descriptor,
            offset,
            &buffer[..n],
        )
        .unwrap();
        offset += n as u64;
    }
    let imported = super::prepared(
        root.path(),
        "remote",
        "network",
        &descriptor.hash.to_string(),
        bytes.len() as u64,
    )
    .unwrap();
    assert_eq!(imported.descriptor(), descriptor);
    let mut actual = Vec::new();
    imported
        .reader(None, ReadLimits::default())
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, bytes);
}
mod group;
