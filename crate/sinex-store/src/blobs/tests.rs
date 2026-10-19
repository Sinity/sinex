use super::*;

#[test]
fn put_then_read_range() {
    let dir = std::env::temp_dir().join(format!("sinex-blobs-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    let name = blobs.put(b"hello world").unwrap();
    assert_eq!(
        name,
        format!("blake3:{}", blake3::hash(b"hello world").to_hex())
    );
    assert_eq!(blobs.read_range(&name, 6, 5).unwrap(), b"world");
    assert!(blobs.contains(&name).unwrap());
    assert!(
        !blobs
            .contains(&format!("blake3:{}", blake3::hash(b"other").to_hex()))
            .unwrap()
    );
    assert!(blobs.contains("sha256:00").is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn chunks_in_order_are_checked_then_stored() {
    let dir = std::env::temp_dir().join(format!("sinex-chunks-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    let bytes: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
    let name = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let total = bytes.len() as u64;
    assert_eq!(
        blobs
            .receive_chunk(&name, 0, total, &bytes[..4_000])
            .unwrap(),
        Received::Partial(4_000)
    );
    // A resent chunk changes nothing; a chunk out of order is refused.
    assert_eq!(
        blobs
            .receive_chunk(&name, 0, total, &bytes[..4_000])
            .unwrap(),
        Received::Partial(4_000)
    );
    assert!(
        blobs
            .receive_chunk(&name, 8_000, total, &bytes[8_000..])
            .is_err()
    );
    assert!(!blobs.contains(&name).unwrap());
    assert_eq!(
        blobs
            .receive_chunk(&name, 4_000, total, &bytes[4_000..])
            .unwrap(),
        Received::Stored
    );
    assert_eq!(blobs.read(&name).unwrap(), bytes);
    assert_eq!(
        blobs.receive_chunk(&name, 0, total, &bytes[..10]).unwrap(),
        Received::Stored,
        "a stored blob takes any chunk as done"
    );

    // Bytes that do not match their name are dropped, not stored.
    let wrong = format!("blake3:{}", blake3::hash(b"something else").to_hex());
    assert!(blobs.receive_chunk(&wrong, 0, 5, b"hello").is_err());
    assert!(!blobs.contains(&wrong).unwrap());
    assert_eq!(
        blobs
            .receive_chunk(&wrong, 0, 14, b"something else")
            .unwrap(),
        Received::Stored,
        "a failed attempt leaves nothing staged"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
