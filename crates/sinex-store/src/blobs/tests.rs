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

#[test]
fn plain_copy_and_retained_reader_keep_exact_bytes() {
    let dir = std::env::temp_dir().join(format!("sinex-plain-copy-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    let bytes: Vec<u8> = (0..(8 << 20)).map(|n| (n % 251) as u8).collect();
    let name = blobs.put(&bytes).unwrap();
    struct Compare<'a> {
        expected: &'a [u8],
        through: usize,
    }
    impl std::io::Write for Compare<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert_eq!(
                bytes,
                &self.expected[self.through..self.through + bytes.len()]
            );
            self.through += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Compare {
        expected: &bytes,
        through: 0,
    };
    assert_eq!(blobs.copy_to(&name, &mut out).unwrap(), bytes.len() as u64);
    assert_eq!(out.through, bytes.len());
    assert!(
        blobs
            .read_plain(&name, Some(bytes.len() as u64 + 1), || Ok(()))
            .is_err()
    );
    let mut reader = blobs
        .read_plain(&name, Some(bytes.len() as u64), || Ok(()))
        .unwrap();
    let mut prefix = [0; 19];
    reader.read_exact(&mut prefix).unwrap();
    assert_eq!(prefix, bytes[..19]);
    drop(reader);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn checked_plain_copy_stops_after_its_consumer_cancels() {
    use std::sync::atomic::{AtomicBool,Ordering};
    struct CancelAfterWrite {cancel:Arc<AtomicBool>,written:usize}
    impl Write for CancelAfterWrite {
        fn write(&mut self,bytes:&[u8])->std::io::Result<usize> {
            self.written+=bytes.len();
            self.cancel.store(true,Ordering::Relaxed);
            Ok(bytes.len())
        }
        fn flush(&mut self)->std::io::Result<()> {Ok(())}
    }
    let dir=std::env::temp_dir().join(format!("sinex-cancel-copy-{}",uuid::Uuid::now_v7()));
    let blobs=Blobs::open(&dir).unwrap();
    let length=1<<20;
    let name=blobs.put(&vec![b'x';length]).unwrap();
    let cancel=Arc::new(AtomicBool::new(false));
    let mut output=CancelAfterWrite {cancel:cancel.clone(),written:0};
    let result=blobs.copy_to_checked(&name,&mut output,move || {
        if cancel.load(Ordering::Relaxed) {Err(std::io::Error::other("synthetic cancellation").into())} else {Ok(())}
    });
    assert!(result.unwrap_err().to_string().contains("synthetic cancellation"));
    assert!(output.written>0 && output.written<length);
    std::fs::remove_dir_all(dir).unwrap();
}
