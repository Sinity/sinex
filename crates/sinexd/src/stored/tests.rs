use super::*;
use std::io::Cursor;

#[devkit::test]
fn streamed_blanking_matches_original_bytes_across_small_buffers_and_overlaps() {
    let original = b"a\nb\nc\nlast\nend";
    let spans = vec![(0, 6), (4, 10), (10, 14)];
    let mut expected = original.to_vec();
    sinex_store::excision::blank(&mut expected, &spans);
    for size in [1, 2, 3, 7, 64] {
        let mut reader = Blanked::new(Cursor::new(original), spans.clone());
        let mut actual = Vec::new();
        let mut block = vec![0; size];
        loop {
            let n = reader.read(&mut block).unwrap();
            if n == 0 {
                break;
            }
            actual.extend_from_slice(&block[..n]);
        }
        assert_eq!(actual, expected, "buffer {size}");
    }
}

#[devkit::test]
fn stored_record_reads_are_exact_and_propagate_read_faults() {
    use sinex_sources::drain::RecordRead;
    let stored = StoredReader::new(Cursor::new(b"alpha\nbeta\n"), 11);
    let mut row = [0; 5];
    stored.read_exact_at(6, &mut row).unwrap();
    assert_eq!(&row, b"beta\n");
    assert!(stored.read_exact_at(7, &mut row).is_err());
}

#[devkit::test]
fn sealed_stored_seek_rechecks_the_unlock_even_for_a_cached_record() {
    use sinex_sources::drain::RecordRead;
    let dir = tempfile::tempdir().unwrap();
    let blobs = sinex_store::Blobs::open(dir.path()).unwrap();
    let key = sinex_store::sealed::new_key().unwrap();
    let bytes = b"first\nother\n";
    let name = blobs.put_sealed(bytes, &key).unwrap();
    let unlock = crate::sealed_access::Unlock::new(
        sinex_store::sealed::Identity::generate(),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let check = unlock.clone();
    let mut reader = blobs
        .read_sealed(
            &name,
            Some(bytes.len() as u64),
            &key,
            crate::sealed_access::read_limits(bytes.len() as u64).unwrap(),
            move || check.check().map_err(|e| std::io::Error::other(e).into()),
        )
        .unwrap();
    std::io::copy(&mut reader, &mut std::io::sink()).unwrap();
    let stored = StoredReader::new(reader, bytes.len() as u64);
    let mut row = [0; 6];
    stored.read_exact_at(0, &mut row).unwrap();
    assert_eq!(&row, b"first\n");
    unlock.revoke();
    assert!(
        stored
            .read_exact_at(0, &mut row)
            .unwrap_err()
            .to_string()
            .contains("revoked")
    );
}

#[devkit::test]
fn blanked_seek_repeats_the_same_exact_transform_without_staging() {
    use std::io::{Cursor, Read, Seek, SeekFrom};
    let bytes = b"kept\nremove\nlast\n";
    let mut reader = Blanked::new(Cursor::new(bytes), vec![(5, 12)]);
    let mut first = Vec::new();
    reader.read_to_end(&mut first).unwrap();
    assert_eq!(first, b"kept\n      \nlast\n");
    reader.rewind().unwrap();
    let mut second = Vec::new();
    reader.read_to_end(&mut second).unwrap();
    assert_eq!(second, first);
    reader.seek(SeekFrom::Start(8)).unwrap();
    let mut suffix = Vec::new();
    reader.read_to_end(&mut suffix).unwrap();
    assert_eq!(suffix, first[8..]);
}
