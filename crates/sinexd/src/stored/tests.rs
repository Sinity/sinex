use super::*;
use std::io::Cursor;

#[devkit::test]
fn stored_record_reads_are_exact_and_propagate_read_faults() {
    use sinex_source_core::drain::RecordRead;
    let stored = StoredReader::new(Cursor::new(b"alpha\nbeta\n"), 11);
    let mut row = [0; 5];
    stored.read_exact_at(6, &mut row).unwrap();
    assert_eq!(&row, b"beta\n");
    assert!(stored.read_exact_at(7, &mut row).is_err());
}

#[devkit::test]
fn sealed_stored_seek_rechecks_the_unlock_even_for_a_cached_record() {
    use sinex_source_core::drain::RecordRead;
    let dir = tempfile::tempdir().unwrap();
    let blobs = sinex_store::Blobs::open(dir.path()).unwrap();
    let key = sinex_sealing::new_key().unwrap();
    let bytes = b"first\nother\n";
    let name = blobs.put_sealed(bytes, &key).unwrap();
    let unlock = crate::material_access::sealing::Unlock::new(
        sinex_sealing::Identity::generate(),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let check = unlock.clone();
    let mut reader = blobs
        .read_sealed(
            &name,
            Some(bytes.len() as u64),
            &key,
            crate::material_access::sealing::read_limits(bytes.len() as u64).unwrap(),
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
