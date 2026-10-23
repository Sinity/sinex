use super::*;
use sinex_sources::drivers::sqlite_image::{Policy, Scan};
use std::os::unix::fs::FileExt;

#[test]
fn sqlite_image_publication_checks_dense_map_bytes_and_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(&dir.path().join("store")).unwrap();
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES ('synthetic'); BEGIN; SELECT count(*) FROM sqlite_master;").unwrap();
    let mut scan = Scan::start(
        connection,
        Policy {
            scope: "whole_database".into(),
            class: Class::Guarded,
            interval_secs: 3600,
            turn_pages: 1,
            rebase_threshold: 1.0,
        },
        None,
        "synthetic".into(),
        "test".into(),
        sinex_model::Timestamp::now(),
    )
    .unwrap();
    while !scan.turn().unwrap() {}
    let image = scan.finish().unwrap();
    image
        .set_check(Arc::new(|| Err("synthetic cancellation".into())))
        .unwrap();
    assert!(
        store(&blobs, &image, None)
            .unwrap_err()
            .contains("cancellation")
    );
    assert!(image.stored().unwrap().is_none());
    image.set_check(Arc::new(|| Ok(()))).unwrap();
    let map = image.map.as_ref().unwrap();
    let mut original = [0; 32];
    map.read_exact_at(&mut original, 0).unwrap();
    map.write_all_at(&[0; 32], 0).unwrap();
    assert!(
        store(&blobs, &image, None)
            .unwrap_err()
            .contains("page map differs")
    );
    map.write_all_at(&original, 0).unwrap();
    let publication = store(&blobs, &image, None).unwrap();
    publication.validate().unwrap();
    assert!(publication.metadata.evidence_bytes > publication.metadata.image_bytes);
}
