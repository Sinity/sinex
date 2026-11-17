use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
const BODY: &[u8] = b"a tiny receiver-installed plain blob\n";
#[devkit::test]
async fn protected_group_mode_keeps_plain_encoding_and_sealed_key_checks(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let shared = tempfile::tempdir().unwrap();
    std::fs::set_permissions(shared.path(), std::fs::Permissions::from_mode(0o2770)).unwrap();
    let blobs = Blobs::open(shared.path()).unwrap();
    let name = blobs.put(BODY).unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&name))
        .await
        .unwrap();
    let descriptors = blobs
        .descriptors(&blobs.locate(&mut conn, &name).await.unwrap())
        .unwrap();
    assert_eq!(
        std::fs::metadata(shared.path().join(SEGMENT))
            .unwrap()
            .mode()
            & 0o777,
        0o640
    );
    assert_eq!(
        hashed_blobs::Reader::new(shared.path())
            .read(&[Extent::decode(&descriptors[0]).unwrap()], None)
            .unwrap(),
        BODY
    );
    let key = sinex_sealing::DataKey {
        key_ref: "synthetic".into(),
        key: [7; 32],
    };
    let sealed = blobs.put_sealed(BODY, &key).unwrap();
    assert!(
        blobs
            .read(&blobs.locate(&mut conn, &sealed).await.unwrap())
            .is_err()
    );
    let mut reader = blobs
        .read_sealed(
            &blobs.locate(&mut conn, &sealed).await.unwrap(),
            Some(BODY.len() as u64),
            &key,
            SealedReadLimits {
                frame_bytes: BLOB_FRAME_BYTES,
                index_bytes: 65536,
            },
            || Ok(()),
        )
        .unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, BODY);
}
#[devkit::test]
async fn deduplicating_an_existing_extent_does_not_rewrite_its_segment(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let name = blobs.put(BODY).unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&name))
        .await
        .unwrap();
    drop(blobs);
    let blobs = Blobs::open(dir.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let before = std::fs::metadata(dir.path().join(SEGMENT)).unwrap();
    let descriptor = blobs
        .descriptors(&blobs.locate(&mut conn, &name).await.unwrap())
        .unwrap();
    assert_eq!(blobs.put(BODY).unwrap(), name);
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&name))
        .await
        .unwrap();
    let after = std::fs::metadata(dir.path().join(SEGMENT)).unwrap();
    assert_eq!(
        (before.ino(), before.len(), before.mode()),
        (after.ino(), after.len(), after.mode())
    );
    assert_eq!(
        blobs
            .descriptors(&blobs.locate(&mut conn, &name).await.unwrap())
            .unwrap(),
        descriptor
    );
}
