use super::*;
use std::io::Write;

#[devkit::test]
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

#[devkit::test]
fn plain_copy_and_stored_reader_keep_exact_bytes() {
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

#[devkit::test]
fn checked_plain_copy_stops_after_its_consumer_cancels() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct CancelAfterWrite {
        cancel: Arc<AtomicBool>,
        written: usize,
    }
    impl Write for CancelAfterWrite {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.written += bytes.len();
            self.cancel.store(true, Ordering::Relaxed);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let dir = std::env::temp_dir().join(format!("sinex-cancel-copy-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    let length = 1 << 20;
    let name = blobs.put(&vec![b'x'; length]).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut output = CancelAfterWrite {
        cancel: cancel.clone(),
        written: 0,
    };
    let result = blobs.copy_to_checked(&name, &mut output, move || {
        if cancel.load(Ordering::Relaxed) {
            Err(std::io::Error::other("synthetic cancellation").into())
        } else {
            Ok(())
        }
    });
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("synthetic cancellation")
    );
    assert!(output.written > 0 && output.written < length);
    std::fs::remove_dir_all(dir).unwrap();
}

mod group_readback;

#[devkit::test]
fn scoped_plain_seek_owns_verified_bytes_and_rechecks_cancellation_on_cached_reads() {
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = std::env::temp_dir().join(format!("sinex-plain-seek-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    let bytes = b"first record\nsecond record\nthird record\n";
    let name = blobs.put(bytes).unwrap();
    assert!(
        blobs
            .read_plain_seek(&name, bytes.len() as u64 + 1, || Ok(()))
            .is_err()
    );
    let key = sinex_sealing::new_key().unwrap();
    let sealed = blobs.put_sealed(bytes, &key).unwrap();
    assert!(
        blobs
            .read_plain_seek(&sealed, bytes.len() as u64, || Ok(()))
            .is_err()
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let check = cancelled.clone();
    let mut reader = blobs
        .read_plain_seek(&name, bytes.len() as u64, move || {
            if check.load(Ordering::Relaxed) {
                return Err(hashed_blobs::Error::Io(std::io::Error::other("cancelled")));
            }
            Ok(())
        })
        .unwrap();
    drop(blobs);
    reader.seek(SeekFrom::Start(13)).unwrap();
    let mut record = [0; 14];
    reader.read_exact(&mut record).unwrap();
    assert_eq!(&record, b"second record\n");
    reader.seek(SeekFrom::End(-13)).unwrap();
    let mut end = Vec::new();
    reader.read_to_end(&mut end).unwrap();
    assert_eq!(end, b"third record\n");
    assert!(reader.seek(SeekFrom::Start(0)).is_ok());
    cancelled.store(true, Ordering::Relaxed);
    assert!(reader.read(&mut record).is_err());
    assert!(reader.seek(SeekFrom::Start(0)).is_err());
    drop(reader);
    std::fs::remove_dir_all(dir).unwrap();
}

#[devkit::test]
async fn grouped_extents_commit_with_rows_and_recovery_truncates_only_the_tail(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let first = blobs.put(b"first retained bytes").unwrap();
    let second = blobs.put(b"second retained bytes").unwrap();
    assert!(
        !dir.path().join(SEGMENT).exists(),
        "preparation has no installed segment"
    );
    let mut tx = conn.begin().await.unwrap();
    let receipt = blobs.commit_extents(&mut tx).await.unwrap();
    sqlx::query(
        "INSERT INTO core.blobs(hash,size,status) VALUES($1,20,'present'),($2,21,'present')",
    )
    .bind(&first)
    .bind(&second)
    .execute(&mut *tx)
    .await
    .unwrap();
    {
        let state = blobs.state().unwrap();
        assert_eq!(state.groups.len(), 1);
        assert_eq!(state.groups[0].extents.len(), 2);
        assert_eq!(state.groups[0].metrics.syncs, 1);
    }
    tx.commit().await.unwrap();
    blobs.acknowledge(receipt).unwrap();
    let committed = std::fs::metadata(dir.path().join(SEGMENT)).unwrap().len();
    let orphan = blobs.put(b"synced but no database commit").unwrap();
    blobs.sync_pending().unwrap();
    assert!(std::fs::metadata(dir.path().join(SEGMENT)).unwrap().len() > committed);
    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(
        std::fs::metadata(dir.path().join(SEGMENT)).unwrap().len(),
        committed
    );
    assert_eq!(reopened.read(&first).unwrap(), b"first retained bytes");
    assert_eq!(reopened.read(&second).unwrap(), b"second retained bytes");
    assert!(!reopened.contains(&orphan).unwrap());
}

#[devkit::test]
async fn orphan_extents_are_marked_before_punch_and_committed_siblings_survive(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let keep = blobs.put(b"keep sibling").unwrap();
    let orphan = blobs.put(b"orphan").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,12,'present')")
        .bind(&keep)
        .execute(&mut conn)
        .await
        .unwrap();
    let result = delete_unreferenced_extents(&mut conn, &blobs)
        .await
        .unwrap();
    assert_eq!(result, [orphan.clone()]);
    assert!(!blobs.contains(&orphan).unwrap());
    assert_eq!(blobs.read(&keep).unwrap(), b"keep sibling");
    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(reopened.read(&keep).unwrap(), b"keep sibling");
    assert!(!reopened.contains(&orphan).unwrap());
}

#[devkit::test]
fn seekable_preparation_streams_from_its_current_position_and_deduplicates_pending_groups() {
    use std::io::{Seek, SeekFrom};
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = vec![b'x'; 2 * BLOB_FRAME_BYTES + 17];
    let mut input = std::io::Cursor::new([b"skip".as_slice(), bytes.as_slice()].concat());
    input.seek(SeekFrom::Start(4)).unwrap();
    let name = blobs.put_seekable(&mut input).unwrap();
    input.seek(SeekFrom::Start(4)).unwrap();
    assert_eq!(blobs.put_seekable(&mut input).unwrap(), name);
    assert_eq!(blobs.state().unwrap().prepared.len(), 1);
    assert_eq!(blobs.read(&name).unwrap(), bytes);
    assert_eq!(blobs.state().unwrap().groups.len(), 1);
}

#[devkit::test]
async fn recovery_refuses_an_extent_beyond_the_index_boundary_before_truncating(db:devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn=sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let root=tempfile::tempdir().unwrap();let blobs=Blobs::open(root.path()).unwrap();
    let name=blobs.put(b"retained bytes").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,14,'present')").bind(&name).execute(&mut conn).await.unwrap();
    commit_pending_extents(&mut conn,&blobs).await.unwrap();
    let path=root.path().join(SEGMENT);let original=std::fs::read(&path).unwrap();
    drop(blobs);
    sqlx::query("UPDATE core.blob_segments SET committed_end=0").execute(&mut conn).await.unwrap();
    let reopened=Blobs::open(root.path()).unwrap();
    assert!(reopened.recover(&mut conn).await.unwrap_err().to_string().contains("committed segment boundary"));
    assert_eq!(std::fs::read(path).unwrap(),original);
}

#[devkit::test]
async fn recovery_finishes_catalog_deletion_after_an_earlier_punch_removed_its_extent(db:devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn=sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let name=Hash::of(b"already punched").to_string();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,15,'deleting')").bind(&name).execute(&mut conn).await.unwrap();
    let root=tempfile::tempdir().unwrap();let blobs=Blobs::open(root.path()).unwrap();
    blobs.recover(&mut conn).await.unwrap();
    let (deleted,left)=crate::excision::finish_deletions(&mut conn,&blobs).await.unwrap();
    assert_eq!(deleted,[name]);assert!(left.is_empty());
}

#[devkit::test]
fn an_unloaded_existing_index_never_truncates_stored_segments() {
    let root=tempfile::tempdir().unwrap();let blobs=Blobs::open(root.path()).unwrap();
    blobs.put(b"original").unwrap();blobs.sync_pending().unwrap();drop(blobs);
    let path=root.path().join(SEGMENT);let original=std::fs::read(&path).unwrap();
    let reopened=Blobs::open(root.path()).unwrap();reopened.put(b"later").unwrap();
    assert!(reopened.sync_pending().is_err());assert_eq!(std::fs::read(path).unwrap(),original);
}
