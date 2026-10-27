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
    let key = crate::sealed::new_key().unwrap();
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
async fn files_without_a_row_are_deleted_and_the_rest_kept(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = std::env::temp_dir().join(format!("sinex-without-rows-{}", uuid::Uuid::now_v7()));
    let blobs = Blobs::open(&dir).unwrap();
    // More names than one query checks, so the listing spans several queries.
    let mut with_rows = Vec::new();
    let mut without_rows = Vec::new();
    for n in 0..(NAMES_PER_QUERY + 200) {
        let name = blobs.put(format!("bytes {n}").as_bytes()).unwrap();
        if n % 3 == 0 {
            without_rows.push(name);
        } else {
            with_rows.push(name);
        }
    }
    sqlx::query(
        "INSERT INTO core.blobs (hash, size, status) SELECT unnest($1::text[]), 1, 'present'",
    )
    .bind(&with_rows)
    .execute(&mut conn)
    .await
    .unwrap();
    let stray = dir.join("blobs").join("notes.txt");
    std::fs::write(&stray, b"").unwrap();

    let mut found = delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    found.deleted.sort();
    without_rows.sort();
    assert_eq!(found.deleted, without_rows);
    assert_eq!(found.not_blobs.len(), 1);
    assert!(found.not_blobs[0].contains("notes.txt"));
    assert!(stray.exists());
    for name in &with_rows {
        assert!(blobs.contains(name).unwrap(), "{name} has a row and stays");
    }
    for name in &without_rows {
        assert!(!blobs.contains(name).unwrap(), "{name} has no row and goes");
    }

    // A second start checks no historical files, even though the store keeps
    // more names than one database page.
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 0);
    eprintln!(
        "startup names checked: before={} after=0 kept={}",
        NAMES_PER_QUERY + 200,
        with_rows.len()
    );
    let again = delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    assert!(again.deleted.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}

#[devkit::test]
async fn later_sweeps_check_only_names_written_since_the_previous_sweep(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let name = blobs.put(b"kept history").unwrap();
    sqlx::query("INSERT INTO core.blobs (hash, size, status) VALUES ($1, 12, 'present')")
        .bind(&name)
        .execute(&mut conn)
        .await
        .unwrap();
    delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 0);
    // Listing history now would report this file. The recorded names do not.
    std::fs::write(dir.path().join("blobs/history.txt"), b"unrelated").unwrap();
    let orphan = blobs.put(b"stopped before admission").unwrap();
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 1);
    // A clean shutdown forgets admitted names but leaves unadmitted ones.
    blobs.put(b"kept history").unwrap();
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 2);
    forget_files_with_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 1);
    // A stop before installation leaves only the recorded name.
    blobs
        .note_unadmitted(hashed_blobs::Hash::of(b"not installed"))
        .unwrap();
    let reopened = Blobs::open(dir.path()).unwrap();
    let found = delete_files_without_rows(&mut conn, &reopened)
        .await
        .unwrap();
    assert_eq!(found.deleted, [orphan]);
    assert!(found.not_blobs.is_empty());
    assert!(blobs.contains(&name).unwrap());
    assert_eq!(blobs.unadmitted_names().unwrap().count(), 0);
    eprintln!("sweep candidates: initial=1 unchanged=0 new=1");
}
