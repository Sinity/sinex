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
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        0
    );
    let full = blobs.names().unwrap().filter_map(|name| name.ok()).count();
    assert_eq!(full, with_rows.len());
    eprintln!(
        "startup names checked: full_walk={full} tracked=0 kept={}",
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
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        0
    );
    // Listing history now would report this file. The recorded names do not.
    std::fs::write(dir.path().join("blobs/history.txt"), b"unrelated").unwrap();
    let orphan = blobs.put(b"stopped before admission").unwrap();
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        1
    );
    // A clean shutdown forgets admitted names but leaves unadmitted ones.
    blobs.put(b"kept history").unwrap();
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        2
    );
    forget_files_with_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        1
    );
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
    assert_eq!(
        blobs
            .unadmitted_names(&unadmitted::DatabasePosition::read(&mut conn).await.unwrap())
            .unwrap()
            .count(),
        0
    );
}

#[devkit::test]
fn seekable_file_records_each_name_and_verifies_repeated_content_without_compression() {
    use std::io::{Seek, SeekFrom};
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    for bytes in [Vec::new(), vec![b'x'; 2 * BLOB_FRAME_BYTES + 17]] {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"skip").unwrap();
        file.write_all(&bytes).unwrap();
        file.seek(SeekFrom::Start(4)).unwrap();
        let name = blobs.put_seekable(&mut file).unwrap();
        assert_eq!(name, format!("blake3:{}", blake3::hash(&bytes).to_hex()));
        assert_eq!(blobs.read(&name).unwrap(), bytes);
        assert!(blobs.recorded_names().unwrap().any(|n| n.unwrap() == name));

        struct RepeatedFile<'a> {
            file: &'a mut std::fs::File,
            root: &'a Path,
            read: usize,
            temporary_files: usize,
        }
        impl Read for RepeatedFile<'_> {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                assert_eq!(
                    std::fs::read_dir(self.root.join("blobs/tmp"))?.count(),
                    self.temporary_files
                );
                let count = self.file.read(out)?;
                self.read += count;
                Ok(count)
            }
        }
        impl Seek for RepeatedFile<'_> {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
                self.file.seek(position)
            }
        }
        // The previous writer prepared and compressed even repeated content.
        file.seek(SeekFrom::Start(4)).unwrap();
        let mut previous = RepeatedFile {
            file: &mut file,
            root: dir.path(),
            read: 0,
            temporary_files: 1,
        };
        let prepared = blobs.0.prepare_plain(&mut previous, 3).unwrap();
        assert_eq!(previous.read, bytes.len());
        assert_eq!(prepared.install().unwrap(), parse(&name).unwrap());
        file.seek(SeekFrom::Start(4)).unwrap();
        blobs.forget_unadmitted(&name).unwrap();
        let mut repeated = RepeatedFile {
            file: &mut file,
            root: dir.path(),
            read: 0,
            temporary_files: 0,
        };
        assert_eq!(blobs.put_seekable(&mut repeated).unwrap(), name);
        assert_eq!(repeated.read, bytes.len());
        assert!(blobs.recorded_names().unwrap().any(|n| n.unwrap() == name));
        let hash = parse(&name).unwrap().hex();
        let path = dir
            .path()
            .join("blobs")
            .join(&hash[..2])
            .join(format!("{hash}.zst"));
        std::fs::write(&path, b"damaged").unwrap();
        file.seek(SeekFrom::Start(4)).unwrap();
        assert!(blobs.put_seekable(&mut file).is_err());
        assert_eq!(std::fs::read(path).unwrap(), b"damaged");
    }
}

#[devkit::test]
fn seekable_file_is_not_installed_when_its_name_cannot_be_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    std::fs::remove_dir(dir.path().join("unadmitted-blobs")).unwrap();
    std::fs::write(dir.path().join("unadmitted-blobs"), b"blocked").unwrap();
    assert!(
        blobs
            .put_seekable(std::io::Cursor::new(b"unadmitted"))
            .is_err()
    );
    assert!(
        !blobs
            .contains(&format!("blake3:{}", blake3::hash(b"unadmitted").to_hex()))
            .unwrap()
    );
}

#[devkit::test]
fn seekable_file_records_content_changed_between_hashing_and_compression() {
    use std::io::{Cursor, SeekFrom};
    struct ChangingInput {
        input: Cursor<Vec<u8>>,
        changed: bool,
    }
    impl Read for ChangingInput {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(bytes)
        }
    }
    impl Seek for ChangingInput {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            if matches!(position, SeekFrom::Start(0)) && !self.changed {
                self.input = Cursor::new(b"second content".to_vec());
                self.changed = true;
            }
            self.input.seek(position)
        }
        fn stream_position(&mut self) -> std::io::Result<u64> {
            self.input.stream_position()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let name = blobs
        .put_seekable(ChangingInput {
            input: Cursor::new(b"first content".to_vec()),
            changed: false,
        })
        .unwrap();
    assert_eq!(blobs.read(&name).unwrap(), b"second content");
    assert!(blobs.recorded_names().unwrap().any(|n| n.unwrap() == name));
}

#[devkit::test]
async fn database_changes_and_rollbacks_require_a_full_file_scan(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    for mismatch in ["system_identifier", "database", "commit_seq"] {
        let name = blobs.put(mismatch.as_bytes()).unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES ($1, 1, 'present')")
            .bind(&name)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("UPDATE core.counters SET value = 10 WHERE name = 'commit_seq'")
            .execute(&mut conn)
            .await
            .unwrap();
        delete_files_without_rows(&mut conn, &blobs).await.unwrap();
        assert_eq!(blobs.recorded_names().unwrap().count(), 0);
        let saved_position = unadmitted::DatabasePosition::read(&mut conn).await.unwrap();
        let saved = serde_json::to_value(saved_position).unwrap();
        sqlx::query("DELETE FROM core.blobs WHERE hash = $1")
            .bind(&name)
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("UPDATE core.counters SET value = $1 WHERE name = 'blob_changes'")
            .bind(saved["blob_changes"].as_i64().unwrap())
            .execute(&mut conn)
            .await
            .unwrap();
        {
            let path = dir.path().join("unadmitted-blobs-ready");
            let mut saved: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            saved[mismatch] = if mismatch == "commit_seq" {
                serde_json::json!(11)
            } else {
                serde_json::json!("another database")
            };
            std::fs::write(path, serde_json::to_vec(&saved).unwrap()).unwrap();
        }
        let found = delete_files_without_rows(&mut conn, &Blobs::open(dir.path()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            found.deleted.as_slice(),
            std::slice::from_ref(&name),
            "{mismatch}"
        );
        assert!(!blobs.contains(&name).unwrap());
    }
}

#[devkit::test]
async fn forgetting_names_cannot_replace_the_first_full_scan(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let name = blobs.put(b"unrecorded historical file").unwrap();
    blobs.forget_unadmitted(&name).unwrap();
    forget_files_with_rows(&mut conn, &blobs).await.unwrap();
    assert!(!dir.path().join("unadmitted-blobs-ready").exists());
    let found = delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(found.deleted, [name]);
}

#[devkit::test]
async fn prepared_writer_can_read_the_database_position(db: devkit::pg::TestDatabase) {
    use sqlx::{Connection, Executor};
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let owner: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    sinex_schema::prepare(&mut conn, &owner).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL ROLE sinex_writer").await.unwrap();
    let position = unadmitted::DatabasePosition::read(&mut tx).await.unwrap();
    assert_eq!(serde_json::to_value(position).unwrap()["commit_seq"], 0);
}

#[devkit::test]
async fn blob_only_changes_require_a_scan_without_an_event_position_change(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let name = blobs.put(b"material that produced no events").unwrap();
    sqlx::query("INSERT INTO core.blobs (hash,size,status) VALUES ($1,1,'present')")
        .bind(&name)
        .execute(&mut conn)
        .await
        .unwrap();
    delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(blobs.recorded_names().unwrap().count(), 0);
    let before = unadmitted::DatabasePosition::read(&mut conn).await.unwrap();
    // Restore a dump from before the blob's admission, with the same event position.
    sqlx::query("DELETE FROM core.blobs WHERE hash=$1")
        .bind(&name)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE core.counters SET value=0 WHERE name='blob_changes'")
        .execute(&mut conn)
        .await
        .unwrap();
    let after = unadmitted::DatabasePosition::read(&mut conn).await.unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap()["commit_seq"],
        serde_json::to_value(after).unwrap()["commit_seq"]
    );
    let found = delete_files_without_rows(&mut conn, &Blobs::open(dir.path()).unwrap())
        .await
        .unwrap();
    assert_eq!(found.deleted, [name]);
}

#[devkit::test]
async fn unclean_stop_checks_only_recorded_names_after_database_progress(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let historical = blobs.put(b"admitted before saved position").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES ($1,1,'present')")
        .bind(&historical)
        .execute(&mut conn)
        .await
        .unwrap();
    delete_files_without_rows(&mut conn, &blobs).await.unwrap();
    assert_eq!(blobs.recorded_names().unwrap().count(), 0);
    // A full scan would report this file; forward progress must use the markers.
    std::fs::write(dir.path().join("blobs/history.txt"), b"unrelated").unwrap();
    let admitted = blobs.put(b"admitted since saved position").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES ($1,1,'present')")
        .bind(&admitted)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE core.counters SET value = value + 1 WHERE name = 'commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    let orphan = blobs.put(b"stopped before admission").unwrap();
    let uninstalled = hashed_blobs::Hash::of(b"stopped before installation");
    blobs.note_unadmitted(uninstalled).unwrap();
    // No shutdown sweep: the saved position still precedes both counters.
    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    let position = unadmitted::DatabasePosition::read(&mut conn).await.unwrap();
    let mut names = reopened
        .unadmitted_names(&position)
        .unwrap()
        .map(|name| name.unwrap())
        .collect::<Vec<_>>();
    names.sort();
    let mut expected = vec![
        admitted.clone(),
        orphan.clone(),
        format!("blake3:{}", uninstalled.hex()),
    ];
    expected.sort();
    assert_eq!(names, expected);
    let found = delete_files_without_rows(&mut conn, &reopened)
        .await
        .unwrap();
    assert_eq!(found.deleted, [orphan.clone()]);
    assert!(found.not_blobs.is_empty());
    assert!(reopened.contains(&historical).unwrap());
    assert!(reopened.contains(&admitted).unwrap());
    assert!(!reopened.contains(&orphan).unwrap());
    assert_eq!(reopened.recorded_names().unwrap().count(), 0);
}
