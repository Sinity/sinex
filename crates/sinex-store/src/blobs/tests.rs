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
    eprintln!("sweep names: initial=1 unchanged=0 installed=1 uninstalled=1");
}

#[devkit::test]
async fn database_changes_and_rollbacks_require_a_full_file_scan(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    for mismatch in ["system_identifier", "database", "commit_seq", "rollback"] {
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
        sqlx::query("DELETE FROM core.blobs WHERE hash = $1")
            .bind(&name)
            .execute(&mut conn)
            .await
            .unwrap();
        if mismatch == "rollback" {
            sqlx::query("UPDATE core.counters SET value = 9 WHERE name = 'commit_seq'")
                .execute(&mut conn)
                .await
                .unwrap();
        } else {
            let path = dir.path().join("unadmitted-blobs-ready");
            let mut saved: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            saved[mismatch] = if mismatch == "commit_seq" {
                serde_json::json!(9)
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
fn blob_writes_measure_marker_syncs_and_keep_the_same_bytes() {
    use std::fs::{File, OpenOptions};
    use std::os::unix::fs::OpenOptionsExt;
    let parent = std::env::var_os("SINEX_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."));
    eprintln!("blob write measurement directory: {}", parent.display());
    let dir = tempfile::tempdir_in(parent).unwrap();
    let mut durations = [Vec::new(), Vec::new()];
    for round in 0..3 {
        // Alternate order so the second path does not always get warmer storage.
        for path in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
            let blobs = Blobs::open(&dir.path().join(format!("{round}-{path}"))).unwrap();
            let started = std::time::Instant::now();
            let mut names = Vec::new();
            for number in 0..16u64 {
                let mut bytes = vec![b'a'; 4096];
                bytes[..8].copy_from_slice(&number.to_le_bytes());
                let name = if path == 0 {
                    let prepared = blobs.0.prepare_plain(bytes.as_slice(), 3).unwrap();
                    let markers = blobs.root().join("unadmitted-blobs");
                    std::fs::create_dir_all(&markers).unwrap();
                    File::open(blobs.root()).unwrap().sync_all().unwrap();
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .mode(0o600)
                        .open(markers.join(prepared.hash().hex()))
                        .unwrap()
                        .sync_all()
                        .unwrap();
                    File::open(markers).unwrap().sync_all().unwrap();
                    format!("blake3:{}", prepared.install().unwrap().hex())
                } else {
                    blobs.put(&bytes).unwrap()
                };
                names.push((name, bytes));
            }
            durations[path].push(started.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(blobs.recorded_names().unwrap().count(), 16);
            for (name, bytes) in names {
                assert_eq!(blobs.read(&name).unwrap(), bytes);
            }
        }
    }
    for (path, mut elapsed) in durations.into_iter().enumerate() {
        elapsed.sort_by(f64::total_cmp);
        eprintln!(
            "blob writes path={} writes=16 bytes_per_write=4096 median_ms={} rounds_ms={elapsed:?}",
            if path == 0 { "before" } else { "after" },
            elapsed[1]
        );
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
