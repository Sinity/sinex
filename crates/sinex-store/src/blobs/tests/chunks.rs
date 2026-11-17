//! Bytes rewritten whole, stored as content-defined chunks.
use super::*;

/// Incompressible bytes, so a chunk's stored size tracks its length.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn name(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

/// The chunks of `bytes` as a producer offers them: each one Sinex does not
/// hold is prepared, the others are named only.
async fn offer(
    blobs: &Blobs,
    conn: &mut sqlx::PgConnection,
    scratch: &Path,
    bytes: &[u8],
) -> Vec<(String, ChunkBytes)> {
    let mut offered = Vec::new();
    for chunk in hashed_blobs::Chunker::plain(hashed_blobs::ChunkSizes::default()).chunks(bytes) {
        let chunk = chunk.unwrap();
        let name = name(&chunk);
        let bytes = if blobs.contains(conn, &name).await.unwrap() {
            ChunkBytes::Located(blobs.locate(conn, &name).await.unwrap())
        } else {
            ChunkBytes::Prepared(
                Preparation::new(scratch)
                    .unwrap()
                    .plain(chunk.as_slice(), 3)
                    .unwrap(),
            )
        };
        offered.push((name, bytes));
    }
    offered
}

fn segment_len(root: &Path) -> u64 {
    std::fs::metadata(root.join(SEGMENT)).unwrap().len()
}

#[devkit::test]
async fn start_reads_no_extent_rows_and_cold_lookup_preserves_chunk_order(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = noise(512 << 10, 19);
    let blob = name(&bytes);
    blobs
        .put_chunks(
            &blob,
            bytes.len() as u64,
            offer(&blobs, &mut conn, scratch.path(), &bytes).await,
        )
        .unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&blob))
        .await
        .unwrap();
    let original = std::fs::read(dir.path().join(SEGMENT)).unwrap();
    let expected = blobs
        .descriptors(&blobs.locate(&mut conn, &blob).await.unwrap())
        .unwrap();
    let chunks: Vec<String> =
        sqlx::query_scalar("SELECT chunk FROM core.blob_chunks WHERE blob=$1 ORDER BY ordinal")
            .bind(&blob)
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let mut sorted = chunks.clone();
    sorted.sort();
    assert_ne!(
        chunks, sorted,
        "fixture must distinguish chunk order from hash order"
    );
    drop(blobs);
    // Unrelated malformed rows discriminate point lookup from hydration of
    // the entire catalog, even when that whole scan is warm and fast.
    sqlx::query("INSERT INTO core.blob_extents(hash,descriptor,deleting) SELECT 'blake3:' || repeat(md5(g::text),2),convert_to('invalid','UTF8'),false FROM generate_series(1,10000) g")
        .execute(&mut conn).await.unwrap();
    let reopened = Blobs::open(dir.path()).unwrap();
    let mut tx = conn.begin().await.unwrap();
    let before: (i64,i64) = sqlx::query_as("SELECT seq_scan,idx_scan FROM pg_stat_xact_user_tables WHERE relid='core.blob_extents'::regclass")
        .fetch_one(&mut *tx).await.unwrap();
    reopened.load(&mut tx).await.unwrap();
    let after: (i64,i64) = sqlx::query_as("SELECT seq_scan,idx_scan FROM pg_stat_xact_user_tables WHERE relid='core.blob_extents'::regclass")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!((after.0 - before.0, after.1 - before.1), (0, 0));
    tx.rollback().await.unwrap();
    let located = reopened.locate(&mut conn, &blob).await.unwrap();
    assert_eq!(reopened.descriptors(&located).unwrap(), expected);
    assert_eq!(reopened.read(&located).unwrap(), bytes);
    let error = reopened
        .locate(
            &mut conn,
            &format!("blake3:{}", "c4ca4238a0b923820dcc509a6f75849b".repeat(2)),
        )
        .await;
    assert!(
        error.is_err(),
        "a selected malformed descriptor is diagnosed"
    );
    assert_eq!(std::fs::read(dir.path().join(SEGMENT)).unwrap(), original);
    let state = reopened.state().unwrap();
    assert!(
        state.prepared.is_empty()
            && state.groups.is_empty()
            && state.unrecorded_chunk_lists.is_empty()
    );
}

#[devkit::test]
async fn a_rewrite_changed_in_one_place_stores_only_the_changed_chunks(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let first = noise(2 << 20, 7);
    let mut second = first.clone();
    second[1 << 20] ^= 0xff;

    let offered = offer(&blobs, &mut conn, scratch.path(), &first).await;
    let chunks = offered.len();
    assert!(chunks > 8, "{chunks} chunks");
    blobs
        .put_chunks(&name(&first), first.len() as u64, offered)
        .unwrap();
    commit_pending_extents(&mut conn, &blobs, &[name(&first)])
        .await
        .unwrap();
    let before = segment_len(dir.path());

    let offered = offer(&blobs, &mut conn, scratch.path(), &second).await;
    let new: Vec<u64> = offered
        .iter()
        .filter_map(|(_, bytes)| match bytes {
            ChunkBytes::Prepared(value) => Some(value.decoded_len()),
            ChunkBytes::Located(_) => None,
        })
        .collect();
    // One changed byte rewrites the chunk holding it, and at most the next
    // one if the change moved a cut point.
    assert!(matches!(new.len(), 1 | 2), "{new:?}");
    blobs
        .put_chunks(&name(&second), second.len() as u64, offered)
        .unwrap();
    commit_pending_extents(&mut conn, &blobs, &[name(&second)])
        .await
        .unwrap();
    let grown = segment_len(dir.path()) - before;
    let new_bytes: u64 = new.iter().sum();
    assert!(
        grown < new_bytes + (64 << 10) && grown * 4 < first.len() as u64,
        "appended {grown} bytes for {new_bytes} new chunk bytes"
    );

    assert_eq!(
        blobs
            .read(&blobs.locate(&mut conn, &name(&second)).await.unwrap())
            .unwrap(),
        second
    );
    assert_eq!(
        blobs
            .read(&blobs.locate(&mut conn, &name(&first)).await.unwrap())
            .unwrap(),
        first
    );
    assert_eq!(
        blobs
            .read_range(
                &blobs.locate(&mut conn, &name(&second)).await.unwrap(),
                (1 << 20) - 3,
                6
            )
            .unwrap(),
        second[(1 << 20) - 3..(1 << 20) + 3]
    );
    assert_eq!(
        blobs
            .descriptors(&blobs.locate(&mut conn, &name(&second)).await.unwrap())
            .unwrap()
            .len(),
        chunks
    );
}

#[devkit::test]
async fn chunks_that_are_not_the_named_bytes_are_refused(db: devkit::pg::TestDatabase) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = noise(300 << 10, 3);
    let mut offered = offer(&blobs, &mut conn, scratch.path(), &bytes).await;
    offered.pop();
    let error = blobs
        .put_chunks(&name(&bytes), bytes.len() as u64, offered)
        .unwrap_err();
    assert!(error.to_string().contains("are not"), "{error}");
    assert!(!blobs.contains(&mut conn, &name(&bytes)).await.unwrap());
}

#[devkit::test]
async fn removing_a_chunked_blob_keeps_the_chunks_another_blob_holds(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let first = noise(1 << 20, 11);
    let mut second = first.clone();
    second[600 << 10] ^= 0xff;
    for bytes in [&first, &second] {
        let offered = offer(&blobs, &mut conn, scratch.path(), bytes).await;
        blobs
            .put_chunks(&name(bytes), bytes.len() as u64, offered)
            .unwrap();
        commit_pending_extents(&mut conn, &blobs, &[name(bytes)])
            .await
            .unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(name(bytes))
            .bind(bytes.len() as i64)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    commit_pending_extents(&mut conn, &blobs, &[name(&first), name(&second)])
        .await
        .unwrap();
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM core.blob_extents")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let only_first: Vec<String> = sqlx::query_scalar(
        "SELECT chunk FROM core.blob_chunks WHERE blob=$1
         EXCEPT SELECT chunk FROM core.blob_chunks WHERE blob=$2",
    )
    .bind(name(&first))
    .bind(name(&second))
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert!(matches!(only_first.len(), 1 | 2), "{only_first:?}");

    sqlx::query("UPDATE core.blobs SET status='deleting' WHERE hash=$1")
        .bind(name(&first))
        .execute(&mut conn)
        .await
        .unwrap();
    let (removed, left) = crate::lifecycle::remove_unreferenced(&mut conn, &blobs)
        .await
        .unwrap();
    assert_eq!((removed, left), (vec![name(&first)], vec![]));
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM core.blob_extents")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(remaining, stored - only_first.len() as i64);
    let lists: Vec<String> = sqlx::query_scalar("SELECT DISTINCT blob FROM core.blob_chunks")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(lists, [name(&second)]);
    assert!(!blobs.contains(&mut conn, &name(&first)).await.unwrap());
    assert_eq!(
        blobs
            .read(&blobs.locate(&mut conn, &name(&second)).await.unwrap())
            .unwrap(),
        second
    );

    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(
        reopened
            .read(&reopened.locate(&mut conn, &name(&second)).await.unwrap())
            .unwrap(),
        second
    );
    assert!(!reopened.contains(&mut conn, &name(&first)).await.unwrap());
}

#[devkit::test]
async fn unrelated_commit_and_cleanup_keep_pending_chunk_ownership(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let shared = noise(4096, 17);
    let shared_name = blobs.put(&shared).unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&shared_name))
        .await
        .unwrap();
    let tail = noise(4096, 91);
    let mut original = shared.clone();
    original.extend_from_slice(&tail);
    let pending = name(&original);
    blobs
        .put_chunks(
            &pending,
            original.len() as u64,
            vec![
                (
                    shared_name.clone(),
                    ChunkBytes::Located(blobs.locate(&mut conn, &shared_name).await.unwrap()),
                ),
                (
                    name(&tail),
                    ChunkBytes::Prepared(
                        Preparation::new(scratch.path())
                            .unwrap()
                            .plain(tail.as_slice(), 3)
                            .unwrap(),
                    ),
                ),
            ],
        )
        .unwrap();
    let unrelated = blobs.put(b"independently admitted bytes").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,28,'present')")
        .bind(&unrelated)
        .execute(&mut conn)
        .await
        .unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&unrelated))
        .await
        .unwrap();
    assert!(blobs.held_by_pending_chunk(&shared_name).unwrap());
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM core.blob_chunks WHERE blob=$1")
        .bind(&pending)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        recorded, 0,
        "an unrelated commit cannot acknowledge this chunk list"
    );
    delete_unreferenced_extents(&mut conn, &blobs)
        .await
        .unwrap();
    let retained: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.blob_extents WHERE hash=$1)")
            .bind(&shared_name)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(
        retained,
        "cleanup keeps the catalog as well as the shared bytes"
    );
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&pending)
        .bind(original.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    commit_pending_extents(&mut conn, &blobs, std::slice::from_ref(&pending))
        .await
        .unwrap();
    assert!(!blobs.held_by_pending_chunk(&shared_name).unwrap());
    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(
        reopened
            .read(&reopened.locate(&mut conn, &pending).await.unwrap())
            .unwrap(),
        original
    );
}
