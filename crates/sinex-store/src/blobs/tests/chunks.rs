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
fn offer(blobs: &Blobs, scratch: &Path, bytes: &[u8]) -> Vec<(String, Option<Prepared>)> {
    hashed_blobs::Chunker::plain(hashed_blobs::ChunkSizes::default())
        .chunks(bytes)
        .map(|chunk| {
            let chunk = chunk.unwrap();
            let chunk_name = name(&chunk);
            let prepared = (!blobs.contains(&chunk_name).unwrap()).then(|| {
                Preparation::new(scratch)
                    .unwrap()
                    .plain(chunk.as_slice(), 3)
                    .unwrap()
            });
            (chunk_name, prepared)
        })
        .collect()
}

fn segment_len(root: &Path) -> u64 {
    std::fs::metadata(root.join(SEGMENT)).unwrap().len()
}

#[devkit::test]
fn a_rewrite_changed_in_one_place_stores_only_the_changed_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let first = noise(2 << 20, 7);
    let mut second = first.clone();
    second[1 << 20] ^= 0xff;

    let offered = offer(&blobs, scratch.path(), &first);
    let chunks = offered.len();
    assert!(chunks > 8, "{chunks} chunks");
    blobs
        .put_chunks(&name(&first), first.len() as u64, offered)
        .unwrap();
    blobs.sync_pending().unwrap();
    let before = segment_len(dir.path());

    let offered = offer(&blobs, scratch.path(), &second);
    let new: Vec<u64> = offered
        .iter()
        .filter_map(|(_, prepared)| prepared.as_ref().map(Prepared::decoded_len))
        .collect();
    // One changed byte rewrites the chunk holding it, and at most the next
    // one if the change moved a cut point.
    assert!(matches!(new.len(), 1 | 2), "{new:?}");
    blobs
        .put_chunks(&name(&second), second.len() as u64, offered)
        .unwrap();
    blobs.sync_pending().unwrap();
    let grown = segment_len(dir.path()) - before;
    let new_bytes: u64 = new.iter().sum();
    assert!(
        grown < new_bytes + (64 << 10) && grown * 4 < first.len() as u64,
        "appended {grown} bytes for {new_bytes} new chunk bytes"
    );

    assert_eq!(blobs.read(&name(&second)).unwrap(), second);
    assert_eq!(blobs.read(&name(&first)).unwrap(), first);
    assert_eq!(
        blobs.read_range(&name(&second), (1 << 20) - 3, 6).unwrap(),
        second[(1 << 20) - 3..(1 << 20) + 3]
    );
    assert_eq!(blobs.descriptors(&name(&second)).unwrap().len(), chunks);
}

#[devkit::test]
fn chunks_that_are_not_the_named_bytes_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let bytes = noise(300 << 10, 3);
    let mut offered = offer(&blobs, scratch.path(), &bytes);
    offered.pop();
    let error = blobs
        .put_chunks(&name(&bytes), bytes.len() as u64, offered)
        .unwrap_err();
    assert!(error.to_string().contains("are not"), "{error}");
    assert!(!blobs.contains(&name(&bytes)).unwrap());
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
        let offered = offer(&blobs, scratch.path(), bytes);
        blobs
            .put_chunks(&name(bytes), bytes.len() as u64, offered)
            .unwrap();
        blobs.sync_pending().unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(name(bytes))
            .bind(bytes.len() as i64)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    commit_pending_extents(&mut conn, &blobs).await.unwrap();
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
    assert!(!blobs.contains(&name(&first)).unwrap());
    assert_eq!(blobs.read(&name(&second)).unwrap(), second);

    drop(blobs);
    let reopened = Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(reopened.read(&name(&second)).unwrap(), second);
    assert!(!reopened.contains(&name(&first)).unwrap());
}
