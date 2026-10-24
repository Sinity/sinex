use super::*;
use crate::Blobs;
use sinex_model::{Class, InputRef, MaterialId};
use sqlx::{Connection, Executor};

fn declaration(bytes: &[u8], frame: &str) -> Publication {
    Publication {
        version: 1,
        frame: frame.into(),
        class: Class::Guarded,
        envelope: None,
        material: Some(Material::from_frame(&MaterialFrame {
            id: MaterialId::new(),
            input: InputRef {
                kind: "file".into(),
                locator: "synthetic".into(),
                host: "h".into(),
            },
            blob: format!("blake3:{}", blake3::hash(bytes).to_hex()),
            length: bytes.len() as u64,
            part_index: 0,
            previous_part: None,
            parser: "lines".into(),
            parser_version: 1,
            manifest: serde_json::json!({}),
        })),
    }
}

#[tokio::test]
async fn producer_reservation_holds_a_shared_name_through_cleanup_install_and_exact_retry() {
    let (db, mut conn) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(root.path()).unwrap();
    let bytes = b"same hash from an independent producer\n";
    let prepared = blobs.prepare_plain(bytes.as_slice()).unwrap();
    let p = declaration(bytes, "pending-final");
    let name = &p.material.as_ref().unwrap().bytes.blob;
    // The previous source's deletion was queued before the independent
    // producer reserved this exact name, as in the original cleanup witness.
    blobs.put(bytes).unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting')")
        .bind(name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    reserve(&mut tx, "producer-b", &p).await.unwrap();
    let mut cleanup = PgConnection::connect(&db.url(None)).await.unwrap();
    cleanup
        .execute("SET default_transaction_isolation='repeatable read'")
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut cleanup)
        .await
        .unwrap();
    let worker_blobs = blobs.clone();
    let worker = tokio::spawn(async move {
        crate::excision::finish_deletions(&mut cleanup, &worker_blobs).await
    });
    let mut observer = PgConnection::connect(&db.url(None)).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                .bind(pid).fetch_one(&mut observer).await.unwrap();
            if waiting {break;}
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }).await.expect("cleanup didn't wait for producer reservation's blob lock");
    tx.commit().await.unwrap();
    assert!(worker.await.unwrap().unwrap().0.is_empty());
    assert_eq!(prepared.install().unwrap(), *name);
    let mut tx = conn.begin().await.unwrap();
    installed(&mut tx, "producer-b", &p).await.unwrap();
    tx.commit().await.unwrap();
    let mut restarted = PgConnection::connect(&db.url(None)).await.unwrap();
    let (frozen, ready) = load(&mut restarted, "producer-b", &p.frame)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen, p);
    assert!(ready);
    assert_eq!(blobs.read(name).unwrap(), bytes);
    let mut changed = p.clone();
    changed.material.as_mut().unwrap().bytes.length += 1;
    let mut tx = restarted.begin().await.unwrap();
    assert!(reserve(&mut tx, "producer-b", &changed).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(
        load(&mut restarted, "producer-b", &p.frame)
            .await
            .unwrap()
            .unwrap()
            .0,
        p
    );
    assert!(
        crate::excision::finish_deletions(&mut restarted, &blobs)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    assert_eq!(blobs.read(name).unwrap(), bytes);
}

#[tokio::test]
async fn cleanup_first_reinstall_uses_prepared_original_bytes_and_reference_projection_cannot_self_authorize()
 {
    let (db, mut conn) = crate::testdb::fresh().await;
    let root = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(root.path()).unwrap();
    let bytes = b"frozen before old source cleanup\n";
    let prepared = blobs.prepare_plain(bytes.as_slice()).unwrap();
    let p = declaration(bytes, "cleanup-first");
    let name = &p.material.as_ref().unwrap().bytes.blob;
    blobs.put(bytes).unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting')")
        .bind(name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        crate::excision::finish_deletions(&mut conn, &blobs)
            .await
            .unwrap()
            .0,
        vec![name.clone()]
    );
    assert!(!blobs.contains(name).unwrap());
    let mut tx = conn.begin().await.unwrap();
    reserve(&mut tx, "producer-b", &p).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM core.blobs WHERE hash=$1")
            .bind(name)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        "reserved"
    );
    assert!(
        !blobs.contains(name).unwrap(),
        "reserved is missing, not retained-byte evidence"
    );
    assert_eq!(prepared.install().unwrap(), *name);
    let mut tx = conn.begin().await.unwrap();
    installed(&mut tx, "producer-b", &p).await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("DELETE FROM raw.pending_producer_blobs WHERE producer='producer-b'")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(load(&mut conn, "producer-b", &p.frame).await.is_err());
    // The projection is reconstructed only from the same typed declaration.
    let mut tx = conn.begin().await.unwrap();
    reserve(&mut tx, "producer-b", &p).await.unwrap();
    tx.commit().await.unwrap();
    let mut restarted = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        load(&mut restarted, "producer-b", &p.frame)
            .await
            .unwrap()
            .unwrap()
            .0,
        p
    );
    assert_eq!(blobs.read(name).unwrap(), bytes);
}
