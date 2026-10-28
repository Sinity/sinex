use super::*;
use sinex_model::{
    BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome,
    RunId, Timestamp, event_types::CommandImported,
};

fn batch(tag: &str) -> Batch {
    let event = Event::build(
        &CommandImported {
            command: tag.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "t",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    }
}

#[devkit::test]
async fn live_batches_go_before_queued_bulk_work(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let mut waits = Vec::new();
    for i in 0..5 {
        let h = handle.clone();
        waits.push(tokio::spawn(async move {
            h.submit(Priority::Bulk, batch(&format!("bulk{i}"))).await
        }));
    }
    let h = handle.clone();
    let live = tokio::spawn(async move { h.submit(Priority::Live, batch("live")).await });
    // Everything is queued before the writer starts.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let live_seq = live.await.unwrap().unwrap().commit_seq.unwrap().0;
    let mut bulk_seqs = Vec::new();
    for w in waits {
        bulk_seqs.push(w.await.unwrap().unwrap().commit_seq.unwrap().0);
    }
    assert_eq!(live_seq, 1, "the live batch committed first");
    assert!(bulk_seqs.iter().all(|s| *s > live_seq));
    drop(handle);
    writer.await.unwrap();
}

#[devkit::test]
async fn a_refused_batch_does_not_stop_the_writer(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut bad = batch("x");
    if let RecordOutcome::Events { events, .. } = &mut bad.records[0] {
        events[0].payload["command"] = serde_json::json!("nul\u{0}");
    }
    assert!(matches!(
        handle.submit(Priority::Live, bad).await,
        Err(WriteError::Refused(_))
    ));
    assert_eq!(
        handle
            .submit(Priority::Live, batch("ok"))
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(handle);
    writer.await.unwrap();
}

/// No silent loss under backpressure: with the writer stalled, far
/// more batches than the queues hold are handed over; producers wait
/// rather than drop, and once the writer runs each is admitted once.
#[devkit::test]
async fn a_stalled_writer_makes_producers_wait_and_loses_nothing(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let n = DEPTH * 10;
    let waits: Vec<_> = (0..n)
        .map(|i| {
            let h = handle.clone();
            tokio::spawn(async move { h.submit(Priority::Bulk, batch(&format!("b{i}"))).await })
        })
        .collect();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        waits.iter().all(|w| !w.is_finished()),
        "nothing completes while the writer is stalled"
    );
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    let mut seqs = std::collections::BTreeSet::new();
    for w in waits {
        let out = w.await.unwrap().unwrap();
        assert_eq!(out.admitted, 1);
        assert!(
            seqs.insert(out.commit_seq.unwrap().0),
            "one commit_seq per batch"
        );
    }
    assert_eq!(seqs.len(), n);
    drop(handle);
    writer.await.unwrap();
}

/// One writer per database: a second claim is refused while the
/// first connection lives, and granted once it is gone.
#[devkit::test]
async fn only_one_connection_can_claim_the_writer(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut first = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    claim(&mut first).await.unwrap();
    let mut second = PgConnection::connect(&url).await.unwrap();
    assert!(claim(&mut second).await.is_err());
    first.close().await.unwrap();
    // The server frees the lock when the backend exits, a moment after
    // the client closes.
    let mut freed = false;
    for _ in 0..100 {
        if claim(&mut second).await.is_ok() {
            freed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        freed,
        "the lock is free once the first writer's connection is gone"
    );
}

#[devkit::test]
async fn queued_checks_reject_a_revoked_unlock_and_roll_back_before_commit(
    db: devkit::pg::TestDatabase,
) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let (handle, queues) = channels();
    let unlock = crate::material_access::sealing::Unlock::new(
        sinex_sealing::Identity::generate(),
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let check_unlock = unlock.clone();
    let h = handle.clone();
    let queued = tokio::spawn(async move {
        h.submit_checked(
            Priority::Bulk,
            batch("revoked"),
            Arc::new(move || check_unlock.check()),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while queues.bulk.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    unlock.revoke();
    let writer = tokio::spawn(run(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
    ));
    assert!(matches!(queued.await.unwrap(), Err(WriteError::Refused(_))));
    let calls = Arc::new(AtomicUsize::new(0));
    let checked = calls.clone();
    let result = handle
        .submit_checked(
            Priority::Bulk,
            batch("cancel-before-commit"),
            Arc::new(move || {
                if checked.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err("capture canceled before commit".into())
                }
            }),
        )
        .await;
    assert!(matches!(result, Err(WriteError::Refused(_))));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the second check is inside the admission transaction"
    );
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "revoked queued work and canceled uncommitted events remain absent"
    );
    assert_eq!(
        handle
            .submit(Priority::Live, batch("unrelated retry"))
            .await
            .unwrap()
            .admitted,
        1
    );
    drop(handle);
    writer.await.unwrap();
}

/// A capture that stored its bytes before an excision deleted the same bytes
/// is refused, rather than registering a material whose file is gone.
#[devkit::test]
async fn a_batch_whose_bytes_are_not_stored_is_refused(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        Some(blobs.clone()),
    ));
    let bytes = b"synthetic stored bytes";
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    let source = || Batch {
        run: RunId::new(),
        kind: BatchKind::Source(Box::new(sinex_model::SourceProgress {
            binding: "synthetic".into(),
            data_keys: Vec::new(),
            input: sinex_model::InputRef {
                kind: "file".into(),
                locator: "synthetic".into(),
                host: "h".into(),
            },
            observation: Default::default(),
            revision: sinex_model::RevisionChange::Appended,
            material: Some(sinex_model::MaterialRef {
                id: MaterialId(uuid::Uuid::from_u128(7)),
                input: None,
                blob: blob.clone(),
                length: bytes.len() as u64,
                part_index: 0,
                previous_part: None,
                parser: "zsh".into(),
                parser_version: 1,
                manifest: sinex_model::Manifest::from_json(serde_json::json!({})),
            }),
            cursor: serde_json::json!({"offset": 1}),
            carry: serde_json::Value::Null,
            memory: None,
        })),
        records: vec![],
        archive: vec![],
    };
    let refused = handle.submit(Priority::Bulk, source()).await.unwrap_err();
    assert!(refused.to_string().contains("does not hold"), "{refused}");
    blobs.put(bytes).unwrap();
    handle.submit(Priority::Bulk, source()).await.unwrap();
    drop(handle);
    writer.await.unwrap();
}
