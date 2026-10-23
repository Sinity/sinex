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
            n: 0,
        },
        "t",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
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

#[tokio::test]
async fn live_batches_go_before_queued_bulk_work() {
    let (_db, url, connection) = crate::testdb::fresh().await;
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

#[tokio::test]
async fn a_refused_batch_does_not_stop_the_writer() {
    let (_db, url, connection) = crate::testdb::fresh().await;
    let (handle, queues) = channels();
    let writer = tokio::spawn(run(
        url,
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
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

/// L12. No silent loss under backpressure: with the writer stalled, far
/// more batches than the queues hold are handed over; producers wait
/// rather than drop, and once the writer runs each is admitted once.
#[tokio::test]
async fn a_stalled_writer_makes_producers_wait_and_loses_nothing() {
    let (_db, url, connection) = crate::testdb::fresh().await;
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

/// L43. One writer per database: a second claim is refused while the
/// first connection lives, and granted once it is gone.
#[tokio::test]
async fn only_one_connection_can_claim_the_writer() {
    let (_db, url, mut first) = crate::testdb::fresh().await;
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
