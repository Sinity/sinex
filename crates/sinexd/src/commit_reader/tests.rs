use super::*;
use crate::writer::{self, Priority, WriterHandle};
use serde_json::json;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp, event_types::CommandImported,
};
use std::sync::Arc;

/// The record of `command`, alone in material `n + 1`, started `n` seconds in.
fn imported(command: &str, n: u64) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000 + n as i64).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::from_u128(u128::from(n) + 1)),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn api_batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

/// A read model for tests: records what it was fed, in its own table.
struct Counter;
impl ReadModel for Counter {
    fn name(&self) -> &'static str {
        "test.counter"
    }
    fn wants(&self, event_type: &str) -> bool {
        event_type == "command.imported"
    }
    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        tx: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            for e in events {
                sqlx::query("INSERT INTO test_counter (seq, command) VALUES ($1, $2)")
                    .bind(e.commit_seq)
                    .bind(e.payload["command"].as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        })
    }
}

async fn counted(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM test_counter")
        .fetch_one(conn)
        .await
        .unwrap()
}

async fn wait_until(conn: &mut PgConnection, n: i64) {
    for _ in 0..500 {
        if counted(conn).await >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the read model did not catch up to {n}");
}

async fn submit(handle: &WriterHandle, range: std::ops::Range<u64>) {
    for i in range {
        handle
            .submit(
                Priority::Live,
                api_batch(vec![imported(&format!("c{i}"), i)]),
            )
            .await
            .unwrap();
    }
}

#[devkit::test]
async fn read_models_get_every_event_once_across_restarts(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let connection = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let mut check = PgConnection::connect(&url).await.unwrap();
    // The primary key makes a second application of an event fail.
    sqlx::query("CREATE TABLE test_counter (seq bigint PRIMARY KEY, command text)")
        .execute(&mut check)
        .await
        .unwrap();
    let (handle, queues) = writer::channels();
    let (commits_tx, commits) = watch::channel(0);
    let writer = tokio::spawn(writer::run_notifying(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        None,
        commits_tx,
    ));
    submit(&handle, 0..3).await;

    let start_reader = |commits: watch::Receiver<u64>| {
        let url = url.clone();
        let (stop_tx, stop) = watch::channel(false);
        let task = tokio::spawn(async move {
            let consumers: Vec<Box<dyn Consumer>> =
                vec![Box::new(ReadModelRunner(Box::new(Counter)))];
            run(
                PgConnection::connect(&url).await.unwrap(),
                consumers,
                commits,
                watch::channel(()).0,
                stop,
            )
            .await
        });
        (stop_tx, task)
    };

    let (stop, reader) = start_reader(commits.clone());
    wait_until(&mut check, 3).await;
    // Woken by commits, not by polling: a commit while it runs is picked up.
    submit(&handle, 3..4).await;
    wait_until(&mut check, 4).await;
    stop.send(true).unwrap();
    reader.await.unwrap().unwrap();

    // Events committed while the reader is down are handled after it
    // restarts, and the earlier ones are not handled again.
    submit(&handle, 4..6).await;
    let (stop, reader) = start_reader(commits.clone());
    wait_until(&mut check, 6).await;
    stop.send(true).unwrap();
    reader.await.unwrap().unwrap();

    assert_eq!(counted(&mut check).await, 6);
    let (position, _) = commit_log::consumer_position(&mut check, "test.counter")
        .await
        .unwrap();
    assert_eq!(position, 6);
    drop(handle);
    writer.await.unwrap();
}

#[devkit::test]
async fn excised_events_never_reach_consumers(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut connection = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = sinex_store::Blobs::open(dir.path()).unwrap();
    crate::excision::tests::register(
        &mut connection,
        &blobs,
        &sinex_sources::sources::clipboard::SPEC,
        MaterialId(uuid::Uuid::from_u128(1)),
        b"secret\n",
    )
    .await;
    let secret = imported("secret", 0);
    let keep = imported("keep", 1);
    let excise_target = secret.id;
    let keep_id=keep.id;
    let mut tx = connection.begin().await.unwrap();
    tx.admit(&api_batch(vec![secret, keep]), &sinex_store::NoDerivations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    crate::excision::tests::excise_directly(
        &mut connection,
        &blobs,
        sinex_store::excision::Target::Event(excise_target),
    )
    .await;
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let page = commit_log::after(&mut conn, 0, 10, i64::MAX).await.unwrap();
    let removed_at:i64=sqlx::query_scalar("SELECT commit_seq FROM core.event_tombstones WHERE event_id=$1").bind(excise_target.0).fetch_one(&mut conn).await.unwrap();
    assert_eq!(page.up_to,Some(removed_at),"the event reader covers the excision lifecycle position even though its removed payload is absent");
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].id,keep_id.0);
    assert_eq!(
        page.events[0].payload,
        json!({"command": "keep", "hostname": "h", "started_at": "2023-11-14T22:13:21Z", "header_elapsed": 0, "n": 0})
    );
    let event = page.events[0].to_event().unwrap();
    assert_eq!(event.event_type, "command.imported");
    assert!(event.location().is_some());
}

struct FailingSearch;
impl ReadModel for FailingSearch {
    fn name(&self) -> &'static str {
        sinex_store::read_models::SEARCH_TEXT
    }
    fn wants(&self, _: &str) -> bool {
        true
    }
    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        tx: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            sinex_store::read_models::apply_search_text(tx, events)
                .await
                .map_err(|e| e.to_string())?;
            Err("stopped before saving position".into())
        })
    }
}

#[devkit::test]
async fn search_failure_rolls_back_text_and_restart_covers_same_position(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &api_batch(vec![imported("cargo test", 0)]),
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // Exercise an empty rebuildable index, not admission's eager projection.
    sqlx::query("DELETE FROM core.search_text").execute(&mut conn).await.unwrap();
    let page = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    let mut failing = ReadModelRunner(Box::new(FailingSearch));
    assert!(
        failing
            .handle(&page.events, page.up_to.unwrap(), &mut conn)
            .await
            .is_err()
    );
    assert_eq!(failing.position(&mut conn).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.search_text")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        0
    );
    let mut restarted = ReadModelRunner(Box::new(SearchText));
    restarted
        .handle(&page.events, page.up_to.unwrap(), &mut conn)
        .await
        .unwrap();
    assert_eq!(restarted.position(&mut conn).await.unwrap(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.search_text")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        1
    );
}

struct BatchSizes(tokio::sync::mpsc::UnboundedSender<usize>);
impl ReadModel for BatchSizes {
    fn name(&self) -> &'static str {
        "test.batch_sizes"
    }
    fn wants(&self, _: &str) -> bool {
        true
    }
    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        _: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move { self.0.send(events.len()).map_err(|e| e.to_string()) })
    }
}

#[devkit::test]
async fn maintenance_batches_immediate_backlog_and_flushes_small_tail(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &api_batch((0..10_001).map(|n| imported("cargo", n)).collect()),
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(conn);
    let conn = PgConnection::connect(&url).await.unwrap();
    let (sender, mut batches) = tokio::sync::mpsc::unbounded_channel();
    let (_commits, commits) = watch::channel(0);
    let (stop_tx, stop) = watch::channel(false);
    let reader = tokio::spawn(run(
        conn,
        vec![Box::new(ReadModelRunner(Box::new(BatchSizes(sender))))],
        commits,
        watch::channel(()).0,
        stop,
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), batches.recv())
            .await
            .unwrap(),
        Some(10_000)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), batches.recv())
            .await
            .unwrap(),
        Some(1)
    );
    stop_tx.send(true).unwrap();
    reader.await.unwrap().unwrap();
    let mut check = PgConnection::connect(&url).await.unwrap();
    assert_eq!(
        commit_log::consumer_position(&mut check, "test.batch_sizes")
            .await
            .unwrap()
            .0,
        10_001
    );
}

#[devkit::test]
async fn maintenance_byte_budget_accepts_oversized_first_event_and_commit_gaps(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    // Random-looking bytes keep PostgreSQL from compressing the payload
    // below the byte budget before commit-log pagination measures it.
    let mut command = String::new();
    for n in 0u64..200_000 {
        command.push_str(blake3::hash(&n.to_le_bytes()).to_hex().as_str());
    }
    tx.admit(
        &api_batch(vec![imported(&command, 0), imported("tail", 1)]),
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let first = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(
        first.events[0].payload["command"].as_str(),
        Some(command.as_str())
    );
    let tail = commit_log::after(
        &mut conn,
        first.up_to.unwrap(),
        PAGE,
        sinex_store::PAGE_BYTES,
    )
    .await
    .unwrap();
    assert_eq!(tail.events.len(), 1);
    sqlx::query("DELETE FROM core.commit_log")
        .execute(&mut conn)
        .await
        .unwrap();
    let gap = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    assert!(gap.events.is_empty());
    assert_eq!(gap.up_to, Some(2));
    let mut consumer = ReadModelRunner(Box::new(SearchText));
    consumer.handle(&gap.events, 2, &mut conn).await.unwrap();
    assert_eq!(consumer.position(&mut conn).await.unwrap(), 2);
}
