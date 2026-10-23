use super::*;
use crate::writer::{self, Priority, WriterHandle};
use serde_json::json;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp, event_types::CommandImported,
};
use std::sync::Arc;

fn imported(command: &str, anchor: u64) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000 + anchor as i64).unwrap()),
            header_elapsed: Some(0),
            n: 0,
        },
        "zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::nil()),
                anchor,
            },
            ordinal: 0,
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

#[tokio::test]
async fn read_models_get_every_event_once_across_restarts() {
    let (_db, url, connection) = crate::testdb::fresh().await;
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

#[tokio::test]
async fn excised_events_never_reach_consumers() {
    let (_db, url, mut connection) = crate::testdb::fresh().await;
    let secret = imported("secret", 0);
    let keep = imported("keep", 1);
    let excise_target = secret.id;
    let mut tx = connection.begin().await.unwrap();
    sinex_store::admit(
        &mut tx,
        &api_batch(vec![secret, keep]),
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    let target = sinex_store::excision::Target::Event(excise_target);
    let plan = sinex_store::excision::plan(&mut tx, target).await.unwrap();
    sinex_store::excision::apply(
        &mut tx,
        target,
        &plan,
        &[],
        sinex_model::OperationId::new(),
        "operator",
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let page = commit_log::after(&mut conn, 0, 10).await.unwrap();
    assert_eq!(page.up_to, Some(2));
    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].payload,
        json!({"command": "keep", "hostname": "h", "started_at": "2023-11-14T22:13:21Z", "header_elapsed": 0, "n": 0})
    );
    let event = page.events[0].to_event().unwrap().unwrap();
    assert_eq!(event.event_type, "command.imported");
    assert!(event.location().is_some());
}
