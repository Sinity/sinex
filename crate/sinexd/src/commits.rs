//! The commit-log reader (architecture §4.3, §7.4) for read models: one task
//! that reads committed events in commit order from `core.commit_log` and
//! feeds every read-model maintainer (`nbi.3`: never database triggers). Each
//! maintainer updates its tables and its position (`core.consumer_positions`)
//! in one transaction, so a restart resumes exactly where it stopped: nothing
//! is skipped, and nothing is applied twice.
//!
//! One page is read for all consumers, from the lowest position among them;
//! each consumer gets the part of the page after its own position.
//! Per-event derivations are run by the automaton driver (`derive.rs`).
use futures::future::BoxFuture;
use sinex_store::committed::{self, Committed};
use sqlx::{Connection, PgConnection};
use std::time::Duration;
use tokio::sync::watch;

/// Events read per page.
const PAGE: i64 = 1_000;
/// How long a failed consumer waits before it is offered the same events again.
const RETRY: Duration = Duration::from_secs(1);

/// Something fed committed events in commit order.
pub trait Consumer: Send + Sync {
    fn name(&self) -> &str;

    /// Whether the consumer reads events of this type. Events it does not
    /// read still advance its position.
    fn wants(&self, event_type: &str) -> bool;

    /// The saved position: the last commit position already handled.
    fn position<'a>(&'a self, conn: &'a mut PgConnection) -> BoxFuture<'a, Result<i64, String>>;

    /// Handle `events` (those it wants, in commit order) and durably record
    /// `up_to` as the new position together with the effect.
    fn handle<'a>(
        &'a mut self,
        events: &'a [Committed],
        up_to: i64,
        conn: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// Run the reader until `stop` is set. `commits` changes whenever the writer
/// commits a batch.
pub async fn run(
    mut conn: PgConnection,
    mut consumers: Vec<Box<dyn Consumer>>,
    mut commits: watch::Receiver<u64>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    if consumers.is_empty() {
        let _ = stop.wait_for(|s| *s).await;
        return Ok(());
    }
    let mut positions = Vec::with_capacity(consumers.len());
    for consumer in &consumers {
        positions.push(consumer.position(&mut conn).await?);
    }
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        commits.borrow_and_update();
        let from = positions.iter().copied().min().unwrap_or(0);
        let page = committed::after(&mut conn, from, PAGE)
            .await
            .map_err(|e| e.to_string())?;
        let Some(up_to) = page.up_to else {
            tokio::select! {
                _ = commits.changed() => {}
                _ = stop.changed() => {}
            }
            continue;
        };
        let mut failed = false;
        for (consumer, position) in consumers.iter_mut().zip(positions.iter_mut()) {
            if *position >= up_to {
                continue;
            }
            let mine: Vec<Committed> = page
                .events
                .iter()
                .filter(|e| e.commit_seq > *position && consumer.wants(&e.event_type))
                .cloned()
                .collect();
            match consumer.handle(&mine, up_to, &mut conn).await {
                Ok(()) => *position = up_to,
                Err(error) => {
                    eprintln!(
                        "sinexd: {} failed after commit position {}: {error}",
                        consumer.name(),
                        *position
                    );
                    failed = true;
                }
            }
        }
        if failed {
            tokio::select! {
                _ = tokio::time::sleep(RETRY) => {}
                _ = stop.changed() => {}
            }
        }
    }
}

/// A table kept only to make reads fast, maintained from committed events
/// (architecture §3.6, `nbi.3`): never by database triggers.
pub trait ReadModel: Send + Sync {
    fn name(&self) -> &'static str;
    fn wants(&self, event_type: &str) -> bool;
    /// Apply `events` inside the open transaction `tx`.
    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        tx: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// Every read model, in one list. The query lane registers `read.search_text`
/// here (architecture §3.6).
pub fn read_models() -> Vec<Box<dyn ReadModel>> {
    Vec::new()
}

/// A read model fed by the reader: its tables and its position change in one
/// transaction.
pub struct ReadModelRunner(pub Box<dyn ReadModel>);

impl Consumer for ReadModelRunner {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn wants(&self, event_type: &str) -> bool {
        self.0.wants(event_type)
    }

    fn position<'a>(&'a self, conn: &'a mut PgConnection) -> BoxFuture<'a, Result<i64, String>> {
        Box::pin(async move {
            committed::consumer_position(conn, self.0.name())
                .await
                .map(|(seq, _)| seq)
                .map_err(|e| e.to_string())
        })
    }

    fn handle<'a>(
        &'a mut self,
        events: &'a [Committed],
        up_to: i64,
        conn: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
            self.0.apply(events, &mut tx).await?;
            committed::set_consumer_position(&mut tx, self.0.name(), up_to, 0)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{self, Priority, WriterHandle};
    use serde_json::json;
    use sinex_model::{
        Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
        RecordOutcome, RunId, Timestamp, kinds::CommandImported,
    };
    use std::sync::Arc;

    fn imported(command: &str, anchor: u64) -> Event {
        Event::build(
            &CommandImported {
                command: command.into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1_700_000_000 + anchor as i64).unwrap(),
                header_elapsed: 0,
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
        let (position, _) = committed::consumer_position(&mut check, "test.counter")
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
        let page = committed::after(&mut conn, 0, 10).await.unwrap();
        assert_eq!(page.up_to, Some(2));
        assert_eq!(page.events.len(), 1);
        assert_eq!(
            page.events[0].payload,
            json!({"command": "keep", "hostname": "h", "started_at": "2023-11-14T22:13:21Z", "header_elapsed": 0})
        );
        let event = page.events[0].to_event().unwrap().unwrap();
        assert_eq!(event.event_type, "command.imported");
        assert!(event.location().is_some());
    }
}
