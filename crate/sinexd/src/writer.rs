//! The writer task: the only database writer (architecture §4.3, §7.4). It
//! takes batches from three channels in priority order, live before derived
//! before bulk, so a backfill never starves live capture, and runs admission
//! for each batch in its own transaction.
use sinex_model::Batch;
use sinex_store::{DirtyKeys, Outcome};
use sqlx::{Connection, PgConnection};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    /// Live capture; the daemon's source tasks submit these.
    #[allow(dead_code, reason = "the daemon's source tasks submit live batches")]
    Live,
    /// Outputs of derivations; the automaton driver (§6.2) submits these.
    #[allow(
        dead_code,
        reason = "the automaton driver lane submits derived batches"
    )]
    Derived,
    Bulk,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("admission refused the batch: {0}")]
    Refused(String),
    #[error("the writer has stopped")]
    Stopped,
}

struct Job {
    batch: Batch,
    reply: oneshot::Sender<Result<Outcome, WriteError>>,
}

/// Where producers hand batches to the writer.
#[derive(Clone)]
pub struct WriterHandle {
    live: mpsc::Sender<Job>,
    derived: mpsc::Sender<Job>,
    bulk: mpsc::Sender<Job>,
}

impl WriterHandle {
    /// Submit a batch and wait until it is committed (or refused).
    pub async fn submit(&self, priority: Priority, batch: Batch) -> Result<Outcome, WriteError> {
        let (reply, answer) = oneshot::channel();
        let channel = match priority {
            Priority::Live => &self.live,
            Priority::Derived => &self.derived,
            Priority::Bulk => &self.bulk,
        };
        channel
            .send(Job { batch, reply })
            .await
            .map_err(|_| WriteError::Stopped)?;
        answer.await.map_err(|_| WriteError::Stopped)?
    }
}

/// The writer's receiving side, separate from the task so tests can queue work
/// before the writer starts.
pub struct Queues {
    live: mpsc::Receiver<Job>,
    derived: mpsc::Receiver<Job>,
    bulk: mpsc::Receiver<Job>,
}

/// Channel depth per priority. A full channel makes producers wait, which is
/// the backpressure on capture and imports.
const DEPTH: usize = 16;

pub fn channels() -> (WriterHandle, Queues) {
    let (live, live_rx) = mpsc::channel(DEPTH);
    let (derived, derived_rx) = mpsc::channel(DEPTH);
    let (bulk, bulk_rx) = mpsc::channel(DEPTH);
    (
        WriterHandle {
            live,
            derived,
            bulk,
        },
        Queues {
            live: live_rx,
            derived: derived_rx,
            bulk: bulk_rx,
        },
    )
}

/// Run the writer until every handle is dropped. A failed batch is rolled back
/// and refused; the writer continues with the next. If the connection is lost
/// it reconnects to `url` before the next batch.
pub async fn run(
    url: String,
    mut connection: PgConnection,
    mut queues: Queues,
    keys: Arc<dyn DirtyKeys>,
) {
    loop {
        let job = tokio::select! {
            biased;
            Some(job) = queues.live.recv() => job,
            Some(job) = queues.derived.recv() => job,
            Some(job) = queues.bulk.recv() => job,
            else => return,
        };
        let result = admit(&mut connection, &job.batch, keys.as_ref()).await;
        if let Err(WriteError::Refused(_)) = &result
            && connection.ping().await.is_err()
            && let Ok(fresh) = PgConnection::connect(&url).await
        {
            connection = fresh;
        }
        let _ = job.reply.send(result);
    }
}

async fn admit(
    connection: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> Result<Outcome, WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    let outcome = sinex_store::admit(&mut tx, batch, keys)
        .await
        .map_err(|e| refuse(&e))?;
    tx.commit().await.map_err(|e| refuse(&e))?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_model::{
        BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome,
        RunId, Timestamp, kinds::CommandImported,
    };

    fn batch(tag: &str) -> Batch {
        let event = Event::build(
            &CommandImported {
                command: tag.into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
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
}
