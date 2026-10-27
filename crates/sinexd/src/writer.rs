//! The writer task: the only database writer. It takes batches from three
//! channels in priority order, live before derived before bulk, so a backfill
//! never starves live capture, and runs admission for each batch in its own
//! transaction.
//!
//! Besides single batches it runs two kinds of work that need one
//! transaction around more than one admission: a replay sequence (archive what
//! is re-read, admit the new readings; in compare mode, read the difference
//! and roll back), and an excision (rewrite bytes, remove readings; delete the
//! freed blob files after commit).
use serde_json::Value;
use sinex_model::{Batch, OperationId};
use sinex_store::excision::{self, Applied, Plan, Target};
use sinex_store::replay::{self, Diff};
use sinex_store::{Blobs, DirtyKeys, Outcome};
use sqlx::{Connection, PgConnection};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Priority {
    /// Live capture; the daemon's source tasks submit these.
    #[allow(dead_code, reason = "the daemon's source tasks submit live batches")]
    Live,
    /// Outputs of derivations, and replays, which re-derive.
    Derived,
    Bulk,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteError {
    #[error("admission refused the batch: {0}")]
    Refused(String),
    #[error("admission is unavailable: {0}")]
    Unavailable(String),
    #[error("the writer has stopped")]
    Stopped,
}

/// One replay sequence: its batches run in one transaction, admitted as they
/// arrive, so only a few are held at a time. A batch arriving as an error
/// (the sequence could not be read) rolls the sequence back.
pub(crate) struct ReplaySequence {
    pub operation: OperationId,
    pub actor: String,
    pub scope: Value,
    /// The sequence's first material, journaled when the sequence commits.
    pub root: uuid::Uuid,
    /// Roll back after reading the difference: a comparison.
    pub compare: bool,
    pub batches: tokio::sync::mpsc::Receiver<Result<Batch, String>>,
}

/// A planned excision.
pub(crate) struct Excision {
    pub target: Target,
    pub plan: Plan,
    pub operation: OperationId,
    pub actor: String,
    /// Where the freed blob files are deleted after commit.
    pub blobs: Blobs,
}

/// What an excision did, and blob files it could not delete after commit
/// (no row refers to them; a later excision of the same bytes retries).
#[derive(Debug)]
pub(crate) struct Excised {
    pub applied: Applied,
    pub files_left: Vec<String>,
}

enum WriterJob {
    Admit(Batch),
    Checked(Batch, AdmissionCheck),
    Replay(ReplaySequence),
    Excise(Box<Excision>),
}

enum Done {
    Admitted(Outcome),
    Replayed(Vec<Outcome>, Diff),
    Excised(Excised),
}

struct Job {
    work: WriterJob,
    reply: oneshot::Sender<Result<Done, WriteError>>,
}

/// Where producers hand batches to the writer.
#[derive(Clone)]
pub(crate) struct WriterHandle {
    live: mpsc::Sender<Job>,
    derived: mpsc::Sender<Job>,
    bulk: mpsc::Sender<Job>,
}

/// A check run just before and after the admission (cancellation, unlock
/// expiry); an error refuses the batch.
pub(crate) type AdmissionCheck = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

impl WriterHandle {
    pub(crate) async fn submit_checked(
        &self,
        priority: Priority,
        batch: Batch,
        check: AdmissionCheck,
    ) -> Result<Outcome, WriteError> {
        match self
            .send(priority, WriterJob::Checked(batch, check))
            .await?
        {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("checked admission returns its outcome"),
        }
    }
    /// Submit a batch and wait until it is committed (or refused).
    pub(crate) async fn submit(
        &self,
        priority: Priority,
        batch: Batch,
    ) -> Result<Outcome, WriteError> {
        match self.send(priority, WriterJob::Admit(batch)).await? {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("an admission answers with its outcome"),
        }
    }

    /// Replay one sequence in one transaction; in compare mode nothing is kept.
    pub(crate) async fn replay(
        &self,
        sequence: ReplaySequence,
    ) -> Result<(Vec<Outcome>, Diff), WriteError> {
        match self
            .send(Priority::Derived, WriterJob::Replay(sequence))
            .await?
        {
            Done::Replayed(outcomes, diff) => Ok((outcomes, diff)),
            _ => unreachable!("a replay answers with its outcomes"),
        }
    }

    pub(crate) async fn excise(&self, excision: Excision) -> Result<Excised, WriteError> {
        match self
            .send(Priority::Live, WriterJob::Excise(Box::new(excision)))
            .await?
        {
            Done::Excised(done) => Ok(done),
            _ => unreachable!("an excision answers with what it removed"),
        }
    }

    async fn send(&self, priority: Priority, work: WriterJob) -> Result<Done, WriteError> {
        let (reply, answer) = oneshot::channel();
        let channel = match priority {
            Priority::Live => &self.live,
            Priority::Derived => &self.derived,
            Priority::Bulk => &self.bulk,
        };
        channel
            .send(Job { work, reply })
            .await
            .map_err(|_| WriteError::Stopped)?;
        answer.await.map_err(|_| WriteError::Stopped)?
    }
}

/// The writer's receiving side, separate from the task so tests can queue work
/// before the writer starts.
pub(crate) struct Queues {
    live: mpsc::Receiver<Job>,
    derived: mpsc::Receiver<Job>,
    bulk: mpsc::Receiver<Job>,
}

/// Channel depth per priority. A full channel makes producers wait, which is
/// the backpressure on capture and imports.
const DEPTH: usize = 16;

pub(crate) fn channels() -> (WriterHandle, Queues) {
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

/// Claim `connection` as the database's one writer. A session advisory lock, so
/// it lasts exactly as long as the connection: a killed writer frees it, a
/// second one is refused.
///
/// A writer that was just killed holds the lock until the server notices its
/// backend is gone, so a restart waits up to [`CLAIM_WAIT`] before refusing.
pub(crate) async fn claim(connection: &mut PgConnection) -> Result<(), String> {
    sinex_schema::require_current(connection)
        .await
        .map_err(|error| error.to_string())?;
    let deadline = tokio::time::Instant::now() + CLAIM_WAIT;
    loop {
        let won = sinex_store::writer_lock::try_claim(connection)
            .await
            .map_err(|e| format!("database: {e}"))?;
        if won {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("another sinexd writer is running against this database".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// How long a starting writer waits for a previous writer's lock to go.
const CLAIM_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Run the writer until every handle is dropped. A failed batch is rolled back
/// and refused; the writer continues with the next. If the connection is lost
/// it reconnects to `url` before the next batch.
///
/// `store` is the content store the batches' bytes are in. The writer admits a
/// batch only while every file it registers is there; it also deletes the
/// files an excision freed. Both happen here, one after the other, so bytes
/// checked before an admission cannot be deleted before it commits. A writer
/// without a store admits only batches that register no bytes.
pub(crate) async fn run(
    url: String,
    connection: PgConnection,
    queues: Queues,
    keys: Arc<dyn DirtyKeys>,
    store: Option<Blobs>,
) {
    run_notifying(url, connection, queues, keys, store, watch::channel(0).0).await;
}

/// [`run`], also counting committed batches on `commits`, which wakes the
/// commit-log reader and the publisher.
pub(crate) async fn run_notifying(
    url: String,
    mut connection: PgConnection,
    mut queues: Queues,
    keys: Arc<dyn DirtyKeys>,
    store: Option<Blobs>,
    commits: watch::Sender<u64>,
) {
    let store = store.as_ref();
    loop {
        let job = tokio::select! {
            biased;
            Some(job) = queues.live.recv() => job,
            Some(job) = queues.derived.recv() => job,
            Some(job) = queues.bulk.recv() => job,
            else => return,
        };
        let result = match job.work {
            WriterJob::Admit(batch) => {
                admit_checked(&mut connection, &batch, keys.as_ref(), store, &|| Ok(()))
                    .await
                    .map(Done::Admitted)
            }
            WriterJob::Checked(batch, check) => {
                admit_checked(&mut connection, &batch, keys.as_ref(), store, &*check)
                    .await
                    .map(Done::Admitted)
            }
            WriterJob::Replay(mut sequence) => {
                replay_sequence(&mut connection, &mut sequence, keys.as_ref())
                    .await
                    .map(|(o, d)| Done::Replayed(o, d))
            }
            WriterJob::Excise(excision) => excise(&mut connection, &excision, keys.as_ref())
                .await
                .map(Done::Excised),
        };
        if result.is_err()
            && connection.ping().await.is_err()
            && let Ok(mut fresh) = PgConnection::connect(&url).await
        {
            // The lock went with the lost connection; without it this
            // process is no longer the writer.
            if let Err(e) = claim(&mut fresh).await {
                let _ = job.reply.send(Err(WriteError::Unavailable(e)));
                return;
            }
            connection = fresh;
        }
        if result.is_ok() {
            commits.send_modify(|n| *n += 1);
        }
        let _ = job.reply.send(result);
    }
}

async fn admit_checked(
    connection: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
    check: &(dyn Fn() -> Result<(), String> + Send + Sync),
) -> Result<Outcome, WriteError> {
    let unavailable = |e: &dyn std::fmt::Display| WriteError::Unavailable(e.to_string());
    check().map_err(WriteError::Refused)?;
    for name in batch.stored_bytes() {
        let present = match store {
            Some(store) => store.contains(name).map_err(|e| unavailable(&e))?,
            None => false,
        };
        if !present {
            return Err(WriteError::Unavailable(format!(
                "the content store does not hold {name}; store the bytes again and resubmit"
            )));
        }
    }
    let mut tx = connection.begin().await.map_err(|e| unavailable(&e))?;
    match sinex_store::admit(&mut tx, batch, keys).await {
        Ok(outcome) => {
            if let Err(error) = check() {
                tx.rollback().await.map_err(|e| unavailable(&e))?;
                return Err(WriteError::Refused(error));
            }
            tx.commit().await.map_err(|e| unavailable(&e))?;
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await.map_err(|e| unavailable(&e))?;
            Err(admission_error(error))
        }
    }
}

/// A transaction that could not run must remain pending at the producer.
/// Only invalid content can be recorded as a final refusal.
fn admission_error(error: sinex_store::Error) -> WriteError {
    let temporary = match &error {
        sinex_store::Error::Database(sqlx::Error::Database(error)) => error.code().is_some_and(|code| {
            ["08", "40", "53", "57", "58"].iter().any(|class| code.starts_with(class))
        }),
        sinex_store::Error::Database(error) => matches!(error,
            sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed),
        sinex_store::Error::Blob(_) => true,
        _ => false,
    };
    if temporary {
        WriteError::Unavailable(error.to_string())
    } else {
        WriteError::Refused(error.to_string())
    }
}

async fn replay_sequence(
    connection: &mut PgConnection,
    sequence: &mut ReplaySequence,
    keys: &dyn DirtyKeys,
) -> Result<(Vec<Outcome>, Diff), WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    replay::begin(
        &mut tx,
        sequence.operation,
        &sequence.actor,
        &sequence.scope,
    )
    .await
    .map_err(|e| refuse(&e))?;
    let mut outcomes = Vec::new();
    while let Some(batch) = sequence.batches.recv().await {
        let batch = batch.map_err(|e| refuse(&e))?;
        outcomes.push(
            sinex_store::admit(&mut tx, &batch, keys)
                .await
                .map_err(|e| refuse(&e))?,
        );
    }
    let admitted: Vec<uuid::Uuid> = outcomes
        .iter()
        .flat_map(|o| o.admitted_ids.iter().map(|id| id.0))
        .collect();
    let diff = replay::diff(&mut tx, sequence.operation, &admitted)
        .await
        .map_err(|e| refuse(&e))?;
    if sequence.compare {
        tx.rollback().await.map_err(|e| refuse(&e))?;
    } else {
        replay::sequence_done(&mut tx, sequence.operation, sequence.root)
            .await
            .map_err(|e| refuse(&e))?;
        tx.commit().await.map_err(|e| refuse(&e))?;
    }
    Ok((outcomes, diff))
}

async fn excise(
    connection: &mut PgConnection,
    e: &Excision,
    keys: &dyn DirtyKeys,
) -> Result<Excised, WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    let applied =
        match excision::apply(&mut tx, e.target, &e.plan, e.operation, &e.actor, keys).await {
            Ok(applied) => {
                tx.commit().await.map_err(|e| refuse(&e))?;
                applied
            }
            Err(error) => {
                tx.rollback().await.map_err(|e| refuse(&e))?;
                return Err(refuse(&error));
            }
        };
    let (_, files_left) = excision::finish_deletions(connection, &e.blobs)
        .await
        .map_err(|e| refuse(&e))?;
    Ok(Excised {
        applied,
        files_left,
    })
}

#[cfg(test)]
mod tests;
