//! The writer task: the only database writer. It takes batches from three
//! channels in priority order, live before derived before bulk, so a stage
//! never starves live capture, and runs admission for each batch in its own
//! transaction.
//!
//! Besides single batches it runs a replay sequence (archive what is re-read,
//! admit the new readings; in compare mode, read the difference and roll
//! back) in one transaction, and one batch of a lifecycle operation at a time
//! (punching the extents of bytes a tombstone freed after commit), so live
//! batches are admitted between an operation's batches.

use serde_json::Value;
use sinex_model::{Batch, OperationId};
use sinex_store::lifecycle::{self, Progress, Refusal, Step};
use sinex_store::replay::{self, Diff};
use sinex_store::{AdmissionContext, Blobs, DirtyKeys, Outcome};
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
    #[error("database unavailable: {0}")]
    Unavailable(String),
    #[error("the writer has stopped")]
    Stopped,
}

fn store_error(error: sinex_store::Error) -> WriteError {
    admission_error(error)
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
    pub ingress: Option<crate::ingress::replay::CheckedReplay>,
}

enum WriterJob {
    Admit(Batch),
    Signals(Option<Batch>, Box<sinex_store::signals::Commit>),
    Checked(Batch, AdmissionCheck),
    Curation(Batch, AdmissionCheck),
    Source(Batch, AdmissionCheck, Option<StageCheckpoint>),
    Replay(Box<ReplaySequence>),
    Lifecycle {
        operation: Box<lifecycle::Operation>,
        progress: Progress,
        roots: i64,
    },
    LifecycleEnd {
        operation: OperationId,
        refusals: Vec<Refusal>,
    },
    RecordReplay {
        operation: OperationId,
        actor: String,
        scope: Value,
        refusals: Vec<Value>,
    },
    Stage(StageWrite),
    BackupHold {
        operation: OperationId,
        actor: String,
        snapshot: bool,
    },
    BackupRelease {
        hold: OperationId,
        operation: OperationId,
        actor: String,
    },
}

/// A staged binding's accounting once a batch commits, short of admission's
/// decisions for that batch: the writer adds them and saves it under the
/// batch's binding in the transaction that saves its cursor, so a
/// resumed stage's accounting includes exactly what was committed.
#[derive(Clone, Debug)]
pub(crate) struct StageCheckpoint {
    pub operation: OperationId,
    pub accounting: sinex_model::Accounting,
}

/// What a commit saves beside its batch.
#[derive(Clone, Copy)]
enum Saved<'a> {
    Nothing,
    Stage(&'a StageCheckpoint),
    Curation,
}

/// What a stage operation writes besides its batches: its record in the
/// operations log, and the files it stores with no parser.
pub(crate) enum StageWrite {
    Begin {
        operation: OperationId,
        actor: String,
        scope: Value,
        inventory: sinex_model::stage::Inventory,
    },
    Progress {
        operation: OperationId,
        inventory: sinex_model::stage::Inventory,
    },
    /// A stage that stopped early is being resumed: it has no result again.
    Reopen {
        operation: OperationId,
    },
    End {
        operation: OperationId,
        result: sinex_model::lifecycle::OperationResult,
        counts: sinex_model::stage::Counts,
        inventory: sinex_model::stage::Inventory,
        refusals: Vec<String>,
    },
    Unparsed(Box<sinex_store::stages::Unparsed>),
}

enum Done {
    Admitted(Outcome),
    Replayed(Vec<Outcome>, Diff),
    Lifecycle(Step),
    Backup(
        sinex_model::backup::Hold,
        Option<Box<sinex_store::backup::Snapshot>>,
    ),
}

struct Job {
    queued_at: std::time::Instant,
    work: WriterJob,
    reply: oneshot::Sender<Result<Done, WriteError>>,
}

/// Where producers hand batches to the writer.
#[derive(Clone)]
pub(crate) struct WriterHandle {
    derivations: Arc<tokio::sync::Mutex<()>>,
    signals: Arc<tokio::sync::Mutex<()>>,
    curation: Arc<tokio::sync::Mutex<()>>,
    live: mpsc::Sender<Job>,
    derived: mpsc::Sender<Job>,
    bulk: mpsc::Sender<Job>,
}

/// A check run just before and after the admission (cancellation, unlock
/// expiry); an error refuses the batch.
pub(crate) type AdmissionCheck = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

impl WriterHandle {
    pub(crate) async fn lock_signals(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.signals.clone().lock_owned().await
    }

    /// Held while a library curation request is planned and admitted, so
    /// requests from this daemon never plan against each other's snapshot.
    pub(crate) async fn lock_curation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.curation.clone().lock_owned().await
    }

    /// Shared by keyed computations in commands and the background runner.
    pub(crate) async fn lock_derivations(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.derivations.clone().lock_owned().await
    }

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
    /// Admit planned library curation records only if they still place as
    /// planned (their bases current, their slots theirs), checked inside the
    /// admitting transaction.
    pub(crate) async fn submit_curation(
        &self,
        priority: Priority,
        batch: Batch,
        check: AdmissionCheck,
    ) -> Result<Outcome, WriteError> {
        match self
            .send(priority, WriterJob::Curation(batch, check))
            .await?
        {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("guarded admission returns its outcome"),
        }
    }
    /// A source advances past refused content, retaining its bytes and failures.
    pub(crate) async fn submit_source(
        &self,
        priority: Priority,
        batch: Batch,
        check: AdmissionCheck,
        stage: Option<StageCheckpoint>,
    ) -> Result<Outcome, WriteError> {
        match self
            .send(priority, WriterJob::Source(batch, check, stage))
            .await?
        {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("source admission returns its outcome"),
        }
    }

    /// Signal events and their evaluator, host policy and notification
    /// checkpoint are one transaction. Effects run only after this method
    /// acknowledges it. `batch` holds outcome and report records, when any.
    pub(crate) async fn submit_signals(
        &self,
        batch: Option<Batch>,
        checkpoint: sinex_store::signals::Commit,
    ) -> Result<Outcome, WriteError> {
        match self
            .send(
                Priority::Live,
                WriterJob::Signals(batch, Box::new(checkpoint)),
            )
            .await?
        {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("signal admission returns its outcome"),
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
            .send(Priority::Derived, WriterJob::Replay(Box::new(sequence)))
            .await?
        {
            Done::Replayed(outcomes, diff) => Ok((outcomes, diff)),
            _ => unreachable!("a replay answers with its outcomes"),
        }
    }

    pub(crate) async fn record_replay(
        &self,
        operation: OperationId,
        actor: String,
        scope: Value,
        refusals: Vec<Value>,
    ) -> Result<(), WriteError> {
        self.send(
            Priority::Derived,
            WriterJob::RecordReplay {
                operation,
                actor,
                scope,
                refusals,
            },
        )
        .await?;
        Ok(())
    }

    /// Write a stage's log entry or one of its unparsed files, in its own
    /// transaction.
    pub(crate) async fn stage(&self, write: StageWrite) -> Result<(), WriteError> {
        self.send(Priority::Derived, WriterJob::Stage(write))
            .await?;
        Ok(())
    }

    pub(crate) async fn backup_hold(
        &self,
        actor: &str,
    ) -> Result<sinex_model::backup::Hold, WriteError> {
        match self
            .send(
                Priority::Derived,
                WriterJob::BackupHold {
                    operation: OperationId::new(),
                    actor: actor.into(),
                    snapshot: false,
                },
            )
            .await?
        {
            Done::Backup(hold, _) => Ok(hold),
            _ => unreachable!("a backup hold returns its boundary"),
        }
    }

    pub(crate) async fn backup_snapshot(
        &self,
        actor: &str,
    ) -> Result<sinex_store::backup::Snapshot, WriteError> {
        match self
            .send(
                Priority::Derived,
                WriterJob::BackupHold {
                    operation: OperationId::new(),
                    actor: actor.into(),
                    snapshot: true,
                },
            )
            .await?
        {
            Done::Backup(_, Some(snapshot)) => Ok(*snapshot),
            _ => unreachable!("a backup preparation returns its exported snapshot"),
        }
    }

    pub(crate) async fn backup_release(
        &self,
        hold: OperationId,
        actor: &str,
    ) -> Result<sinex_model::backup::Hold, WriteError> {
        match self
            .send(
                Priority::Derived,
                WriterJob::BackupRelease {
                    hold,
                    operation: OperationId::new(),
                    actor: actor.into(),
                },
            )
            .await?
        {
            Done::Backup(hold, _) => Ok(hold),
            _ => unreachable!("a backup release returns its retained boundary"),
        }
    }

    /// Run one batch of a lifecycle operation in its own transaction.
    pub(crate) async fn lifecycle_step(
        &self,
        operation: &lifecycle::Operation,
        progress: Progress,
        roots: i64,
    ) -> Result<Step, WriteError> {
        match self
            .send(
                Priority::Derived,
                WriterJob::Lifecycle {
                    operation: Box::new(operation.clone()),
                    progress,
                    roots,
                },
            )
            .await?
        {
            Done::Lifecycle(step) => Ok(step),
            _ => unreachable!("a lifecycle batch answers with its step"),
        }
    }

    /// End an operation `stopped_early`, recording what refused it.
    pub(crate) async fn lifecycle_stopped(
        &self,
        operation: OperationId,
        refusals: Vec<Refusal>,
    ) -> Result<(), WriteError> {
        self.send(
            Priority::Derived,
            WriterJob::LifecycleEnd {
                operation,
                refusals,
            },
        )
        .await?;
        Ok(())
    }

    async fn send(&self, priority: Priority, work: WriterJob) -> Result<Done, WriteError> {
        let (reply, answer) = oneshot::channel();
        let channel = match priority {
            Priority::Live => &self.live,
            Priority::Derived => &self.derived,
            Priority::Bulk => &self.bulk,
        };
        channel
            .send(Job {
                queued_at: std::time::Instant::now(),
                work,
                reply,
            })
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
            derivations: Arc::new(tokio::sync::Mutex::new(())),
            signals: Arc::new(tokio::sync::Mutex::new(())),
            curation: Arc::new(tokio::sync::Mutex::new(())),
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
/// backend is gone, so a restart waits up to five seconds before refusing.
pub(crate) async fn claim(connection: PgConnection) -> Result<AdmissionContext, String> {
    let mut context = AdmissionContext::claim(connection)
        .await
        .map_err(|error| error.to_string())?;
    sinex_schema::kinds::require_bundled(&mut context)
        .await
        .map_err(|error| error.to_string())?;
    Ok(context)
}

/// Run the writer until every handle is dropped. A failed batch is rolled back
/// and refused; the writer continues with the next. If the connection is lost
/// it reconnects to `url` before the next batch.
///
/// `store` is the content store the batches' bytes are in. The writer admits a
/// batch only while every file it registers is there; it also deletes the
/// bytes a tombstone freed. Both happen here, one after the other, so bytes
/// checked before an admission cannot be deleted before it commits. A writer
/// without a store admits only batches that register no bytes.
pub(crate) async fn run(
    url: String,
    connection: AdmissionContext,
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
    mut connection: AdmissionContext,
    mut queues: Queues,
    keys: Arc<dyn DirtyKeys>,
    store: Option<Blobs>,
    commits: watch::Sender<u64>,
) {
    connection = connection.with_blobs(store.clone());
    let store = store.as_ref();
    let mut released_punches = false;
    loop {
        let job = tokio::select! {
            biased;
            Some(job) = queues.live.recv() => job,
            Some(job) = queues.derived.recv() => job,
            Some(job) = queues.bulk.recv() => job,
            else => return,
        };
        // Release dirties deferred physical work. The next ordinary writer group
        // attempts it; no reopen, timer, or second owner is needed.
        if released_punches && let Some(store) = store {
            if let Err(error) = lifecycle::remove_unreferenced(&mut connection, store).await {
                tracing_punch_error(&error);
            }
            if let Err(error) =
                sinex_store::blobs::finish_deleting_extents(&mut connection, store).await
            {
                tracing_punch_error(&error);
            }
            released_punches = false;
        }
        let queue_time = job.queued_at.elapsed();
        let offered = match &job.work {
            WriterJob::Admit(batch)
            | WriterJob::Signals(Some(batch), _)
            | WriterJob::Checked(batch, _)
            | WriterJob::Curation(batch, _)
            | WriterJob::Source(batch, _, _) => Some(batch.event_count()),
            _ => None,
        };
        let service_started = std::time::Instant::now();
        let mut result = match job.work {
            WriterJob::Admit(batch) => admit_checked(
                &mut connection,
                &batch,
                keys.as_ref(),
                store,
                &|| Ok(()),
                false,
            )
            .await
            .map(Done::Admitted),
            WriterJob::Signals(batch, checkpoint) => admit_signals(
                &mut connection,
                batch.as_ref(),
                keys.as_ref(),
                store,
                &checkpoint,
            )
            .await
            .map(Done::Admitted),
            WriterJob::Curation(batch, check) => admit_with_checkpoint(
                &mut connection,
                &batch,
                keys.as_ref(),
                store,
                &*check,
                false,
                Saved::Curation,
            )
            .await
            .map(Done::Admitted),
            WriterJob::Checked(batch, check) => admit_checked(
                &mut connection,
                &batch,
                keys.as_ref(),
                store,
                &*check,
                false,
            )
            .await
            .map(Done::Admitted),
            WriterJob::Source(batch, check, stage) => admit_with_checkpoint(
                &mut connection,
                &batch,
                keys.as_ref(),
                store,
                &*check,
                true,
                stage.as_ref().map_or(Saved::Nothing, Saved::Stage),
            )
            .await
            .map(Done::Admitted),
            WriterJob::Replay(mut sequence) => {
                replay_sequence(&mut connection, &mut sequence, keys.as_ref())
                    .await
                    .map(|(o, d)| Done::Replayed(o, d))
            }
            WriterJob::RecordReplay {
                operation,
                actor,
                scope,
                refusals,
            } => record_replay(&mut connection, operation, &actor, &scope, &refusals)
                .await
                .map(Done::Admitted),
            WriterJob::Lifecycle {
                operation,
                progress,
                roots,
            } => lifecycle_step(
                &mut connection,
                &operation,
                progress,
                roots,
                keys.as_ref(),
                store,
            )
            .await
            .map(Done::Lifecycle),
            WriterJob::LifecycleEnd {
                operation,
                refusals,
            } => lifecycle_end(&mut connection, operation, &refusals)
                .await
                .map(Done::Admitted),
            WriterJob::BackupHold {
                operation,
                actor,
                snapshot,
            } => backup_hold(&mut connection, &url, operation, &actor, snapshot).await,
            WriterJob::BackupRelease {
                hold,
                operation,
                actor,
            } => {
                let result = sinex_store::backup::release(&mut connection, hold, operation, &actor)
                    .await
                    .map(|hold| Done::Backup(hold, None))
                    .map_err(store_error);
                if result.is_ok() {
                    released_punches = true;
                }
                result
            }
            WriterJob::Stage(write) => stage_write(&mut connection, write)
                .await
                .map(Done::Admitted),
        };
        if matches!(
            &result,
            Err(WriteError::Refused(_) | WriteError::Unavailable(_))
        ) && connection.ping().await.is_err()
            && let Ok(fresh) = PgConnection::connect(&url).await
        {
            // The lock went with the lost connection; without it this
            // process is no longer the writer.
            match claim(fresh).await {
                Ok(fresh) => connection = fresh.with_blobs(store.cloned()),
                Err(error) => {
                    let _ = job.reply.send(Err(WriteError::Refused(error)));
                    return;
                }
            }
        }
        if let Ok(Done::Admitted(outcome)) = &mut result {
            outcome.timings.insert("queue", queue_time);
            outcome.timings.insert("service", service_started.elapsed());
        }
        if let (Some(events), Ok(Done::Admitted(outcome))) = (offered, &result)
            && std::env::var_os("SINEX_BENCH_TIMINGS").is_some()
        {
            let timings: std::collections::BTreeMap<_, _> = outcome
                .timings
                .iter()
                .map(|(name, duration)| (*name, duration.as_secs_f64() * 1000.0))
                .collect();
            eprintln!(
                "{}",
                serde_json::json!({"admission_batch": {
                    "events": events, "admitted": outcome.admitted,
                    "repeats": outcome.repeats, "replaced": outcome.replaced,
                    "older": outcome.older,
                    "superseded": outcome.superseded, "failures": outcome.failures,
                    "timings_ms": timings
                }})
            );
        }
        if result.is_ok() {
            commits.send_modify(|n| *n += 1);
        }
        let _ = job.reply.send(result);
    }
}

async fn admit_checked(
    connection: &mut AdmissionContext,
    batch: &Batch,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
    check: &(dyn Fn() -> Result<(), String> + Send + Sync),
    record_refusals: bool,
) -> Result<Outcome, WriteError> {
    admit_with_checkpoint(
        connection,
        batch,
        keys,
        store,
        check,
        record_refusals,
        Saved::Nothing,
    )
    .await
}

async fn admit_with_checkpoint(
    connection: &mut AdmissionContext,
    batch: &Batch,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
    check: &(dyn Fn() -> Result<(), String> + Send + Sync),
    record_refusals: bool,
    saved: Saved<'_>,
) -> Result<Outcome, WriteError> {
    let stage = match saved {
        Saved::Stage(stage) => Some(stage),
        Saved::Nothing | Saved::Curation => None,
    };
    let curation = matches!(saved, Saved::Curation);
    let unavailable = |e: &dyn std::fmt::Display| WriteError::Unavailable(e.to_string());
    let checks_started = std::time::Instant::now();
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
    let checks_time = checks_started.elapsed();
    let begin_started = std::time::Instant::now();
    let mut tx = connection.begin().await.map_err(store_error)?;
    let begin_time = begin_started.elapsed();
    let since = sinex_store::query::current_seq(&mut tx)
        .await
        .map_err(store_error)?;
    if curation {
        let records: Vec<sinex_model::event_types::library::Curation> = batch
            .events()
            .filter(|event| event.event_type == sinex_library::curation::CURATION)
            .map(|event| serde_json::from_value(event.payload.clone()))
            .collect::<Result<_, _>>()
            .map_err(|error| WriteError::Refused(format!("a curation record: {error}")))?;
        if let Err(reason) = sinex_library::plan::verify(&mut tx, &records).await {
            tx.rollback().await.map_err(store_error)?;
            return Err(WriteError::Refused(format!(
                "the library changed while this request was planned ({reason}); send the request again"
            )));
        }
    }
    match tx.admit(batch, keys).await {
        Ok(mut outcome) => {
            if let Err(error) = check() {
                tx.rollback().await.map_err(store_error)?;
                return Err(WriteError::Refused(error));
            }
            if let Some(stage) = stage
                && let Err(error) = save_stage(tx.connection(), batch, stage, &outcome).await
            {
                tx.rollback().await.map_err(store_error)?;
                return Err(store_error(error));
            }
            if let Err(error) = refresh_read_models(&mut tx, since).await {
                tx.rollback().await.map_err(store_error)?;
                return Err(error);
            }
            let started = std::time::Instant::now();
            tx.commit().await.map_err(store_error)?;
            outcome.timings.insert("commit", started.elapsed());
            outcome.timings.insert("checks", checks_time);
            outcome.timings.insert("begin", begin_time);
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await.map_err(store_error)?;
            let recordable = record_refusals && content_error(&error);
            let error = store_error(error);
            if recordable
                && let WriteError::Refused(reason) = &error
                && let Some(failed) = refused_source(batch, reason)
            {
                check().map_err(WriteError::Refused)?;
                let mut tx = connection.begin().await.map_err(store_error)?;
                let outcome = tx.admit(&failed, keys).await.map_err(store_error)?;
                check().map_err(WriteError::Refused)?;
                if let Some(stage) = stage {
                    save_stage(tx.connection(), &failed, stage, &outcome)
                        .await
                        .map_err(store_error)?;
                }
                tx.commit().await.map_err(store_error)?;
                return Ok(outcome);
            }
            Err(error)
        }
    }
}

/// Bring the read models kept inside the writing transaction up to date
/// with what it changed after commit position `since`: the library's
/// shown values, for the items whose curation records were admitted,
/// archived or restored.
async fn refresh_read_models(tx: &mut PgConnection, since: i64) -> Result<(), WriteError> {
    sinex_library::projection::refresh_since(tx, since)
        .await
        .map(drop)
        .map_err(|error| WriteError::Unavailable(format!("library shown values: {error}")))
}

/// One signal step in one transaction: its outcome and report records, the
/// deliveries it handed off, and the checkpoint that produced them, saved
/// only if no other step committed since it was loaded.
async fn admit_signals(
    connection: &mut AdmissionContext,
    batch: Option<&Batch>,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
    commit: &sinex_store::signals::Commit,
) -> Result<Outcome, WriteError> {
    for name in batch.into_iter().flat_map(Batch::stored_bytes) {
        let present = match store {
            Some(store) => store
                .contains(name)
                .map_err(|e| WriteError::Unavailable(e.to_string()))?,
            None => false,
        };
        if !present {
            return Err(WriteError::Unavailable(format!(
                "the content store does not hold {name}; store the bytes again and resubmit"
            )));
        }
    }
    let mut tx = connection
        .begin()
        .await
        .map_err(|error| store_error(error.into()))?;
    let result = signal_step(&mut tx, batch, keys, commit).await;
    match result {
        Ok(outcome) => {
            tx.commit()
                .await
                .map_err(|error| store_error(error.into()))?;
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback()
                .await
                .map_err(|error| store_error(error.into()))?;
            Err(error)
        }
    }
}

/// The writes of one signal step inside its open transaction. A named
/// function rather than an async block keeps the writer future `Send`.
async fn signal_step(
    tx: &mut sinex_store::AdmissionTransaction<'_>,
    batch: Option<&Batch>,
    keys: &dyn DirtyKeys,
    commit: &sinex_store::signals::Commit,
) -> Result<Outcome, WriteError> {
    let mut outcome = Outcome::default();
    if let Some(expected) = commit.observed_seq
        && sinex_store::query::current_seq(tx)
            .await
            .map_err(store_error)?
            != expected
    {
        return Err(WriteError::Refused(
            "signal evaluation input snapshot changed; evaluate again".into(),
        ));
    }
    if let Some(batch) = batch {
        outcome += tx.admit(batch, keys).await.map_err(store_error)?;
    }
    if let Some(handed) = &commit.deliveries {
        for (event, read) in &handed.events {
            let delivery = Batch {
                run: handed.run,
                kind: sinex_model::BatchKind::Derived {
                    derivation: crate::signals::DELIVERIES.into(),
                    scope: sinex_store::DerivedScope::Keys {
                        keys: vec![sinex_store::ComputedKey {
                            key: event.payload["delivery_id"].clone(),
                            dirty_seq: commit.observed_seq.unwrap_or(0),
                            waiting: false,
                            refresh_at: None,
                        }],
                        read: *read,
                    }
                    .to_value(),
                },
                records: vec![sinex_model::RecordOutcome::Events {
                    location: None,
                    events: vec![event.clone()],
                }],
                archive: vec![],
            };
            let admitted = tx.admit(&delivery, keys).await.map_err(store_error)?;
            if admitted.superseded_input != 0 {
                return Err(WriteError::Refused(
                    "signal delivery inputs stopped being available".into(),
                ));
            }
            outcome += admitted;
        }
    }
    let mut state = commit.checkpoint.clone();
    if commit.observed_seq.is_some() {
        state.observed_seq = sinex_store::query::current_seq(tx)
            .await
            .map_err(store_error)?;
    }
    sinex_store::signals::save(tx, &commit.host, commit.expected_revision, &state)
        .await
        .map_err(store_error)?;
    Ok(outcome)
}

fn content_error(error: &sinex_store::Error) -> bool {
    match error {
        sinex_store::Error::Invalid(_)
        | sinex_store::Error::Schema(sinex_schema::Error::Declaration(_)) => true,
        sinex_store::Error::Database(error) => error
            .as_database_error()
            .and_then(|error| error.code())
            .is_some_and(|code| {
                code.starts_with("22") || code.starts_with("23") || code.starts_with("54")
            }),
        _ => false,
    }
}

fn refused_source(batch: &Batch, reason: &str) -> Option<Batch> {
    if !matches!(batch.kind, sinex_model::BatchKind::Source(_))
        || batch.events().any(|event| !event.owned_blobs.is_empty())
    {
        // Separately owned files need a material reference before their events can be discarded.
        return None;
    }
    let mut failed = batch.clone();
    for record in &mut failed.records {
        if let sinex_model::RecordOutcome::Events { location, .. } = record {
            *record = sinex_model::RecordOutcome::Failure {
                location: (*location)?,
                reason: format!("content refused: {reason}"),
            };
        }
    }
    Some(failed)
}

/// A transaction that could not run must remain pending at the producer.
/// Only invalid content can be recorded as a final refusal.
fn admission_error(error: sinex_store::Error) -> WriteError {
    let temporary = match &error {
        sinex_store::Error::Database(sqlx::Error::Database(error)) => {
            error.code().is_some_and(|code| {
                ["08", "40", "53", "57", "58"]
                    .iter()
                    .any(|class| code.starts_with(class))
            })
        }
        sinex_store::Error::Database(error) => matches!(
            error,
            sqlx::Error::Io(_)
                | sqlx::Error::Tls(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed
        ),
        sinex_store::Error::Blob(_) => true,
        _ => false,
    };
    if temporary {
        WriteError::Unavailable(error.to_string())
    } else if let sinex_store::Error::Database(database) = &error {
        match database.as_database_error().and_then(|error| error.code()) {
            Some(code) => WriteError::Refused(format!("database refused invalid data ({code})")),
            None => WriteError::Refused(error.to_string()),
        }
    } else {
        WriteError::Refused(error.to_string())
    }
}

async fn replay_sequence(
    connection: &mut AdmissionContext,
    sequence: &mut ReplaySequence,
    keys: &dyn DirtyKeys,
) -> Result<(Vec<Outcome>, Diff), WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    if !sequence.compare
        && let Some(ingress) = &sequence.ingress
    {
        let mut scope_tx = connection.begin().await.map_err(store_error)?;
        if let Some(recorded) = crate::ingress::replay::recorded(&mut scope_tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?
        {
            scope_tx.rollback().await.map_err(store_error)?;
            return Ok((vec![recorded], Diff::default()));
        }
        crate::ingress::replay::check_scope(&mut scope_tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?;
        // A refused replacement keeps its authorised scope, so another parse
        // cannot reuse that operation. Event changes still commit together.
        replay::begin(
            &mut scope_tx,
            sequence.operation,
            &sequence.actor,
            &sequence.scope,
        )
        .await
        .map_err(store_error)?;
        scope_tx.commit().await.map_err(store_error)?;
    }
    let mut tx = connection.begin().await.map_err(store_error)?;
    let since = sinex_store::query::current_seq(&mut tx)
        .await
        .map_err(store_error)?;
    if let Some(ingress) = &sequence.ingress {
        if let Some(recorded) = crate::ingress::replay::recorded(&mut tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?
        {
            tx.rollback().await.map_err(store_error)?;
            return Ok((vec![recorded], Diff::default()));
        }
        crate::ingress::replay::check(&mut tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?;
    }
    let receipt_number = if sequence.ingress.is_some() {
        Some(
            sinex_store::commit_log::reserve_frame_number(&mut tx)
                .await
                .map_err(store_error)?,
        )
    } else {
        None
    };
    replay::begin(
        &mut tx,
        sequence.operation,
        &sequence.actor,
        &sequence.scope,
    )
    .await
    .map_err(store_error)?;
    let mut outcomes = Vec::new();
    while let Some(batch) = sequence.batches.recv().await {
        let mut batch = batch.map_err(|e| refuse(&e))?;
        if let sinex_model::BatchKind::Ingress(frame) = &mut batch.kind
            && frame.frame.is_empty()
        {
            frame.receipt_number = receipt_number;
        }
        outcomes.push(tx.admit(&batch, keys).await.map_err(store_error)?);
    }
    let admitted: Vec<uuid::Uuid> = outcomes
        .iter()
        .flat_map(|o| o.admitted_ids.iter().map(|id| id.0))
        .collect();
    let diff = replay::diff(&mut tx, sequence.operation, &admitted)
        .await
        .map_err(store_error)?;
    if let Some(ingress) = &sequence.ingress {
        crate::ingress::replay::finish(
            &mut tx,
            ingress,
            &outcomes,
            receipt_number.expect("producer replay reserved a receipt"),
        )
        .await
        .map_err(crate::ingress::Check::write_error)?;
    }
    if sequence.compare {
        tx.rollback().await.map_err(store_error)?;
    } else {
        replay::sequence_done(&mut tx, sequence.operation, sequence.root)
            .await
            .map_err(store_error)?;
        refresh_read_models(&mut tx, since).await?;
        tx.commit().await.map_err(store_error)?;
    }
    Ok((outcomes, diff))
}

async fn record_replay(
    connection: &mut PgConnection,
    operation: OperationId,
    actor: &str,
    scope: &Value,
    refusals: &[Value],
) -> Result<Outcome, WriteError> {
    let mut tx = connection
        .begin()
        .await
        .map_err(|error| store_error(error.into()))?;
    replay::begin(&mut tx, operation, actor, scope)
        .await
        .map_err(store_error)?;
    replay::record_refusals(&mut tx, operation, refusals)
        .await
        .map_err(store_error)?;
    tx.commit()
        .await
        .map_err(|error| store_error(error.into()))?;
    Ok(Outcome::default())
}

/// Save a staged binding's accounting, with admission's decisions for the
/// batch. The cursor stays in `raw.positions`, saved by the same transaction.
async fn save_stage(
    conn: &mut PgConnection,
    batch: &Batch,
    stage: &StageCheckpoint,
    outcome: &Outcome,
) -> Result<(), sinex_store::Error> {
    let sinex_model::BatchKind::Source(progress) = &batch.kind else {
        return Err(sinex_store::Error::Invalid(
            "a stage checkpoint comes with a source batch".into(),
        ));
    };
    let mut accounting = stage.accounting.clone();
    crate::stage::decided(&mut accounting, outcome);
    sinex_store::stages::checkpoint(
        conn,
        stage.operation,
        &progress.binding,
        &sinex_model::stage::Checkpoint { accounting },
    )
    .await
}

async fn stage_write(
    connection: &mut AdmissionContext,
    write: StageWrite,
) -> Result<Outcome, WriteError> {
    use sinex_store::stages;
    let mut tx = connection.begin().await.map_err(store_error)?;
    let written = match &write {
        StageWrite::Unparsed(unparsed) => tx.register_unparsed(unparsed).await,
        StageWrite::Begin {
            operation,
            actor,
            scope,
            inventory,
        } => stages::begin(tx.connection(), *operation, actor, scope, inventory).await,
        StageWrite::Progress {
            operation,
            inventory,
        } => stages::progress(tx.connection(), *operation, inventory).await,
        StageWrite::Reopen { operation } => stages::reopen(tx.connection(), *operation).await,
        StageWrite::End {
            operation,
            result,
            counts,
            inventory,
            refusals,
        } => {
            stages::end(
                tx.connection(),
                *operation,
                *result,
                counts,
                inventory,
                refusals,
            )
            .await
        }
    };
    match written {
        Ok(()) => tx.commit().await.map_err(store_error)?,
        Err(error) => {
            tx.rollback().await.map_err(store_error)?;
            return Err(store_error(error));
        }
    }
    Ok(Outcome::default())
}

async fn lifecycle_step(
    connection: &mut AdmissionContext,
    operation: &lifecycle::Operation,
    progress: Progress,
    roots: i64,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
) -> Result<Step, WriteError> {
    let mut tx = connection.begin().await.map_err(store_error)?;
    let since = sinex_store::query::current_seq(&mut tx)
        .await
        .map_err(store_error)?;
    let step = match lifecycle::step(&mut tx, operation, progress, roots, keys).await {
        Ok(step) => step,
        Err(error) => {
            tx.rollback().await.map_err(store_error)?;
            return Err(store_error(error));
        }
    };
    match &step {
        Step::Refused(_) => tx.rollback().await.map_err(store_error)?,
        Step::Moved { remove, .. } => {
            if let Err(error) = refresh_read_models(&mut tx, since).await {
                tx.rollback().await.map_err(store_error)?;
                return Err(error);
            }
            tx.commit().await.map_err(store_error)?;
            // Bytes left marked (no store here, or a failed punch) are
            // removed at the writer's next start.
            if !remove.is_empty()
                && let Some(store) = store
            {
                lifecycle::remove_unreferenced(connection, store)
                    .await
                    .map_err(store_error)?;
            }
        }
    }
    Ok(step)
}

async fn lifecycle_end(
    connection: &mut PgConnection,
    operation: OperationId,
    refusals: &[Refusal],
) -> Result<Outcome, WriteError> {
    let mut tx = connection
        .begin()
        .await
        .map_err(|error| store_error(error.into()))?;
    lifecycle::end(
        &mut tx,
        operation,
        sinex_model::lifecycle::OperationResult::StoppedEarly,
        refusals,
    )
    .await
    .map_err(store_error)?;
    tx.commit()
        .await
        .map_err(|error| store_error(error.into()))?;
    Ok(Outcome::default())
}

#[cfg(test)]
mod tests;

// A failed punch remains marked in PostgreSQL and ordinary reopen retries it.
fn tracing_punch_error(error: &sinex_store::Error) {
    eprintln!("deferred backup punch: {error}");
}

async fn backup_hold(
    connection: &mut AdmissionContext,
    url: &str,
    operation: OperationId,
    actor: &str,
    snapshot: bool,
) -> Result<Done, WriteError> {
    let (hold, created) = sinex_store::backup::hold(connection, operation, actor)
        .await
        .map_err(store_error)?;
    let snapshot = if snapshot {
        if !created {
            return Err(WriteError::Refused(
                "a fresh exported snapshot needs a fresh backup operation".into(),
            ));
        }
        // This is still one writer job. No admission can advance the cut until
        // pg_export_snapshot has taken its first consistent read after the hold commit.
        let exporter = PgConnection::connect(url)
            .await
            .map_err(|error| WriteError::Unavailable(error.to_string()))?;
        Some(Box::new(
            sinex_store::backup::Snapshot::export(exporter, hold.clone(), url.to_owned())
                .await
                .map_err(store_error)?,
        ))
    } else {
        None
    };
    Ok(Done::Backup(hold, snapshot))
}
