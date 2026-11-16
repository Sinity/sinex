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
use crate::{Error, Result};

use serde_json::Value;
use sinex_model::{Batch, OperationId};
use sinex_store::lifecycle::{self, Progress, Refusal, Step};
use sinex_store::replay::{self, Diff};
use sinex_store::{AdmissionContext, Blobs, DirtyKeys, Outcome};
use sqlx::{Connection, PgConnection};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    #[error("the request read deadline passed before writer admission")]
    ReadDeadline,
    #[error("admission refused the batch: {0}")]
    Refused(String),
    #[error("database unavailable: {0}")]
    Unavailable(String),
    #[error("the writer has stopped")]
    Stopped,
    /// The inputs this write was computed from changed before it committed;
    /// the caller computes it again from the committed state.
    #[error("{0}; evaluate again")]
    Stale(&'static str),
}

impl From<WriteError> for command_surface::Failure {
    fn from(error: WriteError) -> Self {
        use command_surface::{Failure, FailureKind};
        let message = error.to_string();
        match error {
            WriteError::ReadDeadline => crate::request_deadline::timeout().into(),
            WriteError::Refused(_) => Failure::new(FailureKind::BadValue, message),
            WriteError::Unavailable(_) => {
                Failure::new(FailureKind::NotRunning, message).with_code("database_unavailable")
            }
            WriteError::Stopped => {
                Failure::new(FailureKind::NotRunning, message).with_code("writer_stopped")
            }
            WriteError::Stale(_) => {
                let mut failure = Failure::new(FailureKind::Conflict, message).with_code("stale");
                failure.retryable = true;
                failure
            }
        }
    }
}

/// One completely prepared replay sequence, applied in one transaction.
pub(crate) struct ReplaySequence {
    pub operation: OperationId,
    pub actor: String,
    pub scope: Value,
    /// The sequence's first material, journaled when the sequence commits.
    pub root: uuid::Uuid,
    /// Roll back after reading the difference: a comparison.
    pub compare: bool,
    pub batches: crate::replay::prepared::Batches,
    pub ingress: Option<crate::ingress::replay::CheckedReplay>,
    pub check: AdmissionCheck,
}

type Reply<T> = oneshot::Sender<Result<T, WriteError>>;

enum WriterJob {
    Admit(Batch, Reply<Outcome>),
    Signals(
        Option<Batch>,
        Box<sinex_store::signals::Commit>,
        Reply<Outcome>,
    ),
    Checked(Batch, AdmissionCheck, Reply<Outcome>),
    Curation(Batch, AdmissionCheck, Reply<Outcome>),
    Source(
        Batch,
        AdmissionCheck,
        Option<StageCheckpoint>,
        Reply<Outcome>,
    ),
    Replay(Box<ReplaySequence>, Reply<(Vec<Outcome>, Diff)>),
    Lifecycle {
        operation: Box<lifecycle::Operation>,
        progress: Progress,
        roots: lifecycle::Roots,
        reply: Reply<LifecycleStep>,
    },
    LifecycleEnd {
        operation: OperationId,
        refusals: Vec<Refusal>,
        reply: Reply<()>,
    },
    RecordReplay {
        operation: OperationId,
        actor: String,
        scope: Value,
        refusals: Vec<Value>,
        reply: Reply<()>,
    },
    Stage(StageWrite, Reply<()>),
    BackupHold {
        operation: OperationId,
        actor: String,
        reply: Reply<sinex_model::backup::Hold>,
    },
    BackupSnapshot {
        operation: OperationId,
        actor: String,
        reply: Reply<sinex_store::backup::Snapshot>,
    },
    BackupRelease {
        hold: OperationId,
        operation: OperationId,
        actor: String,
        reply: Reply<sinex_model::backup::Hold>,
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

pub(crate) struct LifecycleStep {
    pub step: Step,
    /// Time processing on the writer, excluding queue and source reads.
    pub held: Duration,
}

struct Job {
    queued_at: std::time::Instant,
    work: WriterJob,
}

/// Held while signal decisions are planned and committed.
pub(crate) struct SignalTurn(tokio::sync::OwnedMutexGuard<()>);

/// Held while library curation is planned and committed.
pub(crate) struct CurationTurn(tokio::sync::OwnedMutexGuard<()>);

/// Where producers hand batches to the writer.
#[derive(Clone)]
pub(crate) struct WriterHandle {
    source_publication: sinex_transport::backup::Publication,
    derivations: Arc<tokio::sync::Mutex<()>>,
    signals: Arc<tokio::sync::Mutex<()>>,
    curation: Arc<tokio::sync::Mutex<()>>,
    live: mpsc::Sender<Job>,
    derived: mpsc::Sender<Job>,
    bulk: mpsc::Sender<Job>,
}

/// A check run just before and after the admission (cancellation, unlock
/// expiry); an error refuses the batch.
pub(crate) type AdmissionCheck = Arc<dyn Fn() -> Result<()> + Send + Sync>;

impl WriterHandle {
    pub(crate) fn source_publication(&self) -> sinex_transport::backup::Publication {
        self.source_publication.clone()
    }

    pub(crate) async fn lock_signals(&self) -> SignalTurn {
        SignalTurn(self.signals.clone().lock_owned().await)
    }

    /// Held while a library curation request is planned and admitted, so
    /// requests from this daemon never plan against each other's snapshot.
    pub(crate) async fn lock_curation(&self) -> CurationTurn {
        CurationTurn(self.curation.clone().lock_owned().await)
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
        self.ask(priority, |reply| WriterJob::Checked(batch, check, reply))
            .await
    }

    /// The turn spans planning and admission of the curation records.
    pub(crate) async fn submit_curation(
        &self,
        turn: &CurationTurn,
        priority: Priority,
        batch: Batch,
        check: AdmissionCheck,
    ) -> Result<Outcome, WriteError> {
        let _ = &turn.0;
        self.ask(priority, |reply| WriterJob::Curation(batch, check, reply))
            .await
    }

    /// A source advances past refused content, retaining its bytes and failures.
    pub(crate) async fn submit_source(
        &self,
        priority: Priority,
        batch: Batch,
        check: AdmissionCheck,
        stage: Option<StageCheckpoint>,
    ) -> Result<Outcome, WriteError> {
        // Backup pauses publication before asking this writer for its snapshot.
        let _publication = self.source_publication.enter().await;
        self.ask(priority, |reply| {
            WriterJob::Source(batch, check, stage, reply)
        })
        .await
    }

    /// Signal records and their checkpoint commit together, under the planning turn.
    pub(crate) async fn submit_signals(
        &self,
        turn: &SignalTurn,
        batch: Option<Batch>,
        checkpoint: sinex_store::signals::Commit,
    ) -> Result<Outcome, WriteError> {
        let _ = &turn.0;
        self.ask(Priority::Live, |reply| {
            WriterJob::Signals(batch, Box::new(checkpoint), reply)
        })
        .await
    }

    /// Submit a batch and wait until it is committed (or refused).
    pub(crate) async fn submit(
        &self,
        priority: Priority,
        batch: Batch,
    ) -> Result<Outcome, WriteError> {
        self.ask(priority, |reply| WriterJob::Admit(batch, reply))
            .await
    }

    /// Replay one sequence in one transaction; comparisons keep no changes.
    pub(crate) async fn replay(
        &self,
        mut sequence: ReplaySequence,
    ) -> Result<(Vec<Outcome>, Diff), WriteError> {
        let (caller, pending) = oneshot::channel::<()>();
        let check = sequence.check;
        sequence.check = Arc::new(move || {
            if caller.is_closed() {
                return Err(Error::refused("replay was cancelled before commit"));
            }
            check()
        });
        let result = self
            .ask(Priority::Derived, |reply| {
                WriterJob::Replay(Box::new(sequence), reply)
            })
            .await;
        drop(pending);
        result
    }

    pub(crate) async fn record_replay(
        &self,
        operation: OperationId,
        actor: String,
        scope: Value,
        refusals: Vec<Value>,
    ) -> Result<(), WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::RecordReplay {
            operation,
            actor,
            scope,
            refusals,
            reply,
        })
        .await
    }

    /// Write a stage's log entry or unparsed file in its own transaction.
    pub(crate) async fn stage(&self, write: StageWrite) -> Result<(), WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::Stage(write, reply))
            .await
    }

    pub(crate) async fn backup_hold(
        &self,
        actor: &str,
    ) -> Result<sinex_model::backup::Hold, WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::BackupHold {
            operation: OperationId::new(),
            actor: actor.into(),
            reply,
        })
        .await
    }

    pub(crate) async fn backup_snapshot(
        &self,
        actor: &str,
    ) -> Result<sinex_store::backup::Snapshot, WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::BackupSnapshot {
            operation: OperationId::new(),
            actor: actor.into(),
            reply,
        })
        .await
    }

    pub(crate) async fn backup_release(
        &self,
        hold: OperationId,
        actor: &str,
    ) -> Result<sinex_model::backup::Hold, WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::BackupRelease {
            hold,
            operation: OperationId::new(),
            actor: actor.into(),
            reply,
        })
        .await
    }

    pub(crate) async fn lifecycle_step(
        &self,
        operation: &lifecycle::Operation,
        progress: Progress,
        roots: lifecycle::Roots,
    ) -> Result<LifecycleStep, WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::Lifecycle {
            operation: Box::new(operation.clone()),
            progress,
            roots,
            reply,
        })
        .await
    }

    /// End an operation stopped early, recording what refused it.
    pub(crate) async fn lifecycle_stopped(
        &self,
        operation: OperationId,
        refusals: Vec<Refusal>,
    ) -> Result<(), WriteError> {
        self.ask(Priority::Derived, |reply| WriterJob::LifecycleEnd {
            operation,
            refusals,
            reply,
        })
        .await
    }

    async fn ask<R>(
        &self,
        priority: Priority,
        make: impl FnOnce(Reply<R>) -> WriterJob,
    ) -> Result<R, WriteError> {
        let (reply, answer) = oneshot::channel();
        let channel = match priority {
            Priority::Live => &self.live,
            Priority::Derived => &self.derived,
            Priority::Bulk => &self.bulk,
        };
        let queued_at = std::time::Instant::now();
        let permit = channel.reserve().await.map_err(|_| WriteError::Stopped)?;
        crate::request_deadline::admit().map_err(|_| WriteError::ReadDeadline)?;
        permit.send(Job {
            queued_at,
            work: make(reply),
        });
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
            source_publication: Default::default(),
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
pub(crate) async fn claim(mut connection: PgConnection) -> Result<AdmissionContext> {
    sinex_store::query::name_connection(&mut connection, "sinex.writer").await?;
    let mut context = AdmissionContext::claim(connection).await?;
    sinex_schema::kinds::require_bundled(&mut context).await?;
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
        // A release leaves deferred extent punching to do. The next writer group
        // attempts it; no reopen, timer, or second task is needed.
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
        let service_started = std::time::Instant::now();
        macro_rules! answer {
            ($reply:expr, $result:expr) => {{
                let result = $result;
                if !answer(&mut connection, &url, store, &commits, $reply, result).await {
                    return;
                }
            }};
        }
        macro_rules! batch_answer {
            ($reply:expr, $events:expr, $result:expr) => {{
                let result = $result;
                let result = batch_timings(result, $events, queue_time, service_started.elapsed());
                answer!($reply, result);
            }};
        }
        match job.work {
            WriterJob::Admit(batch, reply) => {
                batch_answer!(
                    reply,
                    Some(batch.event_count()),
                    admit_checked(
                        &mut connection,
                        &batch,
                        keys.as_ref(),
                        store,
                        &|| Ok(()),
                        false
                    )
                    .await
                );
            }
            WriterJob::Signals(batch, checkpoint, reply) => {
                batch_answer!(
                    reply,
                    batch.as_ref().map(Batch::event_count),
                    admit_signals(
                        &mut connection,
                        batch.as_ref(),
                        keys.as_ref(),
                        store,
                        &checkpoint
                    )
                    .await
                );
            }
            WriterJob::Curation(batch, check, reply) => {
                batch_answer!(
                    reply,
                    Some(batch.event_count()),
                    admit_with_checkpoint(
                        &mut connection,
                        &batch,
                        keys.as_ref(),
                        store,
                        &*check,
                        false,
                        Saved::Curation
                    )
                    .await
                );
            }
            WriterJob::Checked(batch, check, reply) => {
                batch_answer!(
                    reply,
                    Some(batch.event_count()),
                    admit_checked(
                        &mut connection,
                        &batch,
                        keys.as_ref(),
                        store,
                        &*check,
                        false
                    )
                    .await
                );
            }
            WriterJob::Source(batch, check, stage, reply) => {
                batch_answer!(
                    reply,
                    Some(batch.event_count()),
                    admit_with_checkpoint(
                        &mut connection,
                        &batch,
                        keys.as_ref(),
                        store,
                        &*check,
                        true,
                        stage.as_ref().map_or(Saved::Nothing, Saved::Stage)
                    )
                    .await
                );
            }
            WriterJob::Replay(mut sequence, reply) => {
                answer!(
                    reply,
                    replay_sequence(&mut connection, &mut sequence, keys.as_ref()).await
                );
            }
            WriterJob::RecordReplay {
                operation,
                actor,
                scope,
                refusals,
                reply,
            } => {
                answer!(
                    reply,
                    record_replay(&mut connection, operation, &actor, &scope, &refusals)
                        .await
                        .map(drop)
                );
            }
            WriterJob::Lifecycle {
                operation,
                progress,
                roots,
                reply,
            } => {
                answer!(
                    reply,
                    lifecycle_step(
                        &mut connection,
                        &operation,
                        progress,
                        roots,
                        keys.as_ref(),
                        store
                    )
                    .await
                );
            }
            WriterJob::LifecycleEnd {
                operation,
                refusals,
                reply,
            } => {
                answer!(
                    reply,
                    lifecycle_end(&mut connection, operation, &refusals)
                        .await
                        .map(drop)
                );
            }
            WriterJob::BackupHold {
                operation,
                actor,
                reply,
            } => {
                answer!(
                    reply,
                    sinex_store::backup::hold(&mut connection, operation, &actor)
                        .await
                        .map(|(hold, _)| hold)
                        .map_err(WriteError::from)
                );
            }
            WriterJob::BackupSnapshot {
                operation,
                actor,
                reply,
            } => {
                answer!(
                    reply,
                    backup_snapshot(&mut connection, &url, operation, &actor).await
                );
            }
            WriterJob::BackupRelease {
                hold,
                operation,
                actor,
                reply,
            } => {
                let result = sinex_store::backup::release(&mut connection, hold, operation, &actor)
                    .await
                    .map_err(WriteError::from);
                if result.is_ok() {
                    released_punches = true;
                }
                answer!(reply, result);
            }
            WriterJob::Stage(write, reply) => {
                answer!(reply, stage_write(&mut connection, write).await.map(drop));
            }
        }
    }
}

/// Reconnect a lost writer before releasing the caller's typed answer.
async fn answer<R>(
    connection: &mut AdmissionContext,
    url: &str,
    store: Option<&Blobs>,
    commits: &watch::Sender<u64>,
    reply: Reply<R>,
    result: Result<R, WriteError>,
) -> bool {
    if matches!(
        &result,
        Err(WriteError::Refused(_) | WriteError::Unavailable(_))
    ) && connection.ping().await.is_err()
        && let Ok(fresh) = PgConnection::connect(url).await
    {
        match claim(fresh).await {
            Ok(fresh) => *connection = fresh.with_blobs(store.cloned()),
            Err(error) => {
                let _ = reply.send(Err(WriteError::Refused(error.to_string())));
                return false;
            }
        }
    }
    if result.is_ok() {
        commits.send_modify(|n| *n += 1);
    }
    let _ = reply.send(result);
    true
}

fn batch_timings(
    mut result: Result<Outcome, WriteError>,
    events: Option<usize>,
    queue: Duration,
    service: Duration,
) -> Result<Outcome, WriteError> {
    if let Ok(outcome) = &mut result {
        outcome.timings.insert("queue", queue);
        outcome.timings.insert("service", service);
        if let Some(events) = events
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
                    "older": outcome.older, "superseded": outcome.superseded,
                    "failures": outcome.failures, "timings_ms": timings
                }})
            );
        }
    }
    result
}

async fn admit_checked(
    connection: &mut AdmissionContext,
    batch: &Batch,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
    check: &(dyn Fn() -> Result<()> + Send + Sync),
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
    check: &(dyn Fn() -> Result<()> + Send + Sync),
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
    check().map_err(|error| WriteError::Refused(error.to_string()))?;
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
    let mut tx = connection.begin().await?;
    let begin_time = begin_started.elapsed();
    let since = sinex_store::query::current_seq(&mut tx).await?;
    if curation {
        let records: Vec<sinex_model::event_types::library::Curation> = batch
            .events()
            .filter(|event| event.event_type == sinex_library::curation::CURATION)
            .map(|event| serde_json::from_value(event.payload.clone()))
            .collect::<Result<_, _>>()
            .map_err(|error| WriteError::Refused(format!("a curation record: {error}")))?;
        if let Err(reason) = sinex_library::plan::verify(&mut tx, &records).await {
            tx.rollback().await?;
            return Err(WriteError::Refused(format!(
                "the library changed while this request was planned ({reason}); send the request again"
            )));
        }
    }
    match tx.admit(batch, keys).await {
        Ok(mut outcome) => {
            if let Err(error) = check() {
                tx.rollback().await?;
                return Err(WriteError::Refused(error.to_string()));
            }
            if let Some(stage) = stage
                && let Err(error) = save_stage(tx.connection(), batch, stage, &outcome).await
            {
                tx.rollback().await?;
                return Err(WriteError::from(error));
            }
            if let Err(error) = refresh_read_models(&mut tx, since).await {
                tx.rollback().await?;
                return Err(error);
            }
            let started = std::time::Instant::now();
            tx.commit().await?;
            outcome.timings.insert("commit", started.elapsed());
            outcome.timings.insert("checks", checks_time);
            outcome.timings.insert("begin", begin_time);
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await?;
            let recordable = record_refusals && content_error(&error);
            let error = WriteError::from(error);
            if recordable
                && let WriteError::Refused(reason) = &error
                && let Some(failed) = refused_source(batch, reason)
            {
                check().map_err(|error| WriteError::Refused(error.to_string()))?;
                let mut tx = connection.begin().await?;
                let outcome = tx.admit(&failed, keys).await?;
                check().map_err(|error| WriteError::Refused(error.to_string()))?;
                if let Some(stage) = stage {
                    save_stage(tx.connection(), &failed, stage, &outcome).await?;
                }
                tx.commit().await?;
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
    sinex_library::shown_view::refresh_since(tx, since)
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
    let mut tx = connection.begin().await?;
    let result = signal_step(&mut tx, batch, keys, commit).await;
    match result {
        Ok(outcome) => {
            tx.commit().await?;
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await?;
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
        && sinex_store::query::current_seq(tx).await? != expected
    {
        return Err(WriteError::Stale(
            "signal evaluation input snapshot changed",
        ));
    }
    if let Some(batch) = batch {
        outcome += tx.admit(batch, keys).await?;
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
            let admitted = tx.admit(&delivery, keys).await?;
            if admitted.superseded_input != 0 {
                return Err(WriteError::Stale(
                    "signal delivery inputs stopped being available",
                ));
            }
            outcome += admitted;
        }
    }
    let mut state = commit.checkpoint.clone();
    if commit.observed_seq.is_some() {
        state.observed_seq = sinex_store::query::current_seq(tx).await?;
    }
    sinex_store::signals::save(tx, &commit.host, commit.expected_revision, &state).await?;
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
impl From<sqlx::Error> for WriteError {
    fn from(error: sqlx::Error) -> Self {
        sinex_store::Error::from(error).into()
    }
}

impl From<sinex_store::Error> for WriteError {
    fn from(error: sinex_store::Error) -> Self {
        if let sinex_store::Error::StaleCheckpoint = error {
            return WriteError::Stale("signal checkpoint changed");
        }
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
                Some(code) => {
                    WriteError::Refused(format!("database refused invalid data ({code})"))
                }
                None => WriteError::Refused(error.to_string()),
            }
        } else {
            WriteError::Refused(error.to_string())
        }
    }
}

async fn replay_sequence(
    connection: &mut AdmissionContext,
    sequence: &mut ReplaySequence,
    keys: &dyn DirtyKeys,
) -> Result<(Vec<Outcome>, Diff), WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    (sequence.check)().map_err(|e| refuse(&e))?;
    if !sequence.compare
        && let Some(ingress) = &sequence.ingress
    {
        let mut scope_tx = connection.begin().await?;
        if let Some(recorded) = crate::ingress::replay::recorded(&mut scope_tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?
        {
            scope_tx.rollback().await?;
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
        .await?;
        scope_tx.commit().await?;
    }
    let mut tx = connection.begin().await?;
    let since = sinex_store::query::current_seq(&mut tx).await?;
    if let Some(ingress) = &sequence.ingress {
        if let Some(recorded) = crate::ingress::replay::recorded(&mut tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?
        {
            tx.rollback().await?;
            return Ok((vec![recorded], Diff::default()));
        }
        crate::ingress::replay::check(&mut tx, ingress)
            .await
            .map_err(crate::ingress::Check::write_error)?;
    }
    let ack_number = if sequence.ingress.is_some() {
        Some(sinex_store::commit_log::reserve_frame_number(&mut tx).await?)
    } else {
        None
    };
    replay::begin(
        &mut tx,
        sequence.operation,
        &sequence.actor,
        &sequence.scope,
    )
    .await?;
    let mut outcomes = Vec::new();
    while let Some(batch) = sequence.batches.next() {
        (sequence.check)().map_err(|e| refuse(&e))?;
        let mut batch = batch.map_err(|e| refuse(&e))?;
        if let sinex_model::BatchKind::Ingress(frame) = &mut batch.kind
            && frame.frame.is_empty()
        {
            frame.ack_number = ack_number;
        }
        outcomes.push(tx.admit(&batch, keys).await?);
    }
    let admitted: Vec<uuid::Uuid> = outcomes
        .iter()
        .flat_map(|o| o.admitted_ids.iter().map(|id| id.0))
        .collect();
    let diff = replay::diff(&mut tx, sequence.operation, &admitted).await?;
    if let Some(ingress) = &sequence.ingress {
        crate::ingress::replay::finish(
            &mut tx,
            ingress,
            &outcomes,
            ack_number.expect("producer replay reserved an ack number"),
        )
        .await
        .map_err(crate::ingress::Check::write_error)?;
    }
    if sequence.compare {
        tx.rollback().await?;
    } else {
        (sequence.check)().map_err(|e| refuse(&e))?;
        replay::sequence_done(&mut tx, sequence.operation, sequence.root).await?;
        refresh_read_models(&mut tx, since).await?;
        (sequence.check)().map_err(|e| refuse(&e))?;
        tx.commit().await?;
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
    let mut tx = connection.begin().await?;
    replay::begin(&mut tx, operation, actor, scope).await?;
    replay::record_refusals(&mut tx, operation, refusals).await?;
    tx.commit().await?;
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
    let mut tx = connection.begin().await?;
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
        Ok(()) => tx.commit().await?,
        Err(error) => {
            tx.rollback().await?;
            return Err(WriteError::from(error));
        }
    }
    Ok(Outcome::default())
}

async fn lifecycle_step(
    connection: &mut AdmissionContext,
    operation: &lifecycle::Operation,
    progress: Progress,
    roots: lifecycle::Roots,
    keys: &dyn DirtyKeys,
    store: Option<&Blobs>,
) -> Result<LifecycleStep, WriteError> {
    let started = Instant::now();
    let mut tx = connection.begin().await?;
    let since = sinex_store::query::current_seq(&mut tx).await?;
    let step = match lifecycle::step(&mut tx, operation, progress, roots, keys).await {
        Ok(step) => step,
        Err(error) => {
            tx.rollback().await?;
            return Err(WriteError::from(error));
        }
    };
    match &step {
        Step::Refused(_) => tx.rollback().await?,
        Step::Moved { remove, .. } => {
            if let Err(error) = refresh_read_models(&mut tx, since).await {
                tx.rollback().await?;
                return Err(error);
            }
            tx.commit().await?;
            // Bytes left marked (no store here, or a failed punch) are
            // removed at the writer's next start.
            if !remove.is_empty()
                && let Some(store) = store
            {
                lifecycle::remove_unreferenced(connection, store).await?;
            }
        }
    }
    Ok(LifecycleStep {
        step,
        held: started.elapsed(),
    })
}

async fn lifecycle_end(
    connection: &mut PgConnection,
    operation: OperationId,
    refusals: &[Refusal],
) -> Result<Outcome, WriteError> {
    let mut tx = connection.begin().await?;
    lifecycle::end(
        &mut tx,
        operation,
        sinex_model::lifecycle::OperationResult::StoppedEarly,
        refusals,
    )
    .await?;
    tx.commit().await?;
    Ok(Outcome::default())
}

#[cfg(test)]
mod tests;

// A failed punch stays marked in PostgreSQL and the next reopen retries it.
fn tracing_punch_error(error: &sinex_store::Error) {
    eprintln!("deferred backup punch: {error}");
}

async fn backup_snapshot(
    connection: &mut AdmissionContext,
    url: &str,
    operation: OperationId,
    actor: &str,
) -> Result<sinex_store::backup::Snapshot, WriteError> {
    let (hold, created) = sinex_store::backup::hold(connection, operation, actor).await?;
    if !created {
        return Err(WriteError::Refused(
            "a fresh exported snapshot needs a fresh backup operation".into(),
        ));
    }
    // No queued write advances the boundary before the exporting transaction opens.
    let exporter = PgConnection::connect(url)
        .await
        .map_err(|error| WriteError::Unavailable(error.to_string()))?;
    Ok(sinex_store::backup::Snapshot::export(exporter, hold, url.to_owned()).await?)
}
