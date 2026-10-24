//! The writer task: the only database writer (architecture §4.3, §7.4). It
//! takes batches from three channels in priority order, live before derived
//! before bulk, so a backfill never starves live capture, and runs admission
//! for each batch in its own transaction.
//!
//! Besides single batches it runs two kinds of work that need one
//! transaction around more than one admission: a replay chain (archive what
//! is re-read, admit the new readings; in compare mode, read the difference
//! and roll back), and an excision (rewrite bytes, remove readings; delete the
//! freed blob files after commit).
use serde_json::Value;
use sinex_model::{Batch, OperationId};
use sinex_store::excision::{self, Applied, Plan, Rewrite, Target};
use sinex_store::replay::{self, Diff};
use sinex_store::{Blobs, DirtyKeys, Outcome};
use sqlx::{Connection, PgConnection};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    /// Live capture; the daemon's source tasks submit these.
    #[allow(dead_code, reason = "the daemon's source tasks submit live batches")]
    Live,
    /// Outputs of derivations (§6.2), and replays, which re-derive.
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

/// One replay chain: its batches run in one transaction, admitted as they
/// arrive, so only a few are held at a time. A batch arriving as an error
/// (the chain could not be read) rolls the chain back.
pub struct ReplayChain {
    pub operation: OperationId,
    pub actor: String,
    pub scope: Value,
    /// The chain's first material, journaled when the chain commits.
    pub root: uuid::Uuid,
    /// Roll back after reading the difference: the comparison lane.
    pub compare: bool,
    pub batches: tokio::sync::mpsc::Receiver<Result<Batch, String>>,
}

/// An excision, planned and with its replacement bytes already stored.
pub struct Excision {
    pub sealed: Option<excision::sealed::PreparedSealed>,
    pub authority: Option<crate::sealed_access::Authority>,
    pub guard: Option<crate::excision::ExcisionGuard>,
    pub cancelled: Arc<std::sync::atomic::AtomicBool>,
    pub target: Target,
    pub plan: Plan,
    pub rewrites: Vec<Rewrite>,
    pub operation: OperationId,
    pub actor: String,
    /// Where the freed blob files are deleted after commit.
    pub blobs: Blobs,
}

/// What an excision did, and blob files it could not delete after commit
/// (no row refers to them; a later excision of the same bytes retries).
#[derive(Debug)]
pub struct Excised {
    pub applied: Applied,
    pub files_left: Vec<String>,
}

enum Work {
    Admit(Batch),
    Native(Batch, crate::source_excision::AcquisitionGuard, NativeCheck),
    Checked(Batch, NativeCheck),
    Replay(ReplayChain),
    Excise(Box<Excision>),
    Retire(String, sinex_protocol::RetireBatches, Blobs),
}

enum Done {
    Admitted(Outcome),
    Replayed(Vec<Outcome>, Diff),
    Excised(Excised),
    Retired(sinex_protocol::Retirement),
}

struct Job {
    work: Work,
    reply: oneshot::Sender<Result<Done, WriteError>>,
}

/// Where producers hand batches to the writer.
#[derive(Clone)]
pub struct WriterHandle {
    live: mpsc::Sender<Job>,
    derived: mpsc::Sender<Job>,
    bulk: mpsc::Sender<Job>,
    source_database: Option<Arc<str>>,
}

/// Capacity is acquired before any readable native blob is published. Dropping
/// the API receiver after send does not release the queued work's source guard.
pub(crate) type NativeCheck = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

pub(crate) struct NativePermit(mpsc::OwnedPermit<Job>);
impl NativePermit {
    pub async fn admit(
        self,
        batch: Batch,
        guard: crate::source_excision::AcquisitionGuard,
        check: NativeCheck,
    ) -> Result<Outcome, WriteError> {
        let (reply, answer) = oneshot::channel();
        self.0.send(Job {
            work: Work::Native(batch, guard, check),
            reply,
        });
        match answer.await.map_err(|_| WriteError::Stopped)?? {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("native admission answers with its outcome"),
        }
    }
}

impl WriterHandle {
    pub(crate) async fn submit_checked(
        &self,
        priority: Priority,
        batch: Batch,
        check: NativeCheck,
    ) -> Result<Outcome, WriteError> {
        match self.send(priority, Work::Checked(batch, check)).await? {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("checked admission returns its outcome"),
        }
    }
    pub(crate) async fn source_guard(
        &self,
        source: &str,
    ) -> Result<crate::source_excision::AcquisitionGuard, String> {
        let database = self
            .source_database
            .as_deref()
            .ok_or("writer has no native acquisition connection configuration")?;
        crate::source_excision::AcquisitionGuard::acquire(database, source).await
    }
    #[cfg(test)]
    pub(crate) async fn excision_guard(
        &self,
        source: String,
    ) -> Result<crate::excision::ExcisionGuard, String> {
        let database = self
            .source_database
            .as_deref()
            .ok_or("writer has no source connection configuration")?;
        crate::excision::ExcisionGuard::connect(database, source).await
    }
    pub(crate) async fn reserve_native(
        &self,
        priority: Priority,
    ) -> Result<NativePermit, WriteError> {
        let channel = match priority {
            Priority::Live => &self.live,
            Priority::Derived => &self.derived,
            Priority::Bulk => &self.bulk,
        };
        Ok(NativePermit(
            channel
                .clone()
                .reserve_owned()
                .await
                .map_err(|_| WriteError::Stopped)?,
        ))
    }

    /// Submit a batch and wait until it is committed (or refused).
    pub async fn submit(&self, priority: Priority, batch: Batch) -> Result<Outcome, WriteError> {
        match self.send(priority, Work::Admit(batch)).await? {
            Done::Admitted(outcome) => Ok(outcome),
            _ => unreachable!("an admission answers with its outcome"),
        }
    }

    /// Replay one chain in one transaction; in compare mode nothing is kept.
    pub async fn replay(&self, chain: ReplayChain) -> Result<(Vec<Outcome>, Diff), WriteError> {
        match self.send(Priority::Derived, Work::Replay(chain)).await? {
            Done::Replayed(outcomes, diff) => Ok((outcomes, diff)),
            _ => unreachable!("a replay answers with its outcomes"),
        }
    }

    pub async fn excise(&self, excision: Excision) -> Result<Excised, WriteError> {
        match self
            .send(Priority::Live, Work::Excise(Box::new(excision)))
            .await?
        {
            Done::Excised(done) => Ok(done),
            _ => unreachable!("an excision answers with what it removed"),
        }
    }

    pub async fn retire(
        &self,
        producer: String,
        request: sinex_protocol::RetireBatches,
        blobs: Blobs,
    ) -> Result<sinex_protocol::Retirement, WriteError> {
        match self
            .send(Priority::Live, Work::Retire(producer, request, blobs))
            .await?
        {
            Done::Retired(done) => Ok(done),
            _ => unreachable!("retirement returns its outcome"),
        }
    }

    async fn send(&self, priority: Priority, work: Work) -> Result<Done, WriteError> {
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
pub struct Queues {
    live: mpsc::Receiver<Job>,
    derived: mpsc::Receiver<Job>,
    bulk: mpsc::Receiver<Job>,
}

/// Channel depth per priority. A full channel makes producers wait, which is
/// the backpressure on capture and imports.
const DEPTH: usize = 16;

#[cfg(test)]
pub fn channels() -> (WriterHandle, Queues) {
    channels_with_database(None)
}

pub(crate) fn channels_for(database: &str) -> (WriterHandle, Queues) {
    channels_with_database(Some(Arc::from(database)))
}

fn channels_with_database(source_database: Option<Arc<str>>) -> (WriterHandle, Queues) {
    let (live, live_rx) = mpsc::channel(DEPTH);
    let (derived, derived_rx) = mpsc::channel(DEPTH);
    let (bulk, bulk_rx) = mpsc::channel(DEPTH);
    (
        WriterHandle {
            live,
            derived,
            bulk,
            source_database,
        },
        Queues {
            live: live_rx,
            derived: derived_rx,
            bulk: bulk_rx,
        },
    )
}

/// The session advisory lock only the writer holds, per database.
const WRITER_LOCK: i64 = sinex_schema::WRITER_LOCK;

/// Claim `connection` as the database's one writer (architecture §4; law
/// L43). A session advisory lock, so it lasts exactly as long as the
/// connection: a killed writer frees it, a second one is refused.
///
/// A writer that was just killed holds the lock until the server notices its
/// backend is gone, so a restart waits up to [`CLAIM_WAIT`] before refusing.
pub async fn claim(connection: &mut PgConnection) -> Result<(), String> {
    sinex_schema::require_current(connection)
        .await
        .map_err(|error| error.to_string())?;
    let deadline = tokio::time::Instant::now() + CLAIM_WAIT;
    loop {
        let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(WRITER_LOCK)
            .fetch_one(&mut *connection)
            .await
            .map_err(|e| format!("database: {e}"))?;
        if won {
            // An upgrade may have won the lock after the initial version read.
            if let Err(error) = sinex_schema::require_current(connection).await {
                sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(WRITER_LOCK)
                    .execute(&mut *connection)
                    .await
                    .map_err(|e| e.to_string())?;
                return Err(error.to_string());
            }
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
pub async fn run(url: String, connection: PgConnection, queues: Queues, keys: Arc<dyn DirtyKeys>) {
    run_notifying(url, connection, queues, keys, watch::channel(0).0).await;
}

/// [`run`], also counting committed batches on `commits`, which wakes the
/// commit-log reader and the publisher (architecture §4.2 step 8).
pub async fn run_notifying(
    url: String,
    mut connection: PgConnection,
    mut queues: Queues,
    keys: Arc<dyn DirtyKeys>,
    commits: watch::Sender<u64>,
) {
    loop {
        let job = tokio::select! {
            biased;
            Some(job) = queues.live.recv() => job,
            Some(job) = queues.derived.recv() => job,
            Some(job) = queues.bulk.recv() => job,
            else => return,
        };
        let result = match job.work {
            Work::Admit(batch) => admit(&mut connection, &batch, keys.as_ref())
                .await
                .map(Done::Admitted),
            Work::Checked(batch, check) => {
                admit_checked(&mut connection, &batch, keys.as_ref(), &*check)
                    .await
                    .map(Done::Admitted)
            }
            Work::Native(batch, mut guard, check) => {
                let result = async {
                    let source = match &batch.kind {
                        sinex_model::BatchKind::Source(progress) => {
                            progress.excision_basis.as_ref().map(|b| b.source.as_str())
                        }
                        sinex_model::BatchKind::Ingress(frame) => Some(frame.producer.as_str()),
                        _ => None,
                    };
                    if source != Some(guard.source()) {
                        return Err(WriteError::Refused(
                            "publication source differs from its acquisition guard".into(),
                        ));
                    }
                    guard.check().await.map_err(WriteError::Refused)?;
                    admit_checked(&mut connection, &batch, keys.as_ref(), &*check)
                        .await
                        .map(Done::Admitted)
                }
                .await;
                // A committed admission is still committed if closing the guard
                // connection fails. The owned connection is never returned to a pool.
                let released = guard.release().await;
                if let Err(error) = released {
                    eprintln!("sinexd: closing native source guard: {error}");
                }
                result
            }
            Work::Replay(mut chain) => replay_chain(&mut connection, &mut chain, keys.as_ref())
                .await
                .map(|(o, d)| Done::Replayed(o, d)),
            Work::Retire(producer, request, blobs) => {
                crate::row_retirement::retire(&mut connection, &blobs, &producer, &request)
                    .await
                    .map(Done::Retired)
                    .map_err(WriteError::Refused)
            }
            Work::Excise(mut excision) => {
                let result = excise(&mut connection, &mut excision, keys.as_ref()).await;
                let released = match excision.guard.take() {
                    Some(guard) => guard.release().await.map_err(WriteError::Refused),
                    None => Ok(()),
                };
                match (result, released) {
                    (Ok(done), Ok(())) => Ok(Done::Excised(done)),
                    (Err(error), _) => Err(error),
                    (_, Err(error)) => Err(error),
                }
            }
        };
        if let Err(WriteError::Refused(_)) = &result
            && connection.ping().await.is_err()
            && let Ok(mut fresh) = PgConnection::connect(&url).await
        {
            // The lock went with the lost connection; without it this
            // process is no longer the writer.
            if let Err(e) = claim(&mut fresh).await {
                let _ = job.reply.send(Err(WriteError::Refused(e)));
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

async fn admit(
    connection: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> Result<Outcome, WriteError> {
    admit_checked(connection, batch, keys, &|| Ok(())).await
}
async fn admit_checked(
    connection: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
    check: &(dyn Fn() -> Result<(), String> + Send + Sync),
) -> Result<Outcome, WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    check().map_err(WriteError::Refused)?;
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    crate::row_retirement::validate_sources(&mut tx, batch)
        .await
        .map_err(WriteError::Refused)?;
    match sinex_store::admit(&mut tx, batch, keys).await {
        Ok(outcome) => {
            if let Err(error) = check() {
                tx.rollback().await.map_err(|e| refuse(&e))?;
                return Err(WriteError::Refused(error));
            }
            tx.commit().await.map_err(|e| refuse(&e))?;
            Ok(outcome)
        }
        Err(error) => {
            tx.rollback().await.map_err(|e| refuse(&e))?;
            Err(refuse(&error))
        }
    }
}

async fn replay_chain(
    connection: &mut PgConnection,
    chain: &mut ReplayChain,
    keys: &dyn DirtyKeys,
) -> Result<(Vec<Outcome>, Diff), WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    replay::begin(&mut tx, chain.operation, &chain.actor, &chain.scope)
        .await
        .map_err(|e| refuse(&e))?;
    let mut outcomes = Vec::new();
    while let Some(batch) = chain.batches.recv().await {
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
    let diff = replay::diff(&mut tx, chain.operation, &admitted)
        .await
        .map_err(|e| refuse(&e))?;
    if chain.compare {
        tx.rollback().await.map_err(|e| refuse(&e))?;
    } else {
        replay::chain_done(&mut tx, chain.operation, chain.root)
            .await
            .map_err(|e| refuse(&e))?;
        tx.commit().await.map_err(|e| refuse(&e))?;
    }
    Ok((outcomes, diff))
}

async fn excise(
    connection: &mut PgConnection,
    e: &mut Excision,
    keys: &dyn DirtyKeys,
) -> Result<Excised, WriteError> {
    let refuse = |e: &dyn std::fmt::Display| WriteError::Refused(e.to_string());
    if let Some(guard) = e.guard.as_mut() {
        guard.check().await.map_err(WriteError::Refused)?;
    }
    if let Some(prepared) = &e.sealed {
        let guard = e.guard.as_mut().ok_or_else(|| {
            WriteError::Refused("sealed excision has no owned source guard".into())
        })?;
        if guard.source() != prepared.source {
            return Err(WriteError::Refused(
                "sealed excision source guard does not match preparation".into(),
            ));
        }
        guard.check().await.map_err(WriteError::Refused)?;
        e.authority
            .as_ref()
            .ok_or_else(|| WriteError::Refused("sealed excision requires scoped authority".into()))?
            .check()
            .map_err(WriteError::Refused)?;
        let authority = e.authority.as_ref().expect("checked scoped authority");
        for rotation in &prepared.rotations {
            let old = authority
                .unwrap_secrets(&rotation.old.wrapped)
                .map_err(WriteError::Refused)?;
            let fresh = authority
                .unwrap_secrets(&rotation.replacement.wrapped)
                .map_err(WriteError::Refused)?;
            if old.name_record_hash(&[0; 32]) != fresh.name_record_hash(&[0; 32]) {
                return Err(WriteError::Refused(
                    "sealed replacement changed its record-naming capability".into(),
                ));
            }
            if let (Ok(old), Ok(fresh)) = (
                old.data_key(rotation.old.reference.clone()),
                fresh.data_key(rotation.replacement.reference.clone()),
            ) && old.key == fresh.key
            {
                return Err(WriteError::Refused(
                    "sealed replacement retained its retiring data secret".into(),
                ));
            }
            for envelope in [&rotation.old, &rotation.replacement] {
                let secrets = authority
                    .unwrap_secrets(&envelope.wrapped)
                    .map_err(WriteError::Refused)?;
                if envelope.class != "sealed" || secrets.role().as_str() != envelope.role {
                    return Err(WriteError::Refused(
                        "sealed replacement capability role or class does not match".into(),
                    ));
                }
            }
        }
    }
    let authority = e.authority.as_ref();
    let cancelled = &e.cancelled;
    let check = || {
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(sinex_store::Error::Invalid(
                "sealed excision was cancelled".into(),
            ));
        }
        if let Some(authority) = authority {
            authority.check().map_err(sinex_store::Error::Invalid)?;
        }
        Ok(())
    };
    check().map_err(|e| refuse(&e))?;
    let mut tx = connection.begin().await.map_err(|e| refuse(&e))?;
    let result = async {
        let applied = excision::apply_prepared(
            &mut tx,
            e.target,
            &e.plan,
            &e.rewrites,
            e.operation,
            &e.actor,
            keys,
            e.sealed.as_ref(),
            &check,
        )
        .await
        .map_err(|e| refuse(&e))?;
        check().map_err(|e| refuse(&e))?;
        if let Some(guard) = e.guard.as_mut() {
            guard.check().await.map_err(WriteError::Refused)?;
        }
        Ok::<_, WriteError>(applied)
    }
    .await;
    let applied = match result {
        Ok(applied) => {
            tx.commit().await.map_err(|e| refuse(&e))?;
            applied
        }
        Err(error) => {
            tx.rollback().await.map_err(|e| refuse(&e))?;
            return Err(error);
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
