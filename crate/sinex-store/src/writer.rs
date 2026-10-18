//! The store and its single writer task.
//!
//! `Store::connect` takes the state directory lock and a PostgreSQL advisory lock,
//! records a run, then moves the one write connection into a task that executes
//! queued commands in order and runs background recomputation between them.
use crate::admission::{Admission, Command, ReplayCommand};
use crate::blob::{BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, WRITING_BLOBS, parse_hash};
use crate::read_pool::{ReadPool, require_reader};
use crate::schema::{WRITER_ROLE, check_schema};
use crate::{Error, PrivacyRule, Result, TestPause, session, shell};
use fs2::FileExt;
use serde_json::Value;
use sqlx::{Connection, Executor, PgConnection, postgres::PgPoolOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use uuid::Uuid;

/// Identity of the running daemon, supplied by the caller and recorded on its run row.
pub struct RuntimeIdentity {
    pub service_name: String,
    pub host: String,
    pub version: String,
    pub executable_blake3: String,
    pub config_blake3: String,
}

/// State owned by the writer task. Holds the only write connection, including its
/// session advisory lock; nothing else gets a write connection.
pub(crate) struct WriterPool {
    pub(crate) connection: PgConnection,
    pub(crate) blobs: Arc<hashed_blobs::Store>,
    _lock: std::fs::File,
    pub(crate) root: String,
    pub(crate) run_id: Uuid,
    pub(crate) reader: ReadPool,
    pub(crate) replay_rules: Option<Vec<PrivacyRule>>,
    pub(crate) shell_producer: Option<Uuid>,
    pub(crate) shell_work: Option<shell::Work>,
    pub(crate) shell_failed: bool,
    pub(crate) session_producer: Option<Uuid>,
    pub(crate) session_work: Option<session::Work>,
    pub(crate) session_failed: bool,
    pub(crate) session_retry_at: Option<Instant>,
    pub(crate) pause: Option<TestPause>,
}

pub struct Store {
    run_id: Uuid,
    pub admission: Admission,
    pub reader: ReadPool,
    pub(crate) writer: JoinHandle<Result<()>>,
}

const WRITER_LOCK: &str = "SELECT pg_try_advisory_lock(73018924012)";
pub(crate) const COUNTER: &str = "UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq";
pub(crate) const MATERIAL: &str = "INSERT INTO raw.source_material_registry (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes) VALUES ($1,'file',$2,'completed','unknown',$3,$4,$4)";
const RUN_INTERRUPT: &str =
    "UPDATE core.runs SET status='interrupted',ended_at=now() WHERE status='running'";
const MANIFEST: &str = "INSERT INTO core.manifests (id,name,manifest_type,version,artifact_kind,artifact_digest,artifact_descriptor,emitted_event_types,consumed_event_types) SELECT COALESCE(MAX(id),0)+1,$1,'daemon',$2,'native_executable',$3,$4,ARRAY['synthetic.observed','command.executed','command.canonical','command.imported','activity.session.boundary']::text[],ARRAY[]::text[] FROM core.manifests RETURNING id";
const RUN_START: &str = "INSERT INTO core.runs (id,manifest_id,service_name,host,status,effective_config_hash) VALUES ($1,$2,$3,$4,'running',$5)";
const RUN_END: &str = "UPDATE core.runs SET status='completed',ended_at=now() WHERE id=$1";

impl Store {
    pub async fn connect(
        writer_url: &str,
        reader_url: &str,
        state_dir: PathBuf,
        capacity: usize,
        identity: RuntimeIdentity,
        pause: Option<TestPause>,
    ) -> Result<Self> {
        if capacity == 0 || capacity > 1024 {
            return Err(Error::Invalid("queue capacity must be 1..=1024"));
        }
        if parse_hash(&identity.executable_blake3).is_err()
            || parse_hash(&identity.config_blake3).is_err()
            || identity.service_name.is_empty()
            || identity.host.is_empty()
            || identity.version.is_empty()
        {
            return Err(Error::Invalid(
                "actual executable/config hashes and runtime identity required",
            ));
        }
        if !check_schema(writer_url).await?.pending_changes.is_empty() {
            return Err(Error::Schema(
                "ordered schema changes pending; use admin schema --apply with a dump path".into(),
            ));
        }
        let (lock, root, blobs) =
            tokio::task::spawn_blocking(move || lock_state_dir(state_dir)).await??;
        let mut connection = PgConnection::connect(writer_url).await?;
        require_role(&mut connection, WRITER_ROLE).await?;
        take_writer_lock(&mut connection).await?;
        let reader_pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query(
                        "SELECT set_config('client_connection_check_interval','100ms',false),\
                         set_config('statement_timeout','10s',false)",
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            })
            .connect(reader_url)
            .await?;
        require_reader(&reader_pool).await?;
        let run_id = start_run(&mut connection, identity).await?;
        // The reader must see this run, or the two URLs point at different databases.
        let paired: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core.runs WHERE id=$1 AND status='running')",
        )
        .bind(run_id)
        .fetch_one(&reader_pool)
        .await?;
        if !paired {
            return Err(Error::Invalid(
                "reader connection belongs to a different store",
            ));
        }
        let reader = ReadPool::new(reader_pool, root.clone());
        let observe_atuin_queue = pause.as_ref().is_some_and(TestPause::observes_atuin_queue);
        let mut owner = WriterPool {
            connection,
            blobs: Arc::new(blobs),
            _lock: lock,
            root,
            run_id,
            reader: reader.clone(),
            replay_rules: None,
            shell_producer: None,
            shell_work: None,
            shell_failed: false,
            session_producer: None,
            session_work: None,
            session_failed: false,
            session_retry_at: None,
            pause,
        };
        owner.recover_blobs().await?;
        owner.recover_parts().await?;
        let (tx, rx) = mpsc::channel(capacity);
        let writer = tokio::spawn(owner.run(rx));
        Ok(Self {
            run_id,
            admission: Admission {
                sender: tx,
                observe_atuin_queue,
            },
            reader,
            writer,
        })
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Closes admission, lets the writer finish every command already queued, then
    /// drops the write connection and its advisory lock. Cloned handles then fail.
    pub async fn shutdown(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.admission.send(Command::Shutdown(tx)).await?;
        rx.await.map_err(|_| Error::Stopped)?;
        self.writer.await??;
        self.reader.close().await;
        Ok(())
    }
}

/// Creates the state directory, takes its exclusive `writer.lock` and opens the blob store.
fn lock_state_dir(state_dir: PathBuf) -> Result<(std::fs::File, String, hashed_blobs::Store)> {
    std::fs::create_dir_all(&state_dir)?;
    let root = std::fs::canonicalize(&state_dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("writer.lock"))?;
    match lock.try_lock_exclusive() {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Err(Error::WriterOwned),
        Err(e) => return Err(e.into()),
    }
    let blobs = hashed_blobs::Store::open(&root, vec![], 0o600)?;
    Ok((lock, root.to_string_lossy().into_owned(), blobs))
}

/// Waits up to ten seconds for the database-wide writer advisory lock.
async fn take_writer_lock(connection: &mut PgConnection) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let locked: bool = sqlx::query_scalar(WRITER_LOCK)
            .fetch_one(&mut *connection)
            .await?;
        if locked {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::WriterOwned);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn require_role(connection: &mut PgConnection, role: &str) -> Result<()> {
    let actual: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(connection)
        .await?;
    if actual != role {
        return Err(Error::Invalid(
            "writer URL must authenticate as sinex_writer",
        ));
    }
    Ok(())
}

/// Marks runs left `running` by a crash as interrupted, then records this run.
async fn start_run(conn: &mut PgConnection, identity: RuntimeIdentity) -> Result<Uuid> {
    let mut tx = conn.begin().await?;
    sqlx::query(RUN_INTERRUPT).execute(&mut *tx).await?;
    let descriptor = serde_json::json!({"scope":"running_executable","retained":false});
    let manifest: i32 = sqlx::query_scalar(MANIFEST)
        .bind(&identity.service_name)
        .bind(identity.version)
        .bind(identity.executable_blake3)
        .bind(descriptor)
        .fetch_one(&mut *tx)
        .await?;
    let run_id = Uuid::now_v7();
    sqlx::query(RUN_START)
        .bind(run_id)
        .bind(manifest)
        .bind(identity.service_name)
        .bind(identity.host)
        .bind(identity.config_blake3)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(run_id)
}

/// 16-byte hash identifying an event's position in its material, used as
/// `core.live_coordinates.occ_hash`.
pub(crate) fn coordinate_hash(
    material: Uuid,
    anchor: i64,
    event_type: &str,
    ordinal: i16,
) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(material.as_bytes());
    hash.update(&anchor.to_be_bytes());
    hash.update(&(event_type.len() as u64).to_be_bytes());
    hash.update(event_type.as_bytes());
    hash.update(&ordinal.to_be_bytes());
    hash.finalize().as_bytes()[..16].to_vec()
}

/// Prepares the writer's dynamic statements against a database, so a test can
/// check them against the installed schema. Checked `query!` macros need no such step.
pub async fn prepare_admission_statements(admin_url: &str) -> Result<()> {
    let mut conn = PgConnection::connect(admin_url).await?;
    for sql in [
        COUNTER,
        MATERIAL,
        BLOB_REGISTER,
        BLOB_LOOKUP,
        BLOB_PRESENT,
        WRITING_BLOBS,
        RUN_INTERRUPT,
        MANIFEST,
        RUN_START,
        RUN_END,
        WRITER_LOCK,
        "SELECT current_user::text",
    ] {
        conn.prepare(sql).await?;
    }
    Ok(())
}

/// True when a step reported admitted or removed rows, which may unblock failed
/// shell and session recomputation.
fn changed_rows(outcome: &Value) -> bool {
    outcome["admitted"].as_i64().unwrap_or(0) > 0 || outcome["removed"].as_i64().unwrap_or(0) > 0
}

impl WriterPool {
    async fn run(mut self, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        let mut shutdown = None;
        let mut replay_turn = false;
        let mut queued_turns = 0_u8;
        loop {
            // Queued commands go first, but at most two run before one background turn,
            // so steady admission cannot starve replay or shell and session
            // recomputation. After shutdown, only the queued commands are drained.
            let command = if shutdown.is_none()
                && (self.replay_rules.is_some() || self.shell_producer.is_some())
            {
                if rx.is_closed() && rx.is_empty() {
                    break;
                }
                let queued = if queued_turns < 2 {
                    match rx.try_recv() {
                        Ok(command) => Some(command),
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                        Err(mpsc::error::TryRecvError::Empty) => None,
                    }
                } else {
                    None
                };
                if queued.is_some() {
                    queued
                } else {
                    queued_turns = 0;
                    if self.background_step(&mut replay_turn, rx.len()).await? {
                        continue;
                    }
                    if let Some(at) = self.session_retry_at {
                        tokio::select! {
                            command = rx.recv() => command,
                            _ = tokio::time::sleep_until(at) => continue,
                        }
                    } else {
                        rx.recv().await
                    }
                }
            } else {
                rx.recv().await
            };
            let Some(command) = command else { break };
            queued_turns = queued_turns.saturating_add(1).min(2);
            if let Command::Shutdown(reply) = command {
                rx.close();
                shutdown = Some(reply);
            } else {
                self.execute(command).await;
            }
        }
        sqlx::query(RUN_END)
            .bind(self.run_id)
            .execute(&mut self.connection)
            .await?;
        self.connection.close().await?;
        if let Some(reply) = shutdown {
            // The requester may have gone; the run is closed either way.
            let _ = reply.send(());
        }
        Ok(())
    }

    /// Runs one command and sends its reply. A caller that stopped waiting cannot
    /// cancel the command; its committed result stands and the reply is dropped.
    async fn execute(&mut self, command: Command) {
        match command {
            Command::SessionEnable(reply) => {
                let _ = reply.send(self.session_enable().await);
            }
            Command::ShellEnable(reply) => {
                let _ = reply.send(self.shell_enable().await);
            }
            Command::ReplayProfile(rules, reply) => {
                self.replay_rules = rules;
                let _ = reply.send(());
            }
            Command::Admit(event, reply) => {
                let _ = reply.send(self.admit(event).await);
            }
            Command::Zsh(step, reply) => {
                let result = self.admit_zsh(step).await;
                if result.as_ref().is_ok_and(changed_rows) {
                    self.clear_recompute_failures();
                }
                let _ = reply.send(result);
            }
            Command::SourceSchedule(observation, schedule, reply) => {
                let _ = reply.send(self.schedule_source(observation, schedule).await);
            }
            Command::BrowserBegin(begin, reply) => {
                let _ = reply.send(self.browser_begin(begin).await);
            }
            Command::BrowserStep(step, reply) => {
                let _ = reply.send(self.browser_step(step).await);
            }
            Command::BrowserPending(input, snapshot_seq, after, reply) => {
                let _ = reply.send(self.browser_pending(input, snapshot_seq, after).await);
            }
            Command::BrowserKnown(profile, generation, keys, reply) => {
                let result = self
                    .browser_known_downloads(profile, generation, keys)
                    .await;
                let _ = reply.send(result);
            }
            Command::BrowserSnapshot(input, observation, path, reply) => {
                let _ = reply.send(self.browser_snapshot(input, observation, path).await);
            }
            Command::AtuinReadStart(locator, reply) => {
                let _ = reply.send(self.atuin_read_start(&locator).await);
            }
            Command::AtuinUnfinished(input, snapshot_seq, after, reply) => {
                let _ = reply.send(self.atuin_unfinished_page(input, snapshot_seq, after).await);
            }
            Command::Atuin(step, reply) => {
                let result = self.admit_atuin(step).await;
                if result.as_ref().is_ok_and(changed_rows) {
                    self.clear_recompute_failures();
                }
                let _ = reply.send(result);
            }
            Command::Exclude(action, request, reply) => {
                self.clear_recompute_failures();
                let _ = reply.send(self.exclude(action, request).await);
            }
            Command::Replay(command, reply) => {
                self.clear_recompute_failures();
                let _ = reply.send(self.replay(command).await);
            }
            Command::Shutdown(_) => unreachable!("run handles shutdown"),
        }
    }

    /// New input may resolve the fault that stopped recomputation, so try again.
    fn clear_recompute_failures(&mut self) {
        self.shell_failed = false;
        self.session_failed = false;
        self.session_retry_at = None;
    }

    /// Statement timeouts and cancellations are transient; other errors are not.
    pub(crate) fn session_retryable(error: &Error) -> bool {
        matches!(error, Error::Deadline)
            || matches!(error, Error::Database(sqlx::Error::Database(e)) if e.code().as_deref() == Some("57014"))
    }

    /// Runs one unit of background work: replay, session or shell recomputation.
    /// Returns whether any work was done.
    async fn background_step(
        &mut self,
        replay_turn: &mut bool,
        queued_commands: usize,
    ) -> Result<bool> {
        // Alternate replay with recomputation when both have work.
        if *replay_turn && self.replay_rules.is_some() {
            *replay_turn = false;
            if let Some(request) = self.next_replay().await? {
                self.replay(ReplayCommand::Start(request)).await?;
                return Ok(true);
            }
        }
        *replay_turn = true;
        if self.session_retry_at.is_some_and(|at| Instant::now() >= at) {
            self.session_failed = false;
            self.session_retry_at = None;
        }
        if !self.session_failed && self.session_producer.is_some() && self.session_turn().await? {
            return Ok(true);
        }
        if !self.shell_failed && self.shell_turn(queued_commands).await? {
            return Ok(true);
        }
        if self.replay_rules.is_some()
            && let Some(request) = self.next_replay().await?
        {
            self.replay(ReplayCommand::Start(request)).await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// One session recomputation step under a 3 second limit (125 seconds while a
    /// test pause is configured). A failure is recorded in `session_progress` and
    /// stops session work until new input arrives or, for timeouts, 250 ms pass.
    async fn session_turn(&mut self) -> Result<bool> {
        sqlx::query("SET statement_timeout='2000ms'")
            .execute(&mut self.connection)
            .await?;
        let limit = Duration::from_secs(if self.pause.is_some() { 125 } else { 3 });
        let result = tokio::time::timeout(limit, self.session_step())
            .await
            .unwrap_or(Err(Error::Deadline));
        sqlx::query("SET statement_timeout=0")
            .execute(&mut self.connection)
            .await?;
        let error = match result {
            Ok(progressed) => return Ok(progressed),
            Err(error) => error,
        };
        self.session_retry_at =
            Self::session_retryable(&error).then(|| Instant::now() + Duration::from_millis(250));
        let reason = match error {
            Error::Deadline => "session computation turn exceeded 3 seconds; dirty key retained",
            Error::Invalid(reason) => reason,
            _ => "session recomputation unavailable; dirty keys retained",
        };
        self.session_failed = true;
        self.session_work = None;
        sqlx::query("UPDATE derivation.session_progress SET failure=$1 WHERE one")
            .bind(reason)
            .execute(&mut self.connection)
            .await?;
        Ok(false)
    }

    /// One shell recomputation step. A failure is recorded in `shell_progress` and
    /// stops shell work until new input arrives.
    async fn shell_turn(&mut self, queued_commands: usize) -> Result<bool> {
        let error = match self.shell_step(queued_commands).await {
            Ok(progressed) => return Ok(progressed),
            Err(error) => error,
        };
        // Publish only static validation reasons; database errors may quote captured values.
        let reason = match error {
            Error::Invalid(reason) => {
                format!("shell recomputation unavailable: {reason}; dirty work retained")
            }
            _ => "shell recomputation unavailable; dirty work retained".into(),
        };
        self.shell_failed = true;
        self.shell_work = None;
        sqlx::query("UPDATE derivation.shell_progress SET failure=$1 WHERE one")
            .bind(reason)
            .execute(&mut self.connection)
            .await?;
        Ok(false)
    }
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
