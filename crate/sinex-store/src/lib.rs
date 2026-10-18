//! Foundation storage authority. No connection or transaction escapes the writer owner.
mod browser;
pub use browser::{BrowserBegin, BrowserPending, BrowserStep};
mod atuin;
mod zsh;
pub use zsh::{PROTOCOL as ZSH_PROTOCOL, SOURCE as ZSH_SOURCE, ZshStep};
mod event_storage;
mod exclusion;
mod projection;
mod query;
mod recall;
mod replay;
mod search;
mod session;
mod shell;
mod source_coverage;
mod source_inspection;
mod test_pause;
pub use atuin::mask as present;
pub use atuin::{
    AtuinInterpretation, AtuinStep, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule,
    SOURCE as ATUIN_SOURCE,
};
pub use projection::Projection;
pub use query::{validate_installed_raw_query, validate_query};
pub use recall::{is_recall, validate_recall};
pub use search::indexed as is_indexed_query;
pub use test_pause::TestPause;
mod schema;
pub use schema::{
    CAPTURED_FUTURE_SKEW_SECONDS, READER_ROLE, SchemaStatus, TIMESCALEDB_VERSION, WRITER_ROLE,
    apply_changes, bootstrap, check_schema,
};

use fs2::FileExt;
use serde::Serialize;
use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use std::path::PathBuf;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("schema incompatible: {0}")]
    Schema(String),
    #[error("invalid foundation admission: {0}")]
    Invalid(&'static str),
    #[error("indexed projection unavailable: {0}")]
    ProjectionUnavailable(&'static str),
    #[error("read execution deadline exceeded")]
    Deadline,
    #[error("primary source inspection statement cancelled")]
    SourceStatementCancelled {
        configured_statement_timeout_ms: u64,
    },
    #[error("writer already owned by another connection")]
    WriterOwned,
    #[error("admission owner has stopped")]
    Stopped,
    #[error("blob operation failed: {0}")]
    Blob(#[from] hashed_blobs::Error),
    #[error("background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("product queries are unavailable in the admin-only build")]
    ProductUnavailable,
    #[error("replay refused: {0}")]
    ReplayRefused(&'static str),
}

/// Actual identities supplied by the packaged caller, never invented by storage.
pub struct RuntimeIdentity {
    pub service_name: String,
    pub host: String,
    pub version: String,
    pub executable_blake3: String,
    pub config_blake3: String,
}

/// A bounded synthetic proof case, not a registered source or public RPC capability.
/// Original time is unknown by construction; no ingestion timestamp substitutes for it.
pub struct MaterialEvent {
    pub source: String,
    pub event_type: String,
    pub bytes: Vec<u8>,
    pub anchor_byte: i64,
    pub ordinal: i16,
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct Receipt {
    pub event_id: Uuid,
    pub material_id: Uuid,
    pub commit_seq: i64,
    pub blob_name: String,
}

#[derive(Debug, Serialize)]
pub struct StoreStatus {
    pub event_count: i64,
    pub material_count: i64,
    pub commit_seq: i64,
    pub progress_count: i64,
}

/// Actual restricted reader credential. The underlying pool is intentionally private.
#[derive(Clone)]
pub struct ReadPool(PgPool, std::sync::Arc<hashed_blobs::Reader>, String);
impl ReadPool {
    /// Read the verified installed schema without writer ownership or state creation.
    pub async fn installed(reader_url: &str, root: PathBuf) -> Result<Self> {
        check_schema(reader_url).await?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(reader_url)
            .await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&pool)
            .await?;
        if role != READER_ROLE {
            return Err(Error::Invalid(
                "reader URL must authenticate as sinex_reader",
            ));
        }
        let root = std::fs::canonicalize(root)?.to_string_lossy().into_owned();
        Ok(Self(
            pool,
            std::sync::Arc::new(hashed_blobs::Reader::new(root.clone(), vec![])),
            root,
        ))
    }
    pub async fn schema_status(&self) -> Result<SchemaStatus> {
        let mut lease = self.read_lease().await?;
        let status = schema::inspect(lease.connection()).await?;
        lease.finish();
        Ok(status)
    }
    #[cfg(feature = "product")]
    pub async fn status(&self) -> Result<StoreStatus> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query!("SELECT (SELECT count(*) FROM core.live_events) AS \"event_count!\", (SELECT count(*) FROM raw.source_material_registry) AS \"material_count!\", (SELECT seq FROM core.commit_counter WHERE one) AS \"commit_seq!\", (SELECT count(*) FROM raw.source_cursors) AS \"progress_count!\"").fetch_one(lease.connection()).await?;
        lease.finish();
        Ok(StoreStatus {
            event_count: row.event_count,
            material_count: row.material_count,
            commit_seq: row.commit_seq,
            progress_count: row.progress_count,
        })
    }
    #[cfg(not(feature = "product"))]
    pub async fn status(&self) -> Result<StoreStatus> {
        Err(Error::ProductUnavailable)
    }
    pub async fn close(&self) {
        self.0.close().await;
    }
}

#[derive(Clone)]
pub struct Admission {
    sender: mpsc::Sender<Command>,
    observe_atuin_queue: bool,
}
impl Admission {
    pub async fn browser_begin(
        &self,
        begin: BrowserBegin,
    ) -> Result<sinex_model::browser::ReadBasis> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::BrowserBegin(begin, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn browser_step(&self, step: BrowserStep) -> Result<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::BrowserStep(step, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn browser_pending(
        &self,
        input: Uuid,
        basis: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<BrowserPending>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::BrowserPending(input, basis, after, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn browser_known_downloads(
        &self,
        profile: String,
        generation: Uuid,
        keys: Vec<String>,
    ) -> Result<Vec<bool>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::BrowserKnown(profile, generation, keys, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn browser_snapshot(
        &self,
        input: Uuid,
        observation: Uuid,
        path: PathBuf,
    ) -> Result<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::BrowserSnapshot(input, observation, path, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn atuin_read_basis(
        &self,
        locator: String,
    ) -> Result<sinex_model::acquisition::AtuinReadBasis> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::AtuinReadBasis(locator, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn atuin_unfinished_page(
        &self,
        input: Uuid,
        basis: i64,
        after: Option<Uuid>,
    ) -> Result<Vec<(Uuid, String)>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::AtuinUnfinished(input, basis, after, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn submit_atuin(&self, step: AtuinStep) -> Result<serde_json::Value> {
        // Test pause only. Log key hashes and row ids, never captured keys.
        let evidence = self.observe_atuin_queue.then(|| serde_json::json!({
            "component":"sinex-store",
            "test_phase":"atuin-step-enqueued",
            "observation":step.observation,
            "sqlite_rowids":step.rows.iter().filter_map(|row| row["_rowid"].as_i64()).collect::<Vec<_>>(),
            "native_key_hashes":step.rows.iter().filter_map(|row| row["id"].as_str()).map(|key| blake3::hash(key.as_bytes()).to_hex().to_string()).collect::<Vec<_>>(),
            "source_eof":step.complete
        }));
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Atuin(step, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        if let Some(evidence) = evidence {
            // Successful channel send precedes this notice; no producer/writer
            // barrier is introduced and the writer can acknowledge earlier batches.
            eprintln!("{evidence}");
        }
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn schedule_source(
        &self,
        observation: Uuid,
        schedule: sinex_model::acquisition::PollSchedule,
    ) -> Result<()> {
        schedule
            .validate()
            .map_err(|_| Error::Invalid("invalid acquisition schedule"))?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::SourceSchedule(observation, schedule, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn submit_zsh(&self, step: ZshStep) -> Result<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Zsh(step, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }

    pub async fn replay(&self, command: ReplayCommand) -> Result<serde_json::Value> {
        let cancelled = match &command {
            ReplayCommand::Cancel(p) => Some(p.operation_id),
            _ => None,
        };
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Replay(command, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        if let Some(id) = cancelled {
            eprintln!(
                "{}",
                serde_json::json!({"component":"sinex-store","phase":"replay_cancel_enqueued","operation_id":id})
            );
        }
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn exclude(
        &self,
        action: sinex_model::exclusion::Action,
        request: sinex_model::exclusion::Request,
    ) -> Result<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Exclude(action, request, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn set_replay_profile(&self, rules: Option<Vec<PrivacyRule>>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::ReplayProfile(rules, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)
    }
    pub async fn enable_shell(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::ShellEnable(tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub async fn enable_sessions(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::SessionEnable(tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
    pub fn is_available(&self) -> bool {
        !self.sender.is_closed()
    }
    /// Backpressure awaits bounded capacity; success means the transaction committed.
    pub async fn submit_synthetic(&self, event: MaterialEvent) -> Result<Receipt> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Admit(event, tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?
    }
}
pub enum ReplayCommand {
    Plan(sinex_model::replay::Selection, Vec<PrivacyRule>),
    Start(sinex_model::replay::Start),
    Cancel(sinex_model::replay::Operation),
}
enum Command {
    BrowserBegin(
        BrowserBegin,
        oneshot::Sender<Result<sinex_model::browser::ReadBasis>>,
    ),
    BrowserStep(BrowserStep, oneshot::Sender<Result<serde_json::Value>>),
    BrowserPending(
        Uuid,
        i64,
        Option<Uuid>,
        oneshot::Sender<Result<Vec<BrowserPending>>>,
    ),
    BrowserKnown(
        String,
        Uuid,
        Vec<String>,
        oneshot::Sender<Result<Vec<bool>>>,
    ),
    BrowserSnapshot(
        Uuid,
        Uuid,
        PathBuf,
        oneshot::Sender<Result<serde_json::Value>>,
    ),
    Exclude(
        sinex_model::exclusion::Action,
        sinex_model::exclusion::Request,
        oneshot::Sender<Result<serde_json::Value>>,
    ),
    ShellEnable(oneshot::Sender<Result<()>>),
    SessionEnable(oneshot::Sender<Result<()>>),
    ReplayProfile(Option<Vec<PrivacyRule>>, oneshot::Sender<()>),
    Replay(ReplayCommand, oneshot::Sender<Result<serde_json::Value>>),
    Admit(MaterialEvent, oneshot::Sender<Result<Receipt>>),
    SourceSchedule(
        Uuid,
        sinex_model::acquisition::PollSchedule,
        oneshot::Sender<Result<()>>,
    ),
    AtuinReadBasis(
        String,
        oneshot::Sender<Result<sinex_model::acquisition::AtuinReadBasis>>,
    ),
    AtuinUnfinished(
        Uuid,
        i64,
        Option<Uuid>,
        oneshot::Sender<Result<Vec<(Uuid, String)>>>,
    ),
    Atuin(AtuinStep, oneshot::Sender<Result<serde_json::Value>>),
    Zsh(ZshStep, oneshot::Sender<Result<serde_json::Value>>),
    Shutdown(oneshot::Sender<()>),
}

/// Kept private and moved exactly once into the writer task. It owns one real connection,
/// including its session advisory lock, rather than lending a pool to API handlers.
struct WriterPool {
    connection: PgConnection,
    blobs: std::sync::Arc<hashed_blobs::Store>,
    _lock: std::fs::File,
    root: String,
    run_id: Uuid,
    reader: ReadPool,
    replay_rules: Option<Vec<PrivacyRule>>,
    shell_producer: Option<Uuid>,
    shell_work: Option<shell::Work>,
    shell_failed: bool,
    session_producer: Option<Uuid>,
    session_work: Option<session::Work>,
    session_failed: bool,
    session_retry_at: Option<tokio::time::Instant>,
    pause: Option<TestPause>,
}

pub struct Store {
    run_id: Uuid,
    pub admission: Admission,
    pub reader: ReadPool,
    writer: JoinHandle<Result<()>>,
}
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
        let (lock, root, blobs) = tokio::task::spawn_blocking(move || -> Result<_> {
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
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(Error::WriterOwned);
                }
                Err(e) => return Err(e.into()),
            }
            let blobs = hashed_blobs::Store::open(&root, vec![], 0o600)?;
            Ok((lock, root.to_string_lossy().into_owned(), blobs))
        })
        .await??;
        let mut connection = PgConnection::connect(writer_url).await?;
        require_role(&mut connection, WRITER_ROLE).await?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(73018924012)")
                .fetch_one(&mut connection)
                .await?;
            if locked {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::WriterOwned);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let reader_pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection,_|Box::pin(async move {sqlx::query("SELECT set_config('client_connection_check_interval','100ms',false),set_config('statement_timeout','10s',false)").execute(connection).await?;Ok(())}))
            .connect(reader_url)
            .await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&reader_pool)
            .await?;
        if role != READER_ROLE {
            return Err(Error::Invalid(
                "reader URL must authenticate as sinex_reader",
            ));
        }
        let run_id = start_run(&mut connection, identity).await?;
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
        let reader = ReadPool(
            reader_pool,
            std::sync::Arc::new(hashed_blobs::Reader::new(root.clone(), vec![])),
            root.clone(),
        );
        let observe_atuin_queue = pause.as_ref().is_some_and(TestPause::observes_atuin_queue);
        let mut owner = WriterPool {
            connection,
            blobs: std::sync::Arc::new(blobs),
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

    /// A shutdown marker closes admission, drains all already accepted commands, then
    /// drops the owning connection and its advisory lock. Cloned handles then refuse.
    pub async fn shutdown(self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.admission
            .sender
            .send(Command::Shutdown(tx))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.map_err(|_| Error::Stopped)?;
        self.writer.await??;
        self.reader.close().await;
        Ok(())
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

const COUNTER: &str = "UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq";
const MATERIAL: &str = "INSERT INTO raw.source_material_registry (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes) VALUES ($1,'file',$2,'completed','unknown',$3,$4,$4)";
const EVENT: &str = "INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_quality,commit_seq,source_material_id,anchor_byte,ordinal,record_hash,run_id,ts_precision,ts_orig_until,ts_clock,time_suspect) VALUES ($1,$2,$3,$4,$11::text::timestamptz,$12,$5,$6,$7,$8,$9,$10,$13,$14::text::timestamptz,$15,$16)";
const LOG: &str = "INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,0,0,1)";
const PROGRESS: &str = "INSERT INTO raw.source_cursors (binding,input,position,carry) VALUES ($1,'synthetic-proof',$2,'{}'::jsonb) ON CONFLICT (binding,input) DO UPDATE SET position=EXCLUDED.position,carry=EXCLUDED.carry,updated_at=now()";
const BLOB_REGISTER: &str = "INSERT INTO core.blobs (id,blake3,size_bytes,root,status) VALUES ($1,$2,$3,$4,'writing') ON CONFLICT (blake3) DO NOTHING";
const BLOB_LOOKUP: &str = "SELECT id,size_bytes,root FROM core.blobs WHERE blake3=$1";
const BLOB_PRESENT: &str = "UPDATE core.blobs SET status='present' WHERE id=$1";
const WRITING_BLOBS: &str = "SELECT id,blake3 FROM core.blobs WHERE status='writing' AND root=$1";
const RUN_INTERRUPT: &str =
    "UPDATE core.runs SET status='interrupted',ended_at=now() WHERE status='running'";
const MANIFEST: &str = "INSERT INTO core.manifests (id,name,manifest_type,version,artifact_kind,artifact_digest,artifact_descriptor,emitted_event_types,consumed_event_types) SELECT COALESCE(MAX(id),0)+1,$1,'daemon',$2,'native_executable',$3,$4,ARRAY['synthetic.observed','command.executed','command.canonical','command.imported','activity.session.boundary']::text[],ARRAY[]::text[] FROM core.manifests RETURNING id";
const RUN_START: &str = "INSERT INTO core.runs (id,manifest_id,service_name,host,status,effective_config_hash) VALUES ($1,$2,$3,$4,'running',$5)";
const RUN_END: &str = "UPDATE core.runs SET status='completed',ended_at=now() WHERE id=$1";

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

fn parse_hash(name: &str) -> Result<hashed_blobs::Hash> {
    let hex = name
        .strip_prefix("blake3:")
        .ok_or(Error::Invalid("invalid blob name"))?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(Error::Invalid("invalid blob name"));
    }
    let mut raw = [0; 32];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid blob name"))?;
    }
    Ok(hashed_blobs::Hash(raw))
}

/// Dynamic admission statements are prepared against the actual isolated schema by the
/// integration test. Admin DDL and catalog queries execute through bootstrap/check.
pub async fn prepare_admission_statements(admin_url: &str) -> Result<()> {
    let mut conn = PgConnection::connect(admin_url).await?;
    for sql in [
        COUNTER,
        MATERIAL,
        EVENT,
        LOG,
        PROGRESS,
        BLOB_REGISTER,
        BLOB_LOOKUP,
        BLOB_PRESENT,
        WRITING_BLOBS,
        RUN_INTERRUPT,
        MANIFEST,
        RUN_START,
        RUN_END,
        "SELECT current_user::text",
        "SELECT pg_try_advisory_lock(73018924012)",
    ] {
        conn.prepare(sql).await?;
    }
    Ok(())
}

impl WriterPool {
    async fn run(mut self, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        let mut shutdown = None;
        let mut replay_turn = false;
        let mut queued_turns = 0_u8;
        loop {
            // Live/control work is preferred, but continuous admission cannot starve
            // an admitted replay or pinned fusion pass: at most two queued commands
            // precede one bounded background turn. Shutdown instead closes and drains.
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
                        tokio::select! {command=rx.recv()=>command,_=tokio::time::sleep_until(at)=>{continue;}}
                    } else {
                        rx.recv().await
                    }
                }
            } else {
                rx.recv().await
            };
            let Some(command) = command else { break };
            queued_turns = queued_turns.saturating_add(1).min(2);
            match command {
                Command::SessionEnable(reply) => {
                    let result = self.session_enable().await;
                    let _ = reply.send(result);
                }
                Command::ShellEnable(reply) => {
                    let result = self.shell_enable().await;
                    let _ = reply.send(result);
                }
                Command::ReplayProfile(rules, reply) => {
                    self.replay_rules = rules;
                    let _ = reply.send(());
                }
                Command::Admit(event, reply) => {
                    // A disconnected caller cannot cancel a queued transaction midway.
                    // Its receipt may be undeliverable, but the database result is durable.
                    let result = self.admit(event).await;
                    if reply.send(result).is_err() { /* caller gone; no commit is undone */ }
                }
                Command::Zsh(step, reply) => {
                    let result = self.admit_zsh(step).await;
                    if result.as_ref().is_ok_and(|outcome| {
                        outcome["admitted"].as_i64().unwrap_or(0) > 0
                            || outcome["removed"].as_i64().unwrap_or(0) > 0
                    }) {
                        self.shell_failed = false;
                        self.session_failed = false;
                        self.session_retry_at = None;
                    }
                    let _ = reply.send(result);
                }
                Command::SourceSchedule(observation, schedule, reply) => {
                    let result = self.schedule_source(observation, schedule).await;
                    let _ = reply.send(result);
                }
                Command::BrowserBegin(begin, reply) => {
                    let result = self.browser_begin(begin).await;
                    let _ = reply.send(result);
                }
                Command::BrowserStep(step, reply) => {
                    let result = self.browser_step(step).await;
                    let _ = reply.send(result);
                }
                Command::BrowserPending(input, basis, after, reply) => {
                    let result = self.browser_pending(input, basis, after).await;
                    let _ = reply.send(result);
                }
                Command::BrowserKnown(profile, generation, keys, reply) => {
                    let result = self
                        .browser_known_downloads(profile, generation, keys)
                        .await;
                    let _ = reply.send(result);
                }
                Command::BrowserSnapshot(input, observation, path, reply) => {
                    let result = self.browser_snapshot(input, observation, path).await;
                    let _ = reply.send(result);
                }
                Command::AtuinReadBasis(locator, reply) => {
                    let _ = reply.send(self.atuin_read_basis(&locator).await);
                }
                Command::AtuinUnfinished(input, basis, after, reply) => {
                    let _ = reply.send(self.atuin_unfinished_page(input, basis, after).await);
                }
                Command::Atuin(step, reply) => {
                    let result = self.admit_atuin(step).await;
                    if result.as_ref().is_ok_and(|outcome| {
                        outcome["admitted"].as_i64().unwrap_or(0) > 0
                            || outcome["removed"].as_i64().unwrap_or(0) > 0
                    }) {
                        self.shell_failed = false;
                        self.session_failed = false;
                        self.session_retry_at = None;
                    }
                    let _ = reply.send(result);
                }
                Command::Exclude(action, request, reply) => {
                    self.shell_failed = false;
                    self.session_failed = false;
                    self.session_retry_at = None;
                    let result = self.exclude(action, request).await;
                    let _ = reply.send(result);
                }
                Command::Replay(command, reply) => {
                    self.shell_failed = false;
                    self.session_failed = false;
                    self.session_retry_at = None;
                    let result = self.replay(command).await;
                    let _ = reply.send(result);
                }
                Command::Shutdown(reply) => {
                    rx.close();
                    shutdown = Some(reply);
                }
            }
        }
        sqlx::query(RUN_END)
            .bind(self.run_id)
            .execute(&mut self.connection)
            .await?;
        self.connection.close().await?;
        if let Some(reply) = shutdown
            && reply.send(()).is_err()
        { /* requester gone */ }
        Ok(())
    }

    fn session_retryable(error: &Error) -> bool {
        matches!(error, Error::Deadline)
            || matches!(error,Error::Database(sqlx::Error::Database(e)) if e.code().as_deref()==Some("57014"))
    }

    async fn background_step(
        &mut self,
        replay_turn: &mut bool,
        queued_commands: usize,
    ) -> Result<bool> {
        // Keep replay and shell ownership alternating when both have runnable work.
        if *replay_turn && self.replay_rules.is_some() {
            *replay_turn = false;
            if let Some(request) = self.next_replay().await? {
                self.replay(ReplayCommand::Start(request)).await?;
                return Ok(true);
            }
        }
        *replay_turn = true;
        if self
            .session_retry_at
            .is_some_and(|at| tokio::time::Instant::now() >= at)
        {
            self.session_failed = false;
            self.session_retry_at = None;
        }
        if !self.session_failed && self.session_producer.is_some() {
            sqlx::query("SET statement_timeout='2000ms'")
                .execute(&mut self.connection)
                .await?;
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(if self.pause.is_some() { 125 } else { 3 }),
                self.session_step(),
            )
            .await
            .unwrap_or(Err(Error::Deadline));
            sqlx::query("SET statement_timeout=0")
                .execute(&mut self.connection)
                .await?;
            match result {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    let transient = Self::session_retryable(&error);
                    self.session_retry_at = transient.then(|| {
                        tokio::time::Instant::now() + std::time::Duration::from_millis(250)
                    });
                    let reason = match error {
                        Error::Deadline => {
                            "session computation turn exceeded 3 seconds; dirty key retained"
                        }
                        Error::Invalid(reason) => reason,
                        _ => "session recomputation unavailable; dirty keys retained",
                    };
                    self.session_failed = true;
                    self.session_work = None;
                    sqlx::query("UPDATE derivation.session_progress SET failure=$1 WHERE one")
                        .bind(reason)
                        .execute(&mut self.connection)
                        .await?;
                }
            }
        }
        if !self.shell_failed {
            match self.shell_step(queued_commands).await {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    // Only static validation reasons are safe to publish;
                    // database diagnostics may contain captured values.
                    let reason = match error {
                        Error::Invalid(reason) => format!(
                            "shell recomputation unavailable: {reason}; dirty work retained"
                        ),
                        _ => "shell recomputation unavailable; dirty work retained".into(),
                    };
                    self.shell_failed = true;
                    self.shell_work = None;
                    sqlx::query("UPDATE derivation.shell_progress SET failure=$1 WHERE one")
                        .bind(reason)
                        .execute(&mut self.connection)
                        .await?;
                }
            }
        }
        if self.replay_rules.is_some()
            && let Some(request) = self.next_replay().await?
        {
            self.replay(ReplayCommand::Start(request)).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn recover_blobs(&mut self) -> Result<()> {
        use sqlx::Row;
        let rows = sqlx::query(WRITING_BLOBS)
            .bind(&self.root)
            .fetch_all(&mut self.connection)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let hash = parse_hash(&row.try_get::<String, _>("blake3")?)?;
            let store = self.blobs.clone();
            match tokio::task::spawn_blocking(move || store.verify(&hash)).await?? {
                hashed_blobs::Verified::Intact => {
                    sqlx::query(BLOB_PRESENT)
                        .bind(id)
                        .execute(&mut self.connection)
                        .await?;
                }
                hashed_blobs::Verified::Missing => { /* tracked writing row remains retryable; never called present */
                }
                _ => {
                    return Err(Error::Invalid(
                        "tracked incomplete blob failed integrity verification",
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "product"))]
    async fn admit(&mut self, _event: MaterialEvent) -> Result<Receipt> {
        Err(Error::ProductUnavailable)
    }

    #[cfg(feature = "product")]
    async fn admit(&mut self, event: MaterialEvent) -> Result<Receipt> {
        if event.bytes.len() > 1024 * 1024 {
            return Err(Error::Invalid("synthetic proof material exceeds 1 MiB"));
        }
        if event.bytes.is_empty()
            || event.anchor_byte < 0
            || event.anchor_byte as usize >= event.bytes.len()
            || event.ordinal < 0
        {
            return Err(Error::Invalid(
                "anchor must address retained bytes and ordinal must be nonnegative",
            ));
        }
        if !dotted(&event.source)
            || event.event_type != "synthetic.observed"
            || !event.payload.is_object()
        {
            return Err(Error::Invalid(
                "dotted source/type and object payload required",
            ));
        }
        let length = event.bytes.len() as i64;
        let record_hash = blake3::hash(&event.bytes).as_bytes().to_vec();
        let hash = hashed_blobs::Hash::of(&event.bytes);
        let blob_name = hash.to_string();
        // Row-first custody: an interrupted file write has an explicit tracked owner.
        let new_blob_id = Uuid::now_v7();
        sqlx::query!("INSERT INTO core.blobs (id,blake3,size_bytes,root,status) VALUES ($1,$2,$3,$4,'writing') ON CONFLICT (blake3) DO NOTHING",new_blob_id,blob_name,length,self.root).execute(&mut self.connection).await?;
        let row = sqlx::query!(
            "SELECT id,size_bytes,root FROM core.blobs WHERE blake3=$1",
            blob_name
        )
        .fetch_one(&mut self.connection)
        .await?;
        let blob_id = row.id;
        if row.size_bytes != length || row.root != self.root {
            return Err(Error::Invalid("existing blob registry identity differs"));
        }
        let store = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            store.put(event.bytes.as_slice(), 3)?;
            if store.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid(
                    "stored material failed integrity verification",
                ));
            }
            Ok(())
        })
        .await??;
        let event_id = Uuid::now_v7();
        let resolved_time = sinex_model::events::TsOrig::unknown()
            .project(
                sinex_model::events::EventId::try_from(event_id)
                    .map_err(|e| Error::Invalid(e.0))?,
                sinex_model::events::CaptureClock::Sinex,
            )
            .map_err(|e| Error::Invalid(e.0))?;
        let original = resolved_time.at.map(|at| at.to_string());
        let until = resolved_time.until.map(|at| at.to_string());
        let material_id = Uuid::now_v7();
        let mut tx = self.connection.begin().await?;
        // UPDATE locks the counter row until commit, assigning visibility order.
        let seq =
            sqlx::query_scalar!("UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq")
                .fetch_one(&mut *tx)
                .await?;
        sqlx::query!(
            "UPDATE core.blobs SET status='present' WHERE id=$1",
            blob_id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!("INSERT INTO raw.source_material_registry (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes) VALUES ($1,'file',$2,'completed','unknown',$3,$4,$4)",material_id,event.source,blob_id,length).execute(&mut *tx).await?;
        sqlx::query!("INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_quality,commit_seq,source_material_id,anchor_byte,ordinal,record_hash,run_id,ts_precision,ts_orig_until,ts_clock,time_suspect) VALUES ($1,$2,$3,$4,$11::text::timestamptz,$12,$5,$6,$7,$8,$9,$10,$13,$14::text::timestamptz,$15,$16)",event_id,event.source,event.event_type,event.payload,seq,material_id,event.anchor_byte,event.ordinal,record_hash,self.run_id,original,resolved_time.quality,resolved_time.precision,until,resolved_time.clock.label(),resolved_time.suspect).execute(&mut *tx).await?;
        let coordinate = coordinate_hash(
            material_id,
            event.anchor_byte,
            &event.event_type,
            event.ordinal,
        );
        sqlx::query!(
            "INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)",
            coordinate,
            event_id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!("INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) VALUES ($1,$2,0,0,1)",seq,event_id).execute(&mut *tx).await?;
        let position = serde_json::json!({"material":material_id,"byte_offset":length});
        sqlx::query!("INSERT INTO raw.source_cursors (binding,input,position,carry) VALUES ($1,'synthetic-proof',$2,'{}'::jsonb) ON CONFLICT (binding,input) DO UPDATE SET position=EXCLUDED.position,carry=EXCLUDED.carry,updated_at=now()",event.source,position).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Receipt {
            event_id,
            material_id,
            commit_seq: seq,
            blob_name,
        })
    }
}

fn coordinate_hash(material: Uuid, anchor: i64, event_type: &str, ordinal: i16) -> Vec<u8> {
    let mut hash = blake3::Hasher::new();
    hash.update(material.as_bytes());
    hash.update(&anchor.to_be_bytes());
    hash.update(&(event_type.len() as u64).to_be_bytes());
    hash.update(event_type.as_bytes());
    hash.update(&ordinal.to_be_bytes());
    hash.finalize().as_bytes()[..16].to_vec()
}

#[cfg(any(feature = "product", test))]
fn dotted(s: &str) -> bool {
    s.len() <= 128
        && s.contains('.')
        && s.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-')
        })
}

#[cfg(test)]
mod session_retry_tests;

#[cfg(test)]
mod tests {
    use super::dotted;
    #[test]
    fn rejects_empty_source_segments() {
        assert!(!dotted("synthetic..case"));
        assert!(dotted("synthetic.case"));
    }
}
