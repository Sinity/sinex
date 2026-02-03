//! Material Assembler for consuming material slices from NATS `JetStream`.
//!
//! The assembler is responsible for rebuilding source material streams from
//! begin/slice/end messages, persisting the assembled material into git-annex,
//! registering blobs in Postgres, updating the source material registry and
//! temporal ledger, and routing failures to the DLQ. State is persisted on disk
//! so that in-flight assemblies can survive process restarts.

mod finalize;
mod io;
mod pipeline;
mod state;

const MAX_BUFFERED_SLICES: usize = 100;
const SLICE_ARRIVAL_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(5); // 5 minutes
const STALE_ASSEMBLY_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1); // 1 minute
const ORPHANED_FILE_AGE_THRESHOLD: std::time::Duration = std::time::Duration::from_hours(1); // 1 hour

use async_nats::{jetstream, Client as NatsClient};
use blake3::Hasher;
use dashmap::DashMap;
use pipeline::MaterialConsumerHandles;
use sinex_db::{DbPool, DbPoolExt};
use sinex_node_sdk::annex::GitAnnex;
use sinex_primitives::Timestamp;
use sinex_primitives::{environment::SinexEnvironment, Id, JsonValue, Ulid};
use std::{collections::BTreeMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{fs, fs::File, sync::Mutex};
use tracing::{info, warn};

use crate::{IngestdResult, SinexError};
use state::{
    is_terminal_status, AssemblerState, FinalizationState, MaterialEndMessage, DLQ_CONSUMER,
    TEMP_FILE_NAME,
};

/// Material assembler service
///
/// # Lock Contention & Concurrency Model
///
/// The assembler uses a per-material isolation strategy to eliminate global lock contention:
///
/// - `assembler_state: Arc<DashMap<Ulid, Arc<Mutex<AssemblerState>>>>` provides independent
///   locking for each material. Materials do not block each other.
/// - Each material's Mutex lock is held only for state snapshots (~1ms), never during slow I/O.
/// - Lock-free reads via `DashMap::get()` for handle retrieval (~100ns).
/// - Semaphore limits concurrent assemblies to 50 to prevent memory exhaustion.
///
/// Critical fix applied in commit c799300cd:
/// - Locks are explicitly dropped before git-annex imports and database writes.
/// - This prevents blocking other slice handlers on the same material.
///
/// For detailed analysis, see `docs/current/analysis/lock-contention-analysis.md`
pub struct MaterialAssembler {
    js: jetstream::Context,
    nats_client: NatsClient,
    pool: DbPool,
    env: SinexEnvironment,
    namespace: Option<String>,
    annex: Arc<GitAnnex>,
    assembler_state: Arc<DashMap<Ulid, Arc<Mutex<AssemblerState>>>>,
    state_root: PathBuf,
    dlq_subject: String,
    slices_max_ack_pending: i64,
    active_assemblies: Arc<tokio::sync::Semaphore>,
}

impl MaterialAssembler {
    /// Create a new material assembler
    pub fn new(
        nats_client: NatsClient,
        pool: DbPool,
        annex: Arc<GitAnnex>,
        state_root: PathBuf,
        namespace: Option<String>,
        slices_max_ack_pending: i64,
    ) -> IngestdResult<Self> {
        if let Err(e) = std::fs::create_dir_all(&state_root) {
            return Err(SinexError::io(format!(
                "Failed to create assembler state directory {}: {}",
                state_root.display(),
                e
            )));
        }

        let js = jetstream::new(nats_client.clone());
        let env = sinex_primitives::environment();

        let dlq_subject = env.nats_subject_with_namespace(
            namespace.as_deref(),
            &format!("events.dlq.{DLQ_CONSUMER}"),
        );

        // Cap concurrent assemblies to prevent memory exhaustion
        // TODO: Make this configurable
        let active_assemblies = Arc::new(tokio::sync::Semaphore::new(50));

        Ok(Self {
            js,
            nats_client,
            pool,
            env,
            namespace,
            annex,
            assembler_state: Arc::new(DashMap::new()),
            state_root,
            dlq_subject,
            slices_max_ack_pending,
            active_assemblies,
        })
    }

    async fn material_is_terminal(&self, material_id: Ulid) -> IngestdResult<bool> {
        let record = self
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_id))
            .await
            .map_err(|e| {
                SinexError::database(format!(
                    "Failed to fetch source material {material_id}: {e}"
                ))
            })?;

        Ok(record.is_some_and(|record| is_terminal_status(record.status.as_str())))
    }

    /// Fetch a handle to an existing assembler state for a material.
    async fn get_state_handle(&self, material_id: &Ulid) -> Option<Arc<Mutex<AssemblerState>>> {
        self.assembler_state
            .get(material_id)
            .map(|entry| entry.value().clone())
    }

    /// Insert a new assembler state if one does not already exist.
    async fn insert_state_handle(
        &self,
        material_id: Ulid,
        state: AssemblerState,
    ) -> Arc<Mutex<AssemblerState>> {
        let state_handle = Arc::new(Mutex::new(state));

        match self.assembler_state.entry(material_id) {
            dashmap::mapref::entry::Entry::Occupied(existing) => existing.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(state_handle.clone());
                state_handle
            }
        }
    }

    /// Build a placeholder assembler state for materials whose slices arrive before the begin message.
    async fn create_placeholder_state(&self, material_id: Ulid) -> IngestdResult<AssemblerState> {
        let state_dir = self.state_root.join(material_id.to_string());
        fs::create_dir_all(&state_dir)
            .await
            .map_err(|e| SinexError::io(format!("Failed to create assembler state dir: {e}")))?;

        let temp_path = state_dir.join(TEMP_FILE_NAME);
        // Important: placeholder creation can race across async tasks (e.g. slices + end arriving
        // "first" on different consumers). Never truncate an existing temp file here, otherwise we
        // can wipe already-written slice bytes while keeping the in-memory counters.
        let temp_file = File::options()
            .create(true)
            .append(true)
            .open(&temp_path)
            .await
            .map_err(|e| SinexError::io(format!("Failed to open temp file: {e}")))?;

        // Limit concurrent assemblies
        let permit = self
            .active_assemblies
            .clone()
            .try_acquire_owned()
            .map_err(|_| SinexError::service("Too many active assemblies (semaphore exhausted)"))?;

        Ok(AssemblerState {
            material_id,
            temp_path,
            temp_file: Some(temp_file),
            wal_file: None,
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            state_dir,
            started_at: Timestamp::now(),
            material_kind: String::new(),
            source_identifier: String::new(),
            metadata: serde_json::json!({}),
            has_begin: false,
            hasher: Hasher::new(),
            pending_write: None,
            pending_end: None,
            finalizing: false,
            last_slice_received: Timestamp::now(),
            _permit: Some(permit),
        })
    }

    /// Handle a begin message
    async fn handle_begin(&self, msg: jetstream::Message) -> IngestdResult<()> {
        state::handle_begin(self, msg).await
    }

    /// Handle a material slice message
    async fn handle_slice(
        &self,
        material_id: Ulid,
        offset: i64,
        data: Vec<u8>,
    ) -> IngestdResult<()> {
        io::handle_slice(self, material_id, offset, data).await
    }

    /// Remove the persisted state directory for a material
    async fn cleanup_state(&self, material_id: Ulid) {
        io::cleanup_state(self, material_id).await;
    }

    /// Import the assembled material into git-annex
    async fn import_into_annex(
        &self,
        state: &FinalizationState,
    ) -> IngestdResult<(sinex_node_sdk::annex::AnnexKey, std::path::PathBuf)> {
        io::import_into_annex(self, state).await
    }

    async fn register_material_record(
        &self,
        material_id: Ulid,
        material_kind: &str,
        source_identifier: &str,
        metadata: JsonValue,
        started_at: Timestamp,
    ) -> IngestdResult<()> {
        self.pool
            .source_materials()
            .register_external_in_flight(
                material_id,
                material_kind,
                Some(source_identifier),
                metadata,
                started_at,
            )
            .await
            .map(|_| ())
            .map_err(|e| {
                SinexError::database(format!(
                    "Failed to register source material {material_id}: {e}"
                ))
            })
    }

    /// Helper for cloning into async tasks
    fn clone_for_task(&self) -> Self {
        Self {
            js: self.js.clone(),
            nats_client: self.nats_client.clone(),
            pool: self.pool.clone(),
            env: self.env.clone(),
            namespace: self.namespace.clone(),
            annex: self.annex.clone(),
            assembler_state: self.assembler_state.clone(),
            state_root: self.state_root.clone(),
            dlq_subject: self.dlq_subject.clone(),
            slices_max_ack_pending: self.slices_max_ack_pending,
            active_assemblies: self.active_assemblies.clone(),
        }
    }

    /// Run the assembler service
    ///
    /// # Observability
    ///
    /// TODO: Add comprehensive assembly metrics for production observability:
    /// - Assembly duration histogram (time from begin to end)
    /// - Active assembly count gauge
    /// - Slice count per material histogram
    /// - Assembly failure counter by reason (`hash_mismatch`, corruption, timeout, etc.)
    /// - Buffer utilization histogram (peak buffered slices per material)
    /// - WAL replay duration on startup
    pub async fn run(self) -> IngestdResult<()> {
        self.run_with_shutdown(Arc::new(std::sync::atomic::AtomicBool::new(false)))
            .await
    }

    /// Run the assembler service with a shared shutdown flag.
    pub async fn run_with_shutdown(
        self,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> IngestdResult<()> {
        info!("Starting Material Assembler");

        pipeline::bootstrap_streams(&self).await?;
        io::restore_state(&self).await?;

        let mut consumers = MaterialConsumerHandles {
            begin: pipeline::spawn_begin_consumer(&self, shutdown_flag.clone()),
            slices: pipeline::spawn_slices_consumer(&self, shutdown_flag.clone()),
            end: pipeline::spawn_end_consumer(&self, shutdown_flag.clone()),
        };

        // Spawn stale assembly cleanup task
        let cleanup_task = {
            let assembler = self.clone_for_task();
            let shutdown = shutdown_flag.clone();
            tokio::spawn(async move { assembler.run_stale_assembly_cleanup(shutdown).await })
        };

        tokio::select! {
            result = &mut consumers.begin => {
                return Self::handle_task_exit("material begin consumer", result, &shutdown_flag);
            }
            result = &mut consumers.slices => {
                return Self::handle_task_exit("material slice consumer", result, &shutdown_flag);
            }
            result = &mut consumers.end => {
                return Self::handle_task_exit("material end consumer", result, &shutdown_flag);
            }
            result = cleanup_task => {
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(SinexError::service(format!("Cleanup task panicked: {e}"))),
                }
            }
        }
    }

    fn handle_task_exit(
        task_name: &str,
        result: Result<IngestdResult<()>, tokio::task::JoinError>,
        shutdown_flag: &Arc<std::sync::atomic::AtomicBool>,
    ) -> IngestdResult<()> {
        match result {
            Ok(Ok(())) if shutdown_flag.load(std::sync::atomic::Ordering::Relaxed) => Ok(()),
            Ok(Ok(())) => Err(SinexError::service(format!(
                "{task_name} exited without signalling shutdown"
            ))),
            Ok(Err(err)) => Err(err),
            Err(join_err) if join_err.is_cancelled() => {
                Err(SinexError::cancelled(format!("{task_name} was cancelled")))
            }
            Err(join_err) => Err(SinexError::service(format!(
                "{task_name} panicked: {join_err}"
            ))),
        }
    }

    /// Periodically check for stale assemblies and clean them up
    async fn run_stale_assembly_cleanup(
        &self,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> IngestdResult<()> {
        let mut interval = tokio::time::interval(STALE_ASSEMBLY_CHECK_INTERVAL);

        loop {
            if shutdown_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            interval.tick().await;

            let stale_materials = self.find_stale_materials().await;

            for (material_id, elapsed_secs) in stale_materials {
                self.process_stale_material(material_id, elapsed_secs).await;
            }

            if let Err(e) = self.cleanup_orphaned_temp_files().await {
                warn!("Failed to cleanup orphaned temp files: {}", e);
            }
        }

        Ok(())
    }

    async fn find_stale_materials(&self) -> Vec<(Ulid, i64)> {
        let now = Timestamp::now();
        let mut stale = Vec::new();

        for entry in self.assembler_state.iter() {
            let material_id = *entry.key();
            let state = entry.value().lock().await;

            if state.finalizing {
                continue;
            }

            let elapsed = now - state.last_slice_received;
            if elapsed.whole_seconds() > SLICE_ARRIVAL_TIMEOUT.as_secs() as i64 {
                if state.pending_end.is_none() || !state.buffered_slices.is_empty() {
                    stale.push((material_id, elapsed.whole_seconds()));
                }
            }
        }
        stale
    }

    async fn process_stale_material(&self, material_id: Ulid, elapsed_secs: i64) {
        info!(
            material_id = %material_id,
            elapsed_secs,
            "Cleaning up stale assembly due to slice arrival timeout"
        );

        self.route_material_error(
            material_id,
            "slice_arrival_timeout",
            serde_json::json!({
                "timeout_seconds": SLICE_ARRIVAL_TIMEOUT.as_secs(),
                "elapsed_seconds": elapsed_secs,
            }),
        )
        .await;

        self.finalize_failed_material(material_id, "slice_arrival_timeout")
            .await;
    }

    /// Scan state root for orphaned temp files from crashed/terminated assemblies
    async fn cleanup_orphaned_temp_files(&self) -> IngestdResult<()> {
        let mut entries = match fs::read_dir(&self.state_root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(SinexError::io(format!(
                    "Failed to read state root for cleanup: {err}"
                )))
            }
        };

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SinexError::io(format!("Failed to iterate state directory: {e}")))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| SinexError::io(format!("Failed to check file type: {e}")))?
                .is_dir()
            {
                self.check_orphaned_folder(entry.path()).await?;
            }
        }

        Ok(())
    }

    async fn check_orphaned_folder(&self, path: std::path::PathBuf) -> IngestdResult<()> {
        let folder_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();

        let Ok(material_id) = Ulid::from_str(folder_name) else {
            return Ok(()); // Skip non-ULID folders
        };

        // Check if this material is still active in memory
        if self.assembler_state.contains_key(&material_id) {
            return Ok(()); // Active, don't touch it
        }

        // Check if material is terminal in database
        if self.material_is_terminal(material_id).await? {
            // Terminal but not cleaned up - clean it now
            info!(
                material_id = %material_id,
                "Cleaning up orphaned state for terminal material"
            );
            self.cleanup_state(material_id).await;
            return Ok(());
        }

        // Check file age - only clean up if old enough
        let temp_path = path.join(state::TEMP_FILE_NAME);
        if temp_path.exists() {
            if let Ok(metadata) = fs::metadata(&temp_path).await {
                if let Ok(modified) = metadata.modified() {
                    let now = std::time::SystemTime::now();
                    if let Ok(age) = now.duration_since(modified) {
                        if age > ORPHANED_FILE_AGE_THRESHOLD {
                            warn!(
                                material_id = %material_id,
                                age_hours = age.as_secs() / 3600,
                                "Cleaning up very old orphaned temp file"
                            );
                            self.cleanup_state(material_id).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
