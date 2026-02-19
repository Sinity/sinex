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
                                                                                             // Reserved for future periodic disk space monitoring task
const _DISK_SPACE_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);
const DEFAULT_DISK_THRESHOLD_PERCENT: u8 = 90;

use async_nats::{jetstream, Client as NatsClient};
use blake3::Hasher;
use dashmap::DashMap;
use pipeline::MaterialConsumerHandles;
use sinex_db::{DbPool, DbPoolExt};
use sinex_node_sdk::annex::GitAnnex;
use sinex_node_sdk::SelfObserver;
use sinex_primitives::Timestamp;
use sinex_primitives::{environment::SinexEnvironment, Id, JsonValue, Ulid};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{fs, fs::File, sync::Mutex};
use tracing::{debug, info, warn};

/// Assembly statistics for observability
#[derive(Debug, Default)]
struct AssemblyStats {
    started: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
    disk_backpressure: AtomicU64,
}

impl AssemblyStats {
    fn inc_started(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_timed_out(&self) {
        self.timed_out.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_disk_backpressure(&self) {
        self.disk_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> AssemblyStatsSnapshot {
        AssemblyStatsSnapshot {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            disk_backpressure: self.disk_backpressure.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of assembly stats for metric emission.
struct AssemblyStatsSnapshot {
    started: u64,
    completed: u64,
    failed: u64,
    timed_out: u64,
    disk_backpressure: u64,
}

use crate::{material_ready_set::MaterialReadySet, IngestdResult, SinexError};
use state::{
    is_terminal_status, AssemblerState, FinalizationState, MaterialEndMessage, DLQ_CONSUMER,
    TEMP_FILE_NAME,
};

/// Disk space monitor for backpressure
struct DiskSpaceMonitor {
    state_root: PathBuf,
    threshold_percent: u8,
    last_check: std::sync::Mutex<std::time::Instant>,
    last_result: std::sync::Mutex<Option<bool>>,
}

impl DiskSpaceMonitor {
    fn new(state_root: PathBuf, threshold_percent: u8) -> Self {
        Self {
            state_root,
            threshold_percent,
            last_check: std::sync::Mutex::new(std::time::Instant::now()),
            last_result: std::sync::Mutex::new(None),
        }
    }

    /// Check if disk space is available (returns false if over threshold)
    #[allow(clippy::unwrap_used)] // Mutex poisoning is unrecoverable; unwrap is the standard pattern
    fn check_available(&self) -> bool {
        let now = std::time::Instant::now();
        let mut last_check = self.last_check.lock().unwrap();

        // Cache check results for 30 seconds to avoid excessive syscalls
        if now.duration_since(*last_check) < std::time::Duration::from_secs(30) {
            if let Some(result) = *self.last_result.lock().unwrap() {
                return result;
            }
        }

        let available = self.check_disk_space_internal();
        *last_check = now;
        *self.last_result.lock().unwrap() = Some(available);
        available
    }

    fn check_disk_space_internal(&self) -> bool {
        // Use statvfs to check disk usage
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = match CString::new(self.state_root.as_os_str().as_bytes()) {
            Ok(p) => p,
            Err(_) => {
                warn!("Failed to convert path to CString for disk space check");
                return true; // Fail open
            }
        };

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::statvfs(path_cstr.as_ptr(), &mut stat) };

        if result != 0 {
            warn!("statvfs failed for disk space check");
            return true; // Fail open
        }

        let total_blocks = stat.f_blocks;
        let available_blocks = stat.f_bavail;

        if total_blocks == 0 {
            return true; // Fail open
        }

        let used_percent = ((total_blocks - available_blocks) * 100) / total_blocks;
        used_percent < self.threshold_percent as u64
    }
}

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
/// - Semaphore limits concurrent assemblies (configurable, default 50) to prevent memory exhaustion.
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
    ready_set: Option<MaterialReadySet>,
    /// Self-observer for emitting assembly metrics
    observer: Option<Arc<SelfObserver>>,
    /// Assembly statistics for observability
    stats: Arc<AssemblyStats>,
    /// Disk space monitor for backpressure
    disk_monitor: Arc<DiskSpaceMonitor>,
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
        max_concurrent_assemblies: usize,
        ready_set: Option<MaterialReadySet>,
    ) -> IngestdResult<Self> {
        if let Err(e) = std::fs::create_dir_all(&state_root) {
            return Err(SinexError::io(format!(
                "Failed to create assembler state directory {}",
                state_root.display()
            ))
            .with_source(e));
        }

        let js = jetstream::new(nats_client.clone());
        let env = sinex_primitives::environment();

        let dlq_subject = env.nats_subject_with_namespace(
            namespace.as_deref(),
            &format!("events.dlq.{DLQ_CONSUMER}"),
        );

        // Cap concurrent assemblies to prevent memory exhaustion
        let active_assemblies = Arc::new(tokio::sync::Semaphore::new(max_concurrent_assemblies));

        // Initialize disk space monitor with threshold from env var
        let disk_threshold = std::env::var("SINEX_INGESTD_DISK_THRESHOLD_PERCENT")
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .unwrap_or(DEFAULT_DISK_THRESHOLD_PERCENT);
        let disk_monitor = Arc::new(DiskSpaceMonitor::new(state_root.clone(), disk_threshold));

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
            ready_set,
            observer: None,
            stats: Arc::new(AssemblyStats::default()),
            disk_monitor,
        })
    }

    /// Set self-observer for emitting assembly metrics
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<SelfObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Increment the "started" stats counter when a new assembly begins
    pub(super) fn stats_inc_started(&self) {
        self.stats.inc_started();
        let active = self.assembler_state.len() as u64;
        tracing::info!(
            target: "sinex_metrics",
            metric = "assembly_started",
            active_assemblies = active,
            total_started = self.stats.started.load(Ordering::Relaxed),
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            tokio::spawn(async move {
                let _ = observer
                    .emit_counter("sinex_assembly_started_total", 1, None)
                    .await;
            });
        }
    }

    /// Increment the "completed" stats counter when assembly succeeds
    pub(super) fn stats_inc_completed(&self, duration_secs: f64, bytes: u64) {
        self.stats.inc_completed();
        tracing::info!(
            target: "sinex_metrics",
            metric = "assembly_completed",
            total_completed = self.stats.completed.load(Ordering::Relaxed),
            active_assemblies = self.assembler_state.len() as u64,
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            tokio::spawn(async move {
                let _ = observer
                    .emit_counter("sinex_assembly_completed_total", 1, None)
                    .await;
                let _ = observer
                    .emit_counter("sinex_assembly_bytes_total", bytes, None)
                    .await;
                // Emit histogram for duration
                let _ = observer
                    .emit_histogram(
                        "sinex_assembly_duration_seconds",
                        1,             // count
                        duration_secs, // sum
                        duration_secs, // min
                        duration_secs, // max
                        None,
                        None,
                    )
                    .await;
            });
        }
    }

    /// Increment the "failed" stats counter when assembly fails
    pub(super) fn stats_inc_failed(&self) {
        self.stats.inc_failed();
        tracing::warn!(
            target: "sinex_metrics",
            metric = "assembly_failed",
            total_failed = self.stats.failed.load(Ordering::Relaxed),
            active_assemblies = self.assembler_state.len() as u64,
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            tokio::spawn(async move {
                let _ = observer
                    .emit_counter("sinex_assembly_failed_total", 1, None)
                    .await;
            });
        }
    }

    /// Increment the "timed_out" stats counter when assembly times out
    fn stats_inc_timed_out(&self) {
        self.stats.inc_timed_out();
        tracing::warn!(
            target: "sinex_metrics",
            metric = "assembly_timed_out",
            total_timed_out = self.stats.timed_out.load(Ordering::Relaxed),
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            tokio::spawn(async move {
                let _ = observer
                    .emit_counter("sinex_assembly_timed_out_total", 1, None)
                    .await;
            });
        }
    }

    async fn material_is_terminal(&self, material_id: Ulid) -> IngestdResult<bool> {
        let record = self
            .pool
            .source_materials()
            .get_by_id(Id::from_ulid(material_id))
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to fetch source material {material_id}"))
                    .with_source(e)
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
        // Check disk space before creating new assembly
        if !self.disk_monitor.check_available() {
            self.stats.inc_disk_backpressure();
            tracing::warn!(
                target: "sinex_metrics",
                metric = "assembly_disk_backpressure",
                threshold_percent = self.disk_monitor.threshold_percent,
                total_disk_backpressure = self.stats.disk_backpressure.load(Ordering::Relaxed),
            );

            if let Some(ref observer) = self.observer {
                let observer = observer.clone();
                tokio::spawn(async move {
                    let _ = observer
                        .emit_counter("sinex_assembly_disk_backpressure_total", 1, None)
                        .await;
                });
            }

            return Err(SinexError::service(format!(
                "Disk space above {}% threshold, rejecting new assembly",
                self.disk_monitor.threshold_percent
            )));
        }

        let state_dir = self.state_root.join(material_id.to_string());
        fs::create_dir_all(&state_dir)
            .await
            .map_err(|e| SinexError::io("Failed to create assembler state dir").with_source(e))?;

        let temp_path = state_dir.join(TEMP_FILE_NAME);
        // Important: placeholder creation can race across async tasks (e.g. slices + end arriving
        // "first" on different consumers). Never truncate an existing temp file here, otherwise we
        // can wipe already-written slice bytes while keeping the in-memory counters.
        let temp_file = File::options()
            .create(true)
            .append(true)
            .open(&temp_path)
            .await
            .map_err(|e| SinexError::io("Failed to open temp file").with_source(e))?;

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
            wal_seq: 0,
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
                SinexError::database(format!("Failed to register source material {material_id}"))
                    .with_source(e)
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
            ready_set: self.ready_set.clone(),
            observer: self.observer.clone(),
            stats: self.stats.clone(),
            disk_monitor: self.disk_monitor.clone(),
        }
    }

    /// Run the assembler service
    ///
    /// # Observability
    ///
    /// Assembly metrics are emitted via structured tracing with `target: "sinex_metrics"`:
    /// - `assembly_started` / `assembly_completed` / `assembly_failed` — lifecycle events
    /// - `assembly_completed` with `duration_ms`, `slice_count`, `size_bytes` — per-material detail
    /// - `assembly_failure` with `failure_reason` — categorized failure tracking
    /// - `assembly_periodic_stats` — periodic gauge of active assemblies, buffer utilization
    /// - `assembly_timed_out` — timeout counter
    /// - `assembly_disk_backpressure` — disk threshold rejections
    /// - `wal_replay_completed` with `duration_ms`, `restored_assemblies` — startup metrics
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

        let wal_replay_start = std::time::Instant::now();
        io::restore_state(&self).await?;
        let wal_replay_duration = wal_replay_start.elapsed();
        let restored_count = self.assembler_state.len() as u64;
        tracing::info!(
            target: "sinex_metrics",
            metric = "wal_replay_completed",
            duration_ms = wal_replay_duration.as_millis() as u64,
            restored_assemblies = restored_count,
        );

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
                Self::handle_task_exit("material begin consumer", result, &shutdown_flag)
            }
            result = &mut consumers.slices => {
                Self::handle_task_exit("material slice consumer", result, &shutdown_flag)
            }
            result = &mut consumers.end => {
                Self::handle_task_exit("material end consumer", result, &shutdown_flag)
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

            // Compute buffer utilization metrics
            let active = self.assembler_state.len() as u32;
            let buffered_slices: u32 = self
                .assembler_state
                .iter()
                .map(|e| {
                    // Try to get count without blocking - use try_lock
                    e.value()
                        .try_lock()
                        .map_or(0, |s| s.buffered_slices.len() as u32)
                })
                .sum();

            let stats = self.stats.snapshot();

            // Emit periodic metrics via structured tracing
            tracing::info!(
                target: "sinex_metrics",
                metric = "assembly_periodic_stats",
                active_assemblies = active,
                total_started = stats.started,
                total_completed = stats.completed,
                total_failed = stats.failed,
                total_timed_out = stats.timed_out,
                total_disk_backpressure = stats.disk_backpressure,
                buffered_slices = buffered_slices,
            );

            // Emit assembly stats via self-observer
            if let Some(ref observer) = self.observer {
                if let Err(e) = observer
                    .emit_assembly_stats(
                        active,
                        stats.started,
                        stats.completed,
                        stats.failed,
                        stats.timed_out,
                        None, // avg_duration_ms - would need tracking
                        buffered_slices,
                    )
                    .await
                {
                    debug!("Failed to emit assembly stats: {}", e);
                }
            }

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
            if elapsed.whole_seconds() > SLICE_ARRIVAL_TIMEOUT.as_secs() as i64
                && (state.pending_end.is_none() || !state.buffered_slices.is_empty())
            {
                stale.push((material_id, elapsed.whole_seconds()));
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

        self.stats_inc_timed_out();

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
            .map_err(|e| SinexError::io("Failed to iterate state directory").with_source(e))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| SinexError::io("Failed to check file type").with_source(e))?
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
