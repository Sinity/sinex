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

const STALE_ASSEMBLY_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1); // 1 minute
// Reserved for future periodic disk space monitoring task
const _DISK_SPACE_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

use async_nats::{Client as NatsClient, jetstream};
use blake3::Hasher;
use dashmap::DashMap;
use sinex_db::{DbPool, DbPoolExt};
use sinex_node_sdk::{SelfObservationError, SelfObserver};
use sinex_node_sdk::annex::GitAnnex;
use sinex_primitives::Timestamp;
use sinex_primitives::{Id, JsonValue, Uuid, environment::SinexEnvironment};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{
    fs,
    fs::File,
    sync::Mutex,
    task::{JoinHandle, JoinSet},
    time::Duration,
};
use tracing::{debug, info, warn};

fn signal_ready(ready_tx: Option<tokio::sync::oneshot::Sender<()>>, component: &str) -> bool {
    match ready_tx {
        Some(tx) => {
            if tx.send(()).is_err() {
                warn!(component, "Readiness receiver dropped before ready signal");
                false
            } else {
                true
            }
        }
        None => true,
    }
}

type MaterialTaskOutcome = (&'static str, Result<IngestdResult<()>, tokio::task::JoinError>);

fn material_task_cleanup_failure(name: &'static str, error: &SinexError) -> SinexError {
    crate::service::task_shutdown_error("material", name, error)
}

fn material_task_join_failure(
    name: &'static str,
    error: &tokio::task::JoinError,
) -> SinexError {
    crate::service::task_shutdown_error("material", name, error)
}

fn material_task_monitor_failure(error: &tokio::task::JoinError) -> SinexError {
    crate::service::task_shutdown_error("material", "monitor", error)
}

fn material_task_timeout(count: usize, timeout: Duration) -> SinexError {
    SinexError::service(format!(
        "timed out waiting for {count} material tasks during shutdown"
    ))
    .with_context("timeout_secs", timeout.as_secs().to_string())
}

/// Assembly statistics for observability
#[derive(Debug, Default)]
struct AssemblyStats {
    started: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
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

    fn inc_cancelled(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
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
            cancelled: self.cancelled.load(Ordering::Relaxed),
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
    cancelled: u64,
    failed: u64,
    timed_out: u64,
    disk_backpressure: u64,
}

use crate::{IngestdResult, SinexError, material_ready_set::MaterialReadySet};
use state::{
    AssemblerState, AssemblyPhase, DLQ_CONSUMER, FinalizationState, MaterialEndMessage,
    TEMP_FILE_NAME, is_terminal_status,
};

/// Disk space monitor for backpressure
struct DiskSpaceMonitor {
    state_root: PathBuf,
    threshold_percent: u8,
    last_check: parking_lot::Mutex<std::time::Instant>,
    last_result: parking_lot::Mutex<Option<bool>>,
}

impl DiskSpaceMonitor {
    fn new(state_root: PathBuf, threshold_percent: u8) -> Self {
        Self {
            state_root,
            threshold_percent,
            last_check: parking_lot::Mutex::new(std::time::Instant::now()),
            last_result: parking_lot::Mutex::new(None),
        }
    }

    /// Check if disk space is available (returns false if over threshold)
    fn check_available(&self) -> bool {
        let now = std::time::Instant::now();
        let mut last_check = self.last_check.lock();

        // Cache check results for 30 seconds to avoid excessive syscalls
        if now.duration_since(*last_check) < std::time::Duration::from_secs(30)
            && let Some(result) = *self.last_result.lock()
        {
            return result;
        }

        let available = self.check_disk_space_internal();
        *last_check = now;
        *self.last_result.lock() = Some(available);
        available
    }

    fn check_disk_space_internal(&self) -> bool {
        // Use statvfs to check disk usage
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path_cstr = if let Ok(p) = CString::new(self.state_root.as_os_str().as_bytes()) {
            p
        } else {
            warn!("Failed to convert path to CString for disk space check");
            return true; // Fail open
        };

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::statvfs(path_cstr.as_ptr(), &raw mut stat) };

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
        used_percent < u64::from(self.threshold_percent)
    }
}

/// Material assembler service.
///
/// Concurrency contract:
/// - `assembler_state` gives each material its own mutable state handle
/// - one material may serialize on its own `Mutex`, but unrelated materials must not
/// - the per-material lock is for state mutation and snapshots only
/// - filesystem, git-annex, and database work must run after dropping that lock
pub struct MaterialAssembler {
    js: jetstream::Context,
    nats_client: NatsClient,
    pool: DbPool,
    env: SinexEnvironment,
    namespace: Option<String>,
    annex: Arc<GitAnnex>,
    assembler_state: Arc<DashMap<Uuid, Arc<Mutex<AssemblerState>>>>,
    state_root: PathBuf,
    dlq_subject: String,
    slices_max_ack_pending: i64,
    ready_set: Option<MaterialReadySet>,
    /// Self-observer for emitting assembly metrics
    observer: Option<Arc<SelfObserver>>,
    /// Assembly statistics for observability
    stats: Arc<AssemblyStats>,
    /// Disk space monitor for backpressure
    disk_monitor: Arc<DiskSpaceMonitor>,
    /// Maximum out-of-order slices to buffer per assembly (from config)
    pub(super) max_buffered_slices: usize,
    /// Maximum bytes one material may accumulate before being failed.
    pub(super) max_material_size_bytes: i64,
    /// Timeout before an in-flight assembly is considered stale (from config)
    pub(super) slice_arrival_timeout: std::time::Duration,
    /// Age threshold for orphaned temp files (from config)
    pub(super) orphaned_file_age_threshold: std::time::Duration,
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
        ready_set: Option<MaterialReadySet>,
        max_buffered_slices: usize,
        max_material_size_bytes: u64,
        slice_timeout_secs: u64,
        orphan_threshold_secs: u64,
        disk_threshold_percent: u8,
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

        let disk_monitor = Arc::new(DiskSpaceMonitor::new(
            state_root.clone(),
            disk_threshold_percent,
        ));
        let max_material_size_bytes = encode_max_material_size_bytes(max_material_size_bytes)?;

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
            ready_set,
            observer: None,
            stats: Arc::new(AssemblyStats::default()),
            disk_monitor,
            max_buffered_slices,
            max_material_size_bytes,
            slice_arrival_timeout: std::time::Duration::from_secs(slice_timeout_secs),
            orphaned_file_age_threshold: std::time::Duration::from_secs(orphan_threshold_secs),
        })
    }

    /// Set self-observer for emitting assembly metrics
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<SelfObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    fn spawn_observer_emit<F>(&self, metric: &'static str, future: F)
    where
        F: Future<Output = Result<(), SelfObservationError>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(error) = future.await {
                warn!(
                    metric,
                    error = %error,
                    "Failed to emit material assembly telemetry"
                );
            }
        });
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
            self.spawn_observer_emit(
                "sinex_assembly_started_total",
                async move { observer.emit_counter("sinex_assembly_started_total", 1, None).await },
            );
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
            self.spawn_observer_emit(
                "sinex_assembly_completed_total",
                async move {
                    observer
                        .emit_counter("sinex_assembly_completed_total", 1, None)
                        .await
                },
            );
        }

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_bytes_total",
                async move { observer.emit_counter("sinex_assembly_bytes_total", bytes, None).await },
            );
        }

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_duration_seconds",
                async move {
                    observer
                        .emit_histogram(
                            "sinex_assembly_duration_seconds",
                            1,
                            duration_secs,
                            duration_secs,
                            duration_secs,
                            None,
                            None,
                        )
                        .await
                },
            );
        }
    }

    /// Increment the "cancelled" stats counter when assembly is ended intentionally.
    pub(super) fn stats_inc_cancelled(&self, duration_secs: f64, bytes: u64) {
        self.stats.inc_cancelled();
        tracing::info!(
            target: "sinex_metrics",
            metric = "assembly_cancelled",
            total_cancelled = self.stats.cancelled.load(Ordering::Relaxed),
            active_assemblies = self.assembler_state.len() as u64,
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_cancelled_total",
                async move {
                    observer
                        .emit_counter("sinex_assembly_cancelled_total", 1, None)
                        .await
                },
            );
        }

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_bytes_total",
                async move { observer.emit_counter("sinex_assembly_bytes_total", bytes, None).await },
            );
        }

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_duration_seconds",
                async move {
                    observer
                        .emit_histogram(
                            "sinex_assembly_duration_seconds",
                            1,
                            duration_secs,
                            duration_secs,
                            duration_secs,
                            None,
                            None,
                        )
                        .await
                },
            );
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
            self.spawn_observer_emit(
                "sinex_assembly_failed_total",
                async move { observer.emit_counter("sinex_assembly_failed_total", 1, None).await },
            );
        }
    }

    /// Increment the "`timed_out`" stats counter when assembly times out
    fn stats_inc_timed_out(&self) {
        self.stats.inc_timed_out();
        tracing::warn!(
            target: "sinex_metrics",
            metric = "assembly_timed_out",
            total_timed_out = self.stats.timed_out.load(Ordering::Relaxed),
        );

        if let Some(ref observer) = self.observer {
            let observer = observer.clone();
            self.spawn_observer_emit(
                "sinex_assembly_timed_out_total",
                async move { observer.emit_counter("sinex_assembly_timed_out_total", 1, None).await },
            );
        }
    }

    async fn material_is_terminal(&self, material_id: Uuid) -> IngestdResult<bool> {
        let record = self
            .pool
            .source_materials()
            .get_by_id(Id::from_uuid(material_id))
            .await
            .map_err(|e| {
                SinexError::database(format!("Failed to fetch source material {material_id}"))
                    .with_source(e)
            })?;

        Ok(record.is_some_and(|record| is_terminal_status(record.status.as_str())))
    }

    /// Fetch a handle to an existing assembler state for a material.
    async fn get_state_handle(&self, material_id: &Uuid) -> Option<Arc<Mutex<AssemblerState>>> {
        self.assembler_state
            .get(material_id)
            .map(|entry| entry.value().clone())
    }

    /// Insert a new assembler state if one does not already exist.
    async fn insert_state_handle(
        &self,
        material_id: Uuid,
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
    async fn create_placeholder_state(&self, material_id: Uuid) -> IngestdResult<AssemblerState> {
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
                self.spawn_observer_emit(
                    "sinex_assembly_disk_backpressure_total",
                    async move {
                        observer
                            .emit_counter("sinex_assembly_disk_backpressure_total", 1, None)
                            .await
                    },
                );
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

        Ok(AssemblerState {
            material_id,
            temp_path,
            temp_file: Some(temp_file),
            wal_file: None,
            wal_seq: 0,
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            buffered_bytes: 0,
            state_dir,
            started_at: Timestamp::now(),
            material_kind: String::new(),
            source_identifier: String::new(),
            metadata: serde_json::json!({}),
            phase: AssemblyPhase::PendingBegin,
            hasher: Hasher::new(),
            pending_write: None,
            pending_end: None,
            last_slice_received: Timestamp::now(),
        })
    }

    /// Handle a begin message
    async fn handle_begin(
        &self,
        material_id: Uuid,
        begin: state::MaterialBeginMessage,
    ) -> IngestdResult<()> {
        state::handle_begin(self, material_id, begin).await
    }

    /// Handle a material slice message
    async fn handle_slice(
        &self,
        material_id: Uuid,
        offset: i64,
        data: Vec<u8>,
    ) -> IngestdResult<()> {
        io::handle_slice(self, material_id, offset, data).await
    }

    /// Remove the persisted state directory for a material
    async fn cleanup_state(&self, material_id: Uuid) {
        io::cleanup_state(self, material_id).await;
    }

    /// Import the assembled material into git-annex
    async fn import_into_annex(
        &self,
        state: &FinalizationState,
    ) -> IngestdResult<sinex_node_sdk::annex::AnnexKey> {
        io::import_into_annex(self, state).await
    }

    async fn register_material_record(
        &self,
        material_id: Uuid,
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
            ready_set: self.ready_set.clone(),
            observer: self.observer.clone(),
            stats: self.stats.clone(),
            disk_monitor: self.disk_monitor.clone(),
            max_buffered_slices: self.max_buffered_slices,
            max_material_size_bytes: self.max_material_size_bytes,
            slice_arrival_timeout: self.slice_arrival_timeout,
            orphaned_file_age_threshold: self.orphaned_file_age_threshold,
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
        self.run_with_shutdown_and_ready(shutdown_flag, None).await
    }

    /// Run the assembler, optionally signalling readiness after streams are bound
    /// and WAL state is restored. Callers can await the receiver before emitting
    /// `sd_notify(READY)` to ensure the assembler is actually ready to process slices.
    pub async fn run_with_shutdown_and_ready(
        self,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
        ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
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

        // Signal readiness: streams bootstrapped, WAL restored, consumers about to start.
        signal_ready(ready_tx, "material-assembler");

        let mut tasks = JoinSet::new();
        Self::track_material_task(
            &mut tasks,
            "material begin consumer",
            pipeline::spawn_begin_consumer(&self, shutdown_flag.clone()),
        );
        Self::track_material_task(
            &mut tasks,
            "material slice consumer",
            pipeline::spawn_slices_consumer(&self, shutdown_flag.clone()),
        );
        Self::track_material_task(
            &mut tasks,
            "material end consumer",
            pipeline::spawn_end_consumer(&self, shutdown_flag.clone()),
        );

        let cleanup_task = {
            let assembler = self.clone_for_task();
            let shutdown = shutdown_flag.clone();
            tokio::spawn(async move { assembler.run_stale_assembly_cleanup(shutdown).await })
        };
        Self::track_material_task(&mut tasks, "material stale cleanup task", cleanup_task);

        let result = match tasks.join_next().await {
            Some(Ok((name, result))) => Self::handle_task_exit(name, result, &shutdown_flag),
            Some(Err(error)) => Err(material_task_monitor_failure(&error)),
            None => Ok(()),
        };

        shutdown_flag.store(true, std::sync::atomic::Ordering::Release);
        let cleanup_error = Self::wait_for_material_tasks(&mut tasks, Duration::from_secs(5)).await;
        match (result, cleanup_error) {
            (Ok(()), Some(error)) => Err(error),
            (Err(error), _) => Err(error),
            (Ok(()), None) => Ok(()),
        }
    }

    fn track_material_task(
        tasks: &mut JoinSet<MaterialTaskOutcome>,
        name: &'static str,
        handle: JoinHandle<IngestdResult<()>>,
    ) {
        tasks.spawn(async move { (name, handle.await) });
    }

    async fn wait_for_material_tasks(
        tasks: &mut JoinSet<MaterialTaskOutcome>,
        timeout: Duration,
    ) -> Option<SinexError> {
        if tasks.is_empty() {
            return None;
        }

        info!("Waiting for {} material tasks to finish...", tasks.len());

        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut cleanup_error = None;
        let mut timed_out = false;

        loop {
            tokio::select! {
                maybe = tasks.join_next(), if !tasks.is_empty() => {
                    match maybe {
                        Some(Ok((name, Ok(Ok(()))))) => {
                            debug!(task = name, "Material task stopped cleanly");
                        }
                        Some(Ok((name, Ok(Err(error))))) => {
                            warn!(task = name, error = %error, "Material task exited with error during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(material_task_cleanup_failure(name, &error));
                            }
                        }
                        Some(Ok((name, Err(error)))) => {
                            warn!(task = name, error = ?error, "Material task join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(material_task_join_failure(name, &error));
                            }
                        }
                        Some(Err(error)) => {
                            warn!(error = ?error, "Material task monitor join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(material_task_monitor_failure(&error));
                            }
                        }
                        None => break,
                    }
                    if tasks.is_empty() {
                        break;
                    }
                }
                () = &mut deadline, if !timed_out => {
                    timed_out = true;
                    let remaining = tasks.len();
                    warn!(
                        "Timed out waiting for {} material tasks after {:?}, continuing to drain shutdown work",
                        remaining,
                        timeout
                    );
                    if cleanup_error.is_none() {
                        cleanup_error = Some(material_task_timeout(remaining, timeout));
                    }
                }
            }
        }

        info!("Material task cleanup complete");
        cleanup_error
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
                total_cancelled = stats.cancelled,
                total_failed = stats.failed,
                total_timed_out = stats.timed_out,
                total_disk_backpressure = stats.disk_backpressure,
                buffered_slices = buffered_slices,
            );

            // Emit assembly stats via self-observer
            if let Some(ref observer) = self.observer
                && let Err(e) = observer
                    .emit_assembly_stats(
                        active,
                        stats.started,
                        stats.completed,
                        stats.cancelled,
                        stats.failed,
                        stats.timed_out,
                        None, // avg_duration_ms - would need tracking
                        buffered_slices,
                    )
                    .await
            {
                debug!("Failed to emit assembly stats: {}", e);
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

    async fn find_stale_materials(&self) -> Vec<(Uuid, i64)> {
        let now = Timestamp::now();
        let mut stale = Vec::new();

        for entry in self.assembler_state.iter() {
            let material_id = *entry.key();
            let state = entry.value().lock().await;

            if state.phase == AssemblyPhase::Finalizing {
                continue;
            }

            let elapsed = now - state.last_slice_received;
            let timed_out = elapsed.whole_seconds() > self.slice_arrival_timeout.as_secs() as i64;
            // A material is stale if it timed out AND any of:
            // - end message never arrived (pending_end is None)
            // - there are buffered out-of-order slices still waiting
            // - we're in PendingBegin (end arrived but begin never did — would leak forever)
            if timed_out
                && (state.pending_end.is_none()
                    || !state.buffered_slices.is_empty()
                    || state.phase == AssemblyPhase::PendingBegin)
            {
                stale.push((material_id, elapsed.whole_seconds()));
            }
        }
        stale
    }

    async fn process_stale_material(&self, material_id: Uuid, elapsed_secs: i64) {
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
                "timeout_seconds": self.slice_arrival_timeout.as_secs(),
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
                )));
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
        let Some(folder_name) = path
            .file_name()
            .and_then(|n| n.to_str())
        else {
            return Err(SinexError::invalid_state(format!(
                "Assembler state folder name is not valid UTF-8: {}",
                path.display()
            )));
        };

        let material_id = Uuid::from_str(folder_name).map_err(|error| {
            SinexError::invalid_state(format!(
                "Assembler state folder has invalid material id `{folder_name}`"
            ))
            .with_source(error)
            .with_context("path", path.display().to_string())
        })?;

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
        if fs::try_exists(&temp_path).await.map_err(|error| {
            SinexError::io(format!(
                "Failed to check orphaned temp file existence {}",
                temp_path.display()
            ))
            .with_source(error)
        })? {
            let metadata = fs::metadata(&temp_path).await.map_err(|error| {
                SinexError::io(format!(
                    "Failed to read orphaned temp file metadata {}",
                    temp_path.display()
                ))
                .with_source(error)
            })?;
            let modified = metadata.modified().map_err(|error| {
                SinexError::io(format!(
                    "Failed to read orphaned temp file modification time {}",
                    temp_path.display()
                ))
                .with_source(error)
            })?;
            let age = std::time::SystemTime::now()
                .duration_since(modified)
                .map_err(|error| {
                    SinexError::invalid_state(format!(
                        "Orphaned temp file modification time is in the future: {}",
                        temp_path.display()
                    ))
                    .with_source(error)
                })?;
            if age > self.orphaned_file_age_threshold {
                warn!(
                    material_id = %material_id,
                    age_hours = age.as_secs() / 3600,
                    "Cleaning up very old orphaned temp file"
                );
                self.cleanup_state(material_id).await;
            }
        }

        Ok(())
    }
}

fn encode_max_material_size_bytes(max_material_size_bytes: u64) -> IngestdResult<i64> {
    i64::try_from(max_material_size_bytes).map_err(|error| {
        SinexError::validation("max_material_size_bytes exceeds i64 range")
            .with_context("max_material_size_bytes", max_material_size_bytes.to_string())
            .with_std_error(&error)
    })
}

#[cfg(test)]
mod tests {
    // Inline because this exercises private orphan-state cleanup paths.
    use super::{MaterialAssembler, MaterialTaskOutcome, signal_ready};
    use crate::MaterialReadySet;
    use camino::Utf8PathBuf;
    use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task::JoinSet;
    use xtask::sandbox::prelude::*;

    async fn test_assembler(
        ctx: &TestContext,
    ) -> TestResult<(MaterialAssembler, tempfile::TempDir, tempfile::TempDir)> {
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("orphan-cleanup-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);

        let state_dir = tempfile::tempdir()?;
        let assembler = MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            Some(MaterialReadySet::default()),
            100,
            512 * 1024 * 1024,
            300,
            3_600,
            90,
        )?;

        Ok((assembler, annex_dir, state_dir))
    }

    #[sinex_test]
    async fn check_orphaned_folder_rejects_non_uuid_name(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let path = state_dir.path().join("not-a-uuid");
        tokio::fs::create_dir_all(&path).await?;

        let error = assembler
            .check_orphaned_folder(path)
            .await
            .expect_err("invalid state directory names must fail honestly");
        assert!(error.to_string().contains("invalid material id"));
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn check_orphaned_folder_rejects_non_utf8_name(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let (assembler, _annex_dir, state_dir) = test_assembler(&ctx).await?;
        let invalid_name = std::ffi::OsString::from_vec(vec![0xff, 0xfe, b'x']);
        let path = state_dir.path().join(invalid_name);
        tokio::fs::create_dir_all(&path).await?;

        let error = assembler
            .check_orphaned_folder(path)
            .await
            .expect_err("non-utf8 state directory names must fail honestly");
        assert!(error.to_string().contains("not valid UTF-8"));
        Ok(())
    }

    #[sinex_test]
    async fn ready_signal_reports_dropped_receiver() -> TestResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(rx);

        assert!(!signal_ready(Some(tx), "material-assembler"));
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_material_tasks_accepts_clean_shutdown() -> TestResult<()> {
        let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
        tasks.spawn(async { ("material begin consumer", Ok(Ok(()))) });

        let error = MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_secs(1))
            .await;

        assert!(error.is_none(), "clean shutdown should not report an error");
        assert!(tasks.is_empty(), "all tracked tasks should be drained");
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_material_tasks_preserves_first_shutdown_error() -> TestResult<()> {
        let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
        tasks.spawn(async {
            (
                "material slice consumer",
                Ok(Err(sinex_primitives::error::SinexError::service(
                    "slice consumer failed",
                ))),
            )
        });
        tasks.spawn(async { ("material end consumer", Ok(Ok(()))) });

        let error = MaterialAssembler::wait_for_material_tasks(&mut tasks, Duration::from_secs(1))
            .await
            .expect("shutdown error should be preserved");

        assert!(error.to_string().contains("material slice consumer"));
        assert!(
            error.to_string().contains("shutdown"),
            "cleanup path should annotate the shutdown phase"
        );
        Ok(())
    }

    #[sinex_test]
    async fn wait_for_material_tasks_times_out_hung_tasks() -> TestResult<()> {
        let mut tasks = JoinSet::<MaterialTaskOutcome>::new();
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_flag = completed.clone();
        tasks.spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            completed_flag.store(true, std::sync::atomic::Ordering::Release);
            ("material stale cleanup task", Ok(Ok(())))
        });

        let error = MaterialAssembler::wait_for_material_tasks(
            &mut tasks,
            Duration::from_millis(10),
        )
        .await
        .expect("hung task should time out");

        assert!(error.to_string().contains("timed out waiting"));
        assert!(
            completed.load(std::sync::atomic::Ordering::Acquire),
            "timed out shutdown should still let the material task finish"
        );
        assert!(tasks.is_empty(), "timed out tasks should still be drained");
        Ok(())
    }

    #[sinex_test]
    async fn assembler_rejects_unrepresentable_max_material_size(ctx: TestContext) -> TestResult<()> {
        let ctx = ctx.with_nats().shared().await?;
        let annex_dir = tempfile::tempdir()?;
        let repo_path = Utf8PathBuf::from_path_buf(annex_dir.path().to_path_buf())
            .map_err(|_| color_eyre::eyre::eyre!("tempdir path is not valid utf-8"))?;
        GitAnnex::init(&repo_path, Some("oversized-config-test")).await?;
        let annex = Arc::new(GitAnnex::new(AnnexConfig {
            repo_path,
            num_copies: None,
            large_files: None,
        })?);
        let state_dir = tempfile::tempdir()?;

        let error = match MaterialAssembler::new(
            ctx.nats_client(),
            ctx.pool.clone(),
            annex,
            state_dir.path().to_path_buf(),
            Some(ctx.pipeline_namespace().prefix().to_string()),
            1_000,
            Some(MaterialReadySet::default()),
            100,
            u64::MAX,
            300,
            3_600,
            90,
        ) {
            Ok(_) => panic!("oversized material limits must fail honestly"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("max_material_size_bytes exceeds i64 range"));
        Ok(())
    }
}
