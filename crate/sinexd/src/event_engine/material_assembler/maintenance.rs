//! Runtime maintenance loops and task lifecycle handling for material assembly.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::{
    fs,
    sync::Notify,
    task::{JoinHandle, JoinSet},
    time::Duration,
};
use tracing::{debug, info, warn};

use sinex_db::DbPoolExt;
use sinex_primitives::{Id, Timestamp, Uuid};

use super::{MaterialAssembler, state};
use crate::event_engine::{EventEngineResult, SinexError};

const STALE_ASSEMBLY_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1);
const STALE_REGISTRY_RECONCILE_LIMIT: i64 = 128;
const ORPHANED_SENSING_REASON: &str = "orphaned_sensing_material";
const ORPHANED_SELF_OBSERVATION_RECOVERY_REASON: &str =
    "orphaned_self_observation_material_recovered_partial";

pub(super) type MaterialTaskOutcome = (
    &'static str,
    Result<EventEngineResult<()>, tokio::task::JoinError>,
);

struct AbortOnDropHandle<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDropHandle<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn material_task_timeout(count: usize, timeout: Duration) -> SinexError {
    SinexError::service(format!(
        "timed out waiting for {count} material tasks during shutdown"
    ))
    .with_context("timeout_secs", timeout.as_secs().to_string())
}

impl MaterialAssembler {
    pub(super) fn track_material_task(
        tasks: &mut JoinSet<MaterialTaskOutcome>,
        name: &'static str,
        handle: JoinHandle<EventEngineResult<()>>,
    ) {
        tasks.spawn(async move { (name, AbortOnDropHandle::new(handle).join().await) });
    }

    pub(super) async fn wait_for_material_tasks(
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
                                cleanup_error = Some(crate::event_engine::service::task_shutdown_error("material", name, &error));
                            }
                        }
                        Some(Ok((name, Err(error)))) => {
                            warn!(task = name, error = ?error, "Material task join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(crate::event_engine::service::task_shutdown_error("material", name, &error));
                            }
                        }
                        Some(Err(error)) => {
                            warn!(error = ?error, "Material task monitor join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(crate::event_engine::service::task_shutdown_error("material", "monitor", &error));
                            }
                        }
                        None => break,
                    }
                    if tasks.is_empty() {
                        break;
                    }
                }
                () = &mut deadline => {
                    let remaining = tasks.len();
                    warn!(
                        "Timed out waiting for {} material tasks after {:?}, aborting remaining work",
                        remaining,
                        timeout
                    );
                    tasks.abort_all();
                    while let Some(result) = tasks.join_next().await {
                        if let Err(error) = result {
                            debug!(error = ?error, "Material task aborted during shutdown cleanup");
                        }
                    }
                    if cleanup_error.is_none() {
                        cleanup_error = Some(material_task_timeout(remaining, timeout));
                    }
                    break;
                }
            }
        }

        info!("Material task cleanup complete");
        cleanup_error
    }

    pub(super) fn handle_task_exit(
        task_name: &str,
        result: Result<EventEngineResult<()>, tokio::task::JoinError>,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> EventEngineResult<()> {
        match result {
            Ok(Ok(())) if shutdown_flag.load(Ordering::Relaxed) => Ok(()),
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

    /// Periodically check for stale assemblies and clean them up.
    pub(super) async fn run_stale_assembly_cleanup(
        &self,
        shutdown_flag: Arc<AtomicBool>,
        shutdown_notify: Arc<Notify>,
    ) -> EventEngineResult<()> {
        let mut interval = tokio::time::interval(STALE_ASSEMBLY_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                () = crate::runtime::wait_for_shutdown_signal_bool(&shutdown_flag, &shutdown_notify) => break,
            }

            let active = self.assembler_state.len() as u32;
            let buffered_slices: u32 = self
                .assembler_state
                .iter()
                .map(|entry| {
                    entry
                        .value()
                        .try_lock()
                        .map_or(0, |state| state.buffered_slices.len() as u32)
                })
                .sum();

            let stats = self.stats.snapshot();

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
                total_commit_outcome_unknown = stats.commit_outcome_unknown,
                buffered_slices = buffered_slices,
                finalize_in_flight = self.finalize_in_flight() as u64,
                max_pending_finalizes = self.max_pending_finalizes as u64,
            );

            if let Some(ref observer) = self.observer
                && let Err(error) = observer
                    .emit_assembly_stats(
                        active,
                        stats.started,
                        stats.completed,
                        stats.cancelled,
                        stats.failed,
                        stats.timed_out,
                        stats.commit_outcome_unknown,
                        None,
                        buffered_slices,
                    )
                    .await
            {
                debug!("Failed to emit assembly stats: {}", error);
            }

            let stale_materials = self.find_stale_materials().await;

            for (material_id, elapsed_secs) in stale_materials {
                self.process_stale_material(material_id, elapsed_secs).await;
            }

            if let Err(error) = self.reconcile_orphaned_sensing_materials().await {
                warn!(
                    error = %error,
                    "Failed to reconcile orphaned sensing source materials"
                );
            }

            // Re-drive finalizations that were dispatched off the consumer but
            // failed-and-reverted under transient DB/IO stress (#2187 prong d).
            // The decoupled finalize path no longer relies on a redelivered NATS
            // frame, so this maintenance pass is the live retry channel between
            // crashes (WAL replay covers the across-restart case).
            self.redrive_pending_finalizes().await;

            if let Err(error) = self.cleanup_orphaned_temp_files().await {
                warn!("Failed to cleanup orphaned temp files: {}", error);
            }
        }

        Ok(())
    }

    pub(super) async fn find_stale_materials(&self) -> Vec<(Uuid, i64)> {
        let now = Timestamp::now();
        let mut stale = Vec::new();

        let state_handles: Vec<_> = self
            .assembler_state
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        for (material_id, state_handle) in state_handles {
            let state = state_handle.lock().await;

            if state.phase == state::AssemblyPhase::Finalizing {
                continue;
            }

            let elapsed = now - state.last_slice_received;
            let timed_out = elapsed.whole_seconds() > self.slice_arrival_timeout.as_secs() as i64;
            if timed_out
                && (state.pending_end.is_none()
                    || !state.buffered_slices.is_empty()
                    || state.phase == state::AssemblyPhase::PendingBegin)
            {
                stale.push((material_id, elapsed.whole_seconds()));
            }
        }
        stale
    }

    /// Re-dispatch finalization for materials that hold a recorded `End` but are
    /// not currently finalizing — i.e. a decoupled finalize failed-and-reverted,
    /// or an `End` was restored from the WAL on a previous boot and never landed.
    ///
    /// Gated on a minimum idle age so the consumer's own immediate dispatch (on the
    /// END / last-slice frame) is not duplicated for freshly-completed materials.
    /// `try_finalize_pending_end` is idempotent under the per-material lock + phase
    /// guard, so a redundant re-drive against an in-flight finalize simply no-ops.
    pub(super) async fn redrive_pending_finalizes(&self) {
        const REDRIVE_MIN_IDLE_SECS: i64 = 30;
        let now = Timestamp::now();

        let state_handles: Vec<_> = self
            .assembler_state
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        let mut to_redrive = Vec::new();
        for (material_id, state_handle) in state_handles {
            let Ok(state) = state_handle.try_lock() else {
                continue;
            };
            let idle = (now - state.last_slice_received).whole_seconds();
            if state.pending_end.is_some()
                && state.phase != state::AssemblyPhase::Finalizing
                && state.phase != state::AssemblyPhase::PendingBegin
                && idle >= REDRIVE_MIN_IDLE_SECS
            {
                to_redrive.push((material_id, state_handle.clone()));
            }
        }

        for (material_id, state_handle) in to_redrive {
            debug!(
                material_id = %material_id,
                "Re-driving stuck pending-end finalize from maintenance loop"
            );
            self.dispatch_finalize(material_id, state_handle);
        }
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

    pub(super) async fn reconcile_orphaned_sensing_materials(&self) -> EventEngineResult<()> {
        let global_cutoff =
            Timestamp::now() - time::Duration::seconds(self.slice_arrival_timeout.as_secs() as i64);
        let stale_rows = self
            .pool
            .source_materials()
            .list_stale_sensing(global_cutoff, STALE_REGISTRY_RECONCILE_LIMIT)
            .await
            .map_err(|error| {
                SinexError::database("Failed to list orphaned sensing source materials")
                    .with_source(error)
            })?;

        for row in stale_rows {
            let material_id = row.id;
            if self.assembler_state.contains_key(&material_id) {
                continue;
            }

            let material_started_at = row.start_time.unwrap_or(row.staged_at);
            let elapsed_secs = (Timestamp::now() - material_started_at).whole_seconds();
            warn!(
                material_id = %material_id,
                source_identifier = %row.source_identifier,
                elapsed_secs,
                "Reconciling orphaned sensing source material with no active assembly state"
            );
            if is_self_observation_material(&row.source_identifier) {
                self.recover_orphaned_self_observation_material(
                    material_id,
                    &row.source_identifier,
                    elapsed_secs,
                )
                .await?;
                continue;
            }
            self.route_material_error(
                material_id,
                ORPHANED_SENSING_REASON,
                serde_json::json!({
                    "timeout_seconds": self.slice_arrival_timeout.as_secs(),
                    "elapsed_seconds": elapsed_secs,
                    "source_identifier": row.source_identifier,
                    "staged_at": row.staged_at.to_string(),
                    "start_time": row.start_time.map(|ts| ts.to_string()),
                }),
            )
            .await;
            self.finalize_failed_material(material_id, ORPHANED_SENSING_REASON)
                .await;
        }

        Ok(())
    }

    async fn recover_orphaned_self_observation_material(
        &self,
        material_id: Uuid,
        source_identifier: &str,
        elapsed_secs: i64,
    ) -> EventEngineResult<()> {
        info!(
            material_id = %material_id,
            source_identifier,
            elapsed_secs,
            "Marking orphaned self-observation material as recovered_partial"
        );
        self.pool
            .source_materials()
            .mark_as_recovered_partial(
                Id::from_uuid(material_id),
                ORPHANED_SELF_OBSERVATION_RECOVERY_REASON,
                serde_json::json!({
                    "orphaned_sensing_material": {
                        "source_identifier": source_identifier,
                        "elapsed_seconds": elapsed_secs,
                        "timeout_seconds": self.slice_arrival_timeout.as_secs(),
                        "dlq_policy": "suppressed_self_observation_restart_orphan"
                    }
                }),
            )
            .await
            .map_err(|error| {
                SinexError::database("Failed to mark orphaned self-observation material recovered_partial")
                    .with_context("material_id", material_id.to_string())
                    .with_context("source_identifier", source_identifier.to_string())
                    .with_source(error)
            })
    }

    /// Scan state root for orphaned temp files from crashed/terminated assemblies.
    pub(super) async fn cleanup_orphaned_temp_files(&self) -> EventEngineResult<()> {
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

    pub(super) async fn check_orphaned_folder(
        &self,
        path: std::path::PathBuf,
    ) -> EventEngineResult<()> {
        let Some(folder_name) = path.file_name().and_then(|n| n.to_str()) else {
            return Err(SinexError::invalid_state(format!(
                "Assembler state folder name is not valid UTF-8: {}",
                path.display()
            )));
        };

        let material_id = Uuid::parse_str(folder_name).map_err(|error| {
            SinexError::invalid_state(format!(
                "Assembler state folder has invalid material id `{folder_name}`"
            ))
            .with_source(error)
            .with_context("path", path.display().to_string())
        })?;

        if self.assembler_state.contains_key(&material_id) {
            return Ok(());
        }

        if self.material_is_terminal(material_id).await? {
            info!(
                material_id = %material_id,
                "Cleaning up orphaned state for terminal material"
            );
            self.cleanup_state(material_id).await;
            return Ok(());
        }

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

fn is_self_observation_material(source_identifier: &str) -> bool {
    source_identifier.starts_with("sinex.self-observation.")
}

#[cfg(test)]
#[path = "maintenance_test.rs"]
mod tests;
