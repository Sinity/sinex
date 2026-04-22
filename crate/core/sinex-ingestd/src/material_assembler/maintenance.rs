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

use sinex_primitives::{Timestamp, Uuid};

use super::{MaterialAssembler, shutdown_signal, state};
use crate::{IngestdResult, SinexError};

const STALE_ASSEMBLY_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1);

pub(super) type MaterialTaskOutcome = (
    &'static str,
    Result<IngestdResult<()>, tokio::task::JoinError>,
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
        handle: JoinHandle<IngestdResult<()>>,
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
                                cleanup_error = Some(crate::service::task_shutdown_error("material", name, &error));
                            }
                        }
                        Some(Ok((name, Err(error)))) => {
                            warn!(task = name, error = ?error, "Material task join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(crate::service::task_shutdown_error("material", name, &error));
                            }
                        }
                        Some(Err(error)) => {
                            warn!(error = ?error, "Material task monitor join failed during shutdown");
                            if cleanup_error.is_none() {
                                cleanup_error = Some(crate::service::task_shutdown_error("material", "monitor", &error));
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
        result: Result<IngestdResult<()>, tokio::task::JoinError>,
        shutdown_flag: &Arc<AtomicBool>,
    ) -> IngestdResult<()> {
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
    ) -> IngestdResult<()> {
        let mut interval = tokio::time::interval(STALE_ASSEMBLY_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                () = shutdown_signal(&shutdown_flag, &shutdown_notify) => break,
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
                buffered_slices = buffered_slices,
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

    /// Scan state root for orphaned temp files from crashed/terminated assemblies.
    pub(super) async fn cleanup_orphaned_temp_files(&self) -> IngestdResult<()> {
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
    ) -> IngestdResult<()> {
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
