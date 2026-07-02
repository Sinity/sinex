//! Checkpoint state I/O and bookkeeping for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{AutomatonRuntime, restore_resume_position};

use crate::runtime::automaton::traits::Automaton;
use crate::runtime::checkpoint::CheckpointState;
use crate::runtime::processing::PersistedState;
use crate::runtime::stream::Checkpoint;
use crate::runtime::{RuntimeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};

use std::path::Path;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Progress markers extracted from a hot-reload checkpoint file, used to
/// reconcile the file against the durable NATS KV checkpoint in `load_state`.
#[derive(Debug)]
pub(super) struct FileCandidate {
    processed_count: u64,
    revision: u64,
}

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    pub(super) async fn cleanup_hot_reload_file_best_effort(
        path: &Path,
        module_name: &str,
        reason: &'static str,
    ) {
        if let Err(error) = CheckpointState::delete_file(path).await {
            warn!(
                automaton = module_name,
                path = %path.display(),
                error = %error,
                reason,
                "Failed to clean up hot reload checkpoint file"
            );
        }
    }

    pub(super) async fn finalize_restored_hot_reload_file(
        &mut self,
        checkpoint_state: &CheckpointState,
    ) -> RuntimeResult<()> {
        let Some(path) = self.pending_hot_reload_cleanup.take() else {
            return Ok(());
        };

        match CheckpointState::delete_file(&path).await {
            Ok(()) => Ok(()),
            Err(delete_error) => {
                warn!(
                    automaton = %self.automaton.name(),
                    path = %path.display(),
                    error = %delete_error,
                    "Failed to delete restored hot reload checkpoint file after syncing to NATS KV; rewriting it with the latest durable state"
                );
                checkpoint_state.save_to_file(&path).await.map_err(|error| {
                    SinexError::io(
                        "Failed to synchronize restored hot reload file after checkpoint save",
                    )
                    .with_context("automaton", self.automaton.name())
                    .with_context("path", path.display().to_string())
                    .with_context("delete_error", delete_error.to_string())
                    .with_std_error(&error)
                })
            }
        }
    }

    pub(super) async fn load_state(&mut self) -> RuntimeResult<()> {
        let hot_reload_path = self.shutdown_config.checkpoint_path(self.automaton.name());
        let mut invalid_hot_reload_file = None;

        // Stage 1: take the hot-reload (SIGTERM fast-save) file as a *candidate*,
        // not an unconditional winner. It is committed only after reconciling
        // against the durable KV checkpoint below.
        let mut file_candidate: Option<(PersistedState<N::State>, FileCandidate)> = None;
        if self.shutdown_config.restore_state_on_startup {
            match self.try_restore_from_file().await {
                Ok(Some(candidate)) => file_candidate = Some(candidate),
                Ok(None) => {}
                Err(error) if self.checkpoint_manager.is_some() => {
                    warn!(
                        automaton = %self.automaton.name(),
                        path = %hot_reload_path.display(),
                        error = %error,
                        "Failed to restore hot reload checkpoint file; falling back to NATS KV"
                    );
                    invalid_hot_reload_file = Some(hot_reload_path.clone());
                }
                Err(error) => return Err(error),
            }
        }

        // Without a KV manager the file is the only source of truth (local/dev).
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            if let Some((persisted, file)) = file_candidate {
                self.persisted_state = persisted;
                self.last_revision = file.revision;
            }
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;

        // Stage 2: reconcile file vs KV. Trust the hot-reload file when it is
        // ahead-or-equal to the durable KV checkpoint (a genuine SIGTERM save that
        // may not yet be flushed to KV). In EITHER case rebase `last_revision` onto
        // the LIVE KV revision — this is the actual fix for the checkpoint CAS
        // crash-loop: the file's recorded revision can lag KV (older incarnation, or
        // KV advanced past the last fast-save), and seeding `last_revision` from it
        // makes every CAS save fail and crash-loops the automaton through replay (the
        // memory-pinning restart loop). A file that is strictly BEHIND KV is stale —
        // discard it and restore KV's more-advanced state instead.
        if let Some((file_persisted, file)) = file_candidate {
            if file.processed_count >= checkpoint_state.processed_count {
                info!(
                    automaton = %self.automaton.name(),
                    file_processed_count = file.processed_count,
                    kv_processed_count = checkpoint_state.processed_count,
                    file_revision = file.revision,
                    kv_revision = checkpoint_state.revision,
                    "Hot reload file is ahead-or-equal to KV; resuming from file, rebasing onto live KV revision"
                );
                self.persisted_state = file_persisted;
                self.last_revision = checkpoint_state.revision;
                return Ok(());
            }
            warn!(
                automaton = %self.automaton.name(),
                file_processed_count = file.processed_count,
                kv_processed_count = checkpoint_state.processed_count,
                "Hot reload file is stale (strictly behind KV); discarding it and restoring from KV"
            );
            // We are not resuming from the file, so cancel its finalize-on-save
            // cleanup and remove it as a stale artifact instead.
            self.pending_hot_reload_cleanup = None;
            invalid_hot_reload_file = Some(hot_reload_path.clone());
        }
        match checkpoint_state.data {
            Some(data) => {
                let mut persisted: PersistedState<N::State> =
                    crate::runtime::checkpoint::decode_checkpoint_data(
                        data,
                        "derived checkpoint state",
                        self.automaton.name(),
                    )?;
                restore_resume_position(&mut persisted, &checkpoint_state.checkpoint);
                info!(
                    automaton = %self.automaton.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from NATS KV checkpoint"
                );
                self.persisted_state = persisted;
                self.last_revision = checkpoint_state.revision;
            }
            None if matches!(checkpoint_state.checkpoint, Checkpoint::None) => {
                warn!(
                    automaton = %self.automaton.name(),
                    "No valid checkpoint for automaton; replaying full historical input"
                );
                self.persisted_state = PersistedState::default();
                self.last_revision = checkpoint_state.revision;
            }
            None => {
                return Err(SinexError::checkpoint(
                    "Derived checkpoint KV entry is missing state data",
                )
                .with_context("automaton", self.automaton.name()));
            }
        }

        if let Some(path) = invalid_hot_reload_file {
            Self::cleanup_hot_reload_file_best_effort(
                &path,
                self.automaton.name(),
                "discarding invalid hot reload checkpoint file after successful NATS KV restore",
            )
            .await;
        }

        Ok(())
    }

    pub(super) async fn try_restore_from_file(
        &mut self,
    ) -> RuntimeResult<Option<(PersistedState<N::State>, FileCandidate)>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.automaton.name());
        let Some(file_state) = CheckpointState::load_from_file(&checkpoint_path).await? else {
            return Ok(None);
        };
        // Capture the progress markers before `data` is moved out below; these
        // drive file-vs-KV reconciliation in `load_state`.
        let file_processed_count = file_state.processed_count;
        let file_revision = file_state.revision;
        let Some(data) = file_state.data else {
            return Err(SinexError::checkpoint(
                "Derived hot reload checkpoint file is missing state data",
            )
            .with_context("automaton", self.automaton.name())
            .with_context("path", checkpoint_path.display().to_string()));
        };

        let mut persisted: PersistedState<N::State> =
            crate::runtime::checkpoint::decode_checkpoint_data(
                data,
                "derived hot reload state",
                self.automaton.name(),
            )?;
        restore_resume_position(&mut persisted, &file_state.checkpoint);
        info!(
            automaton = %self.automaton.name(),
            events_processed = persisted.events_processed,
            "Restored state from hot reload file"
        );
        self.pending_hot_reload_cleanup = Some(checkpoint_path);
        Ok(Some((
            persisted,
            FileCandidate {
                processed_count: file_processed_count,
                revision: file_revision,
            },
        )))
    }

    pub async fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.automaton.name());
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let checkpoint_state = CheckpointState {
            checkpoint: self.checkpoint_position(),
            processed_count: self.persisted_state.events_processed,
            last_activity: Timestamp::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        checkpoint_state.save_to_file(&checkpoint_path).await
    }

    pub(super) async fn save_state(&mut self) -> RuntimeResult<()> {
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        self.persisted_state.last_checkpoint = Timestamp::now();
        let state_json = serde_json::to_value(&self.persisted_state)
            .map_err(|e| SinexError::processing(format!("Failed to serialize state: {e}")))?;

        let mut checkpoint_state = CheckpointState {
            checkpoint: self.checkpoint_position(),
            processed_count: self.persisted_state.events_processed,
            last_activity: Timestamp::now(),
            data: Some(state_json),
            version: 2,
            revision: self.last_revision,
        };

        self.last_revision = checkpoint_mgr.save_checkpoint(&checkpoint_state).await?;
        checkpoint_state.revision = self.last_revision;
        self.finalize_restored_hot_reload_file(&checkpoint_state)
            .await?;
        self.events_since_checkpoint = 0;
        self.last_checkpoint_time = Instant::now();
        self.observe_checkpoint_state(&checkpoint_state).await;

        debug!(
            automaton = %self.automaton.name(),
            events_processed = self.persisted_state.events_processed,
            revision = self.last_revision,
            "Saved checkpoint"
        );

        Ok(())
    }

    pub(super) async fn save_state_with_file_fallback(
        &mut self,
        context: &'static str,
    ) -> RuntimeResult<()> {
        match self.save_state().await {
            Ok(()) => Ok(()),
            Err(kv_error) => {
                warn!(
                    automaton = %self.automaton.name(),
                    context,
                    error = %kv_error,
                    "NATS KV checkpoint save failed; attempting file-backed checkpoint fallback"
                );
                self.save_state_to_file().await.map_err(|file_error| {
                    SinexError::checkpoint(
                        "Failed to save checkpoint to NATS KV and fallback file",
                    )
                    .with_context("automaton", self.automaton.name())
                    .with_context("checkpoint_context", context)
                    .with_context("kv_error", kv_error.to_string())
                    .with_std_error(&file_error)
                })?;
                warn!(
                    automaton = %self.automaton.name(),
                    context,
                    "Saved checkpoint to fallback file after NATS KV failure"
                );
                self.events_since_checkpoint = 0;
                self.last_checkpoint_time = Instant::now();
                Ok(())
            }
        }
    }

    pub(super) fn should_checkpoint(&self) -> bool {
        self.events_since_checkpoint >= self.config.checkpoint_interval
            || self.last_checkpoint_time.elapsed()
                >= Duration::from_secs(self.config.checkpoint_timeout_secs)
    }

    pub(super) fn checkpoint_position(&self) -> Checkpoint {
        if let Some(event_id) = self.persisted_state.last_input_event_id {
            return Checkpoint::internal(event_id, self.persisted_state.events_processed);
        }

        if self.persisted_state.events_processed > 0 {
            return Checkpoint::timestamp(self.persisted_state.last_checkpoint, None);
        }

        Checkpoint::None
    }

    pub(super) fn current_checkpoint_internal(&self) -> Checkpoint {
        self.checkpoint_position()
    }

    pub(super) fn record_processed_input(&mut self, event_id: Id<Event<JsonValue>>) {
        self.persisted_state.last_input_event_id = Some(*event_id.as_uuid());
        self.persisted_state.events_processed += 1;
        self.events_since_checkpoint += 1;
        self.run_events_processed = self.run_events_processed.saturating_add(1);
    }

    pub(super) fn record_state_mutation(&mut self) {
        self.events_since_checkpoint += 1;
    }
}
