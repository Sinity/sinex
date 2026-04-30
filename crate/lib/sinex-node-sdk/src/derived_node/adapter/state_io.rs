//! Checkpoint state I/O and bookkeeping for `DerivedNodeAdapter`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::{DerivedNodeAdapter, restore_resume_position};

use crate::checkpoint::CheckpointState;
use crate::derived_node::traits::DerivedNodeImpl;
use crate::processing::PersistedState;
use crate::runtime::stream::Checkpoint;
use crate::{NodeResult, SinexError};

use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};

use std::path::Path;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

impl<N> DerivedNodeAdapter<N>
where
    N: DerivedNodeImpl,
{
    pub(super) async fn cleanup_hot_reload_file_best_effort(
        path: &Path,
        node_name: &str,
        reason: &'static str,
    ) {
        if let Err(error) = CheckpointState::delete_file(path).await {
            warn!(
                node = node_name,
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
    ) -> NodeResult<()> {
        let Some(path) = self.pending_hot_reload_cleanup.take() else {
            return Ok(());
        };

        match CheckpointState::delete_file(&path).await {
            Ok(()) => Ok(()),
            Err(delete_error) => {
                warn!(
                    node = %self.node.name(),
                    path = %path.display(),
                    error = %delete_error,
                    "Failed to delete restored hot reload checkpoint file after syncing to NATS KV; rewriting it with the latest durable state"
                );
                checkpoint_state.save_to_file(&path).await.map_err(|error| {
                    SinexError::io(
                        "Failed to synchronize restored hot reload file after checkpoint save",
                    )
                    .with_context("node", self.node.name())
                    .with_context("path", path.display().to_string())
                    .with_context("delete_error", delete_error.to_string())
                    .with_std_error(&error)
                })
            }
        }
    }

    pub(super) async fn load_state(&mut self) -> NodeResult<()> {
        let hot_reload_path = self.shutdown_config.checkpoint_path(self.node.name());
        let mut invalid_hot_reload_file = None;

        // Priority 1: file-based checkpoint (hot reload)
        if self.shutdown_config.restore_state_on_startup {
            match self.try_restore_from_file().await {
                Ok(Some((persisted, revision))) => {
                    self.persisted_state = persisted;
                    self.last_revision = revision;
                    return Ok(());
                }
                Ok(None) => {}
                Err(error) if self.checkpoint_manager.is_some() => {
                    warn!(
                        node = %self.node.name(),
                        path = %hot_reload_path.display(),
                        error = %error,
                        "Failed to restore hot reload checkpoint file; falling back to NATS KV"
                    );
                    invalid_hot_reload_file = Some(hot_reload_path.clone());
                }
                Err(error) => return Err(error),
            }
        }

        // Priority 2: NATS KV checkpoint
        let Some(checkpoint_mgr) = &self.checkpoint_manager else {
            return Ok(());
        };

        let checkpoint_state = checkpoint_mgr.load_checkpoint().await?;
        match checkpoint_state.data {
            Some(data) => {
                let mut persisted: PersistedState<N::State> = crate::checkpoint::decode_checkpoint_data(
                    data,
                    "derived checkpoint state",
                    self.node.name(),
                )?;
                restore_resume_position(&mut persisted, &checkpoint_state.checkpoint);
                info!(
                    node = %self.node.name(),
                    events_processed = persisted.events_processed,
                    "Restored state from NATS KV checkpoint"
                );
                self.persisted_state = persisted;
                self.last_revision = checkpoint_state.revision;
            }
            None if matches!(checkpoint_state.checkpoint, Checkpoint::None) => {
                warn!(
                    node = %self.node.name(),
                    "No valid checkpoint for derived node; replaying full historical input"
                );
                self.persisted_state = PersistedState::default();
                self.last_revision = checkpoint_state.revision;
            }
            None => {
                return Err(SinexError::checkpoint(
                    "Derived checkpoint KV entry is missing state data",
                )
                .with_context("node", self.node.name()));
            }
        }

        if let Some(path) = invalid_hot_reload_file {
            Self::cleanup_hot_reload_file_best_effort(
                &path,
                self.node.name(),
                "discarding invalid hot reload checkpoint file after successful NATS KV restore",
            )
            .await;
        }

        Ok(())
    }

    pub(super) async fn try_restore_from_file(
        &mut self,
    ) -> NodeResult<Option<(PersistedState<N::State>, u64)>> {
        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
        let Some(file_state) = CheckpointState::load_from_file(&checkpoint_path).await? else {
            return Ok(None);
        };
        let Some(data) = file_state.data else {
            return Err(SinexError::checkpoint(
                "Derived hot reload checkpoint file is missing state data",
            )
            .with_context("node", self.node.name())
            .with_context("path", checkpoint_path.display().to_string()));
        };

        let mut persisted: PersistedState<N::State> = crate::checkpoint::decode_checkpoint_data(
            data,
            "derived hot reload state",
            self.node.name(),
        )?;
        restore_resume_position(&mut persisted, &file_state.checkpoint);
        info!(
            node = %self.node.name(),
            events_processed = persisted.events_processed,
            "Restored state from hot reload file"
        );
        self.pending_hot_reload_cleanup = Some(checkpoint_path);
        Ok(Some((persisted, file_state.revision)))
    }

    pub async fn save_state_to_file(&self) -> std::io::Result<()> {
        if !self.shutdown_config.save_state_on_shutdown {
            return Ok(());
        }

        let checkpoint_path = self.shutdown_config.checkpoint_path(self.node.name());
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

    pub(super) async fn save_state(&mut self) -> NodeResult<()> {
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
            node = %self.node.name(),
            events_processed = self.persisted_state.events_processed,
            revision = self.last_revision,
            "Saved checkpoint"
        );

        Ok(())
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
