//! Shutdown sequence for `NodeRunner<T>`.
//!
//! Hosts the public `shutdown` entry point and its supporting helpers
//! (`shutdown_task`, `shutdown_leader_state`, `shutdown_event_batcher`).
//! Idempotent: safe to call on already-shut-down or never-initialized
//! runners.

use super::*;

impl<T: Node + 'static> NodeRunner<T> {
    /// Graceful shutdown.
    ///
    /// Idempotent: safe to call multiple times or on a never-initialized runner.
    pub async fn shutdown(&mut self) -> NodeResult<()> {
        if matches!(self.lifecycle, RunnerLifecycle::ShutDown) {
            debug!("shutdown() called on already shut-down runner; no-op");
            return Ok(());
        }
        if matches!(self.lifecycle, RunnerLifecycle::Created) {
            debug!("shutdown() called on never-initialized runner; no-op");
            self.lifecycle = RunnerLifecycle::ShutDown;
            return Ok(());
        }

        info!("Shutting down stream node runner");

        let mut shutdown_errors = Vec::new();
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "schema broadcast listener",
            Self::shutdown_task(
                &mut self.schema_listener_handle,
                self.schema_listener_shutdown.take(),
                "schema broadcast listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "command listener",
            Self::shutdown_task(
                &mut self.command_listener_handle,
                self.command_listener_shutdown.take(),
                "command listener",
            )
            .await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "coordination",
            self.shutdown_leader_state().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "automaton consumer",
            Self::shutdown_task(&mut self.consumer_handle, None, "automaton consumer").await,
        );
        // Save checkpoint BEFORE draining the event batcher. This ensures the
        // checkpoint reflects the last fully-processed position. Events still in
        // the batcher channel will be published during drain but are "ahead" of
        // the checkpoint — on restart they'll be re-processed (at-least-once).
        // The previous order (batcher first, then checkpoint) could silently drop
        // events if the batcher's 250ms grace period expired mid-flush.
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "node shutdown",
            self.node.shutdown().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "event batcher",
            self.shutdown_event_batcher().await,
        );
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "checkpoint cleanup",
            Self::shutdown_task(
                &mut self.checkpoint_cleanup_handle,
                self.checkpoint_cleanup_shutdown.take(),
                "checkpoint cleanup",
            )
            .await,
        );

        match Self::collapse_shutdown_errors(shutdown_errors) {
            Ok(()) => {
                self.lifecycle = RunnerLifecycle::ShutDown;
                Ok(())
            }
            Err(error) => {
                self.lifecycle = RunnerLifecycle::ShutdownFailed;
                Err(error)
            }
        }
    }

    pub(super) async fn shutdown_task(
        handle: &mut Option<tokio::task::JoinHandle<()>>,
        shutdown_tx: Option<watch::Sender<bool>>,
        name: &str,
    ) -> NodeResult<()> {
        if let Some(shutdown_tx) = shutdown_tx {
            Self::signal_watch_shutdown(shutdown_tx, name);
        }
        if let Some(mut h) = handle.take() {
            if let Ok(result) = tokio::time::timeout(TASK_SHUTDOWN_GRACE_PERIOD, &mut h).await {
                Self::shutdown_join_result(name, result)
            } else {
                debug!(
                    task = name,
                    grace_period_ms = TASK_SHUTDOWN_GRACE_PERIOD.as_millis(),
                    "Task did not exit within shutdown grace period; aborting"
                );
                h.abort();
                Self::shutdown_join_result(name, h.await)
            }
        } else {
            Ok(())
        }
    }

    pub(super) async fn shutdown_leader_state(&mut self) -> NodeResult<()> {
        if let Some(state) = self.leader_state.take() {
            let mut shutdown_errors = Vec::new();
            Self::signal_shutdown_channel(state.heartbeat_shutdown, "coordination heartbeat");
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination heartbeat",
                Self::shutdown_join_result("coordination heartbeat", state.heartbeat_handle.await),
            );
            Self::push_shutdown_error(
                &mut shutdown_errors,
                "coordination leadership release",
                Self::leadership_release_result(
                    &state.instance_id,
                    state.kv_client.release_leadership(&state.instance_id).await,
                ),
            );
            Self::collapse_shutdown_errors(shutdown_errors)
        } else {
            Ok(())
        }
    }

    pub(super) async fn shutdown_event_batcher(&mut self) -> NodeResult<()> {
        if let Some(shutdown_tx) = self.event_batcher_shutdown.take() {
            Self::signal_shutdown_channel(shutdown_tx, "event batcher");
        }
        if let Some(handle) = self.event_batcher_handle.take() {
            Self::event_batcher_shutdown_result(handle.await)
        } else {
            Ok(())
        }
    }

}
