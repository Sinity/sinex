//! Shutdown sequence for `RuntimeRunner`.
//!
//! Hosts the public `shutdown` entry point and its supporting helpers
//! (`shutdown_task`, `shutdown_event_batcher`).
//! Idempotent: safe to call on already-shut-down or never-initialized
//! runners.

use super::{
    RunnerLifecycle, RuntimeResult, RuntimeRunner, TASK_SHUTDOWN_GRACE_PERIOD, debug, info, watch,
};

impl RuntimeRunner {
    /// Graceful shutdown.
    ///
    /// Idempotent: safe to call multiple times or on a never-initialized runner.
    pub async fn shutdown(&mut self) -> RuntimeResult<()> {
        if matches!(self.lifecycle, RunnerLifecycle::ShutDown) {
            debug!("shutdown() called on already shut-down runner; no-op");
            return Ok(());
        }
        if matches!(self.lifecycle, RunnerLifecycle::Created) {
            debug!("shutdown() called on never-initialized runner; no-op");
            self.lifecycle = RunnerLifecycle::ShutDown;
            return Ok(());
        }

        info!("Shutting down stream module runner");

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
            "dispatched replay worker",
            self.shutdown_replay_worker().await,
        );
        // Parse listener (#1780) holds a NATS subscription with no clean-exit
        // signal; aborted directly after the grace period (like the consumer).
        Self::push_shutdown_error(
            &mut shutdown_errors,
            "parse listener",
            Self::shutdown_task(&mut self.parse_listener_handle, None, "parse listener").await,
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
            "module shutdown",
            self.module.shutdown().await,
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
    ) -> RuntimeResult<()> {
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

    /// Stop and join the replay worker that the command listener dispatched.
    ///
    /// The listener is stopped first, so no new worker can be accepted while
    /// shutdown owns this handle. Cancellation lets the worker run its own
    /// shutdown path; the grace-period abort is only a last resort.
    async fn shutdown_replay_worker(&self) -> RuntimeResult<()> {
        let cancel = self
            .replay_worker_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((_operation_id, cancel_tx)) = cancel {
            Self::signal_watch_shutdown(cancel_tx, "dispatched replay worker");
        }

        let handle = self
            .replay_worker_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(mut handle) = handle else {
            return Ok(());
        };

        if let Ok(result) = tokio::time::timeout(TASK_SHUTDOWN_GRACE_PERIOD, &mut handle).await {
            Self::shutdown_join_result("dispatched replay worker", result)
        } else {
            debug!(
                grace_period_ms = TASK_SHUTDOWN_GRACE_PERIOD.as_millis(),
                "Dispatched replay worker did not stop within shutdown grace period; aborting"
            );
            handle.abort();
            Self::shutdown_join_result("dispatched replay worker", handle.await)
        }
    }

    pub(super) async fn shutdown_event_batcher(&mut self) -> RuntimeResult<()> {
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
