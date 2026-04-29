//! Static helper functions for `NodeRunner` shutdown bookkeeping.
//!
//! These are pure (no `self`) utilities for signalling shutdown channels,
//! collating errors from multiple shutdown steps, and building stable
//! identifiers used during runner setup.

use super::{Node, NodeRunner};
use crate::{NodeResult, SinexError};
use sinex_primitives::Uuid;
use tokio::sync::watch;
use tracing::{debug, warn};

impl<T: Node + 'static> NodeRunner<T> {
    pub(super) fn signal_shutdown_channel(
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
        task_name: &str,
    ) -> bool {
        if shutdown_tx.send(()).is_err() {
            warn!(
                task = task_name,
                "Shutdown receiver was already dropped before graceful shutdown"
            );
            return false;
        }
        true
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "watch::Sender must be moved to send"
    )]
    pub(super) fn signal_watch_shutdown(shutdown_tx: watch::Sender<bool>, task_name: &str) -> bool {
        if shutdown_tx.send(true).is_err() {
            warn!(
                task = task_name,
                "Watch shutdown receiver was already dropped before graceful shutdown"
            );
            return false;
        }
        true
    }

    pub(super) fn shutdown_join_result(
        task_name: &str,
        result: Result<(), tokio::task::JoinError>,
    ) -> NodeResult<()> {
        match result {
            Ok(()) => {
                debug!(task = task_name, "Task finished before shutdown cleanup");
                Ok(())
            }
            Err(join_error) if join_error.is_cancelled() => {
                debug!(task = task_name, "Task aborted during shutdown cleanup");
                Ok(())
            }
            Err(join_error) => Err(SinexError::processing("Task failed during shutdown")
                .with_context("task", task_name.to_string())
                .with_context("join_error", join_error.to_string())),
        }
    }

    pub(super) fn leadership_release_result(
        instance_id: &str,
        result: NodeResult<()>,
    ) -> NodeResult<()> {
        result.map_err(|error| error.with_context("instance_id", instance_id.to_string()))
    }

    pub(super) fn event_batcher_shutdown_result(
        result: Result<NodeResult<()>, tokio::task::JoinError>,
    ) -> NodeResult<()> {
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(join_error) => Err(
                SinexError::processing("Event batcher failed during shutdown")
                    .with_context("join_error", join_error.to_string()),
            ),
        }
    }

    pub(super) fn push_shutdown_error(
        errors: &mut Vec<(String, SinexError)>,
        step: impl Into<String>,
        result: NodeResult<()>,
    ) {
        if let Err(error) = result {
            errors.push((step.into(), error));
        }
    }

    pub(super) fn collapse_shutdown_errors(
        mut errors: Vec<(String, SinexError)>,
    ) -> NodeResult<()> {
        if errors.is_empty() {
            return Ok(());
        }

        let (step, error) = errors.remove(0);
        let mut combined = error.with_context("shutdown_step", step);
        for (index, (step, extra)) in errors.into_iter().enumerate() {
            combined = combined
                .with_context(format!("additional_shutdown_step_{}", index + 1), step)
                .with_context(
                    format!("additional_shutdown_error_{}", index + 1),
                    extra.to_string(),
                );
        }
        Err(combined)
    }

    pub(super) fn build_instance_id(host: &str) -> String {
        format!("{host}-{}-{}", std::process::id(), Uuid::now_v7().simple())
    }
}
