//! Readiness signal helper for consumer startup coordination.

use tracing::warn;

pub(super) fn signal_ready(
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    component: &str,
) -> bool {
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
