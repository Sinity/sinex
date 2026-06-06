//! Source runner — assembles per-source runtime handles before the runtime
//! [`RuntimeRunner`](crate::runtime::stream::RuntimeRunner) takes over.
//!
//! Each source gets:
//! - A per-source [`SourceDrainController`] for the enhanced drain protocol
//! - Per-source checkpoint isolation via the `--source` flag (routed through
//!   [`ServiceInfo::checkpoint_identity`](crate::runtime::stream::ServiceInfo))
//! - Per-source health reporting (auto-enabled by [`SourceDriverRuntime`])
//!
//! The runner itself is thin — most lifecycle work is handled by the runtime's
//! [`RuntimeRunner`] and [`SourceDriverRuntime`]. The source host adds drain
//! protocol and recovery context on top.

use crate::sources::drain::SourceDrainController;
use std::sync::Arc;

/// Per-source runtime context assembled during source startup.
///
/// Holds the enhanced drain controller for the source. The runtime's `RuntimeRunner`
/// already provides checkpoint, health, NATS, and DB handles through
/// [`RuntimeHandles`](crate::runtime::stream::RuntimeHandles).
#[derive(Debug)]
pub struct SourceRunner {
    unit_id: String,
    drain_controller: Arc<SourceDrainController>,
}

impl SourceRunner {
    /// Create a new runner for the given source id.
    #[must_use]
    pub fn new(unit_id: String) -> Self {
        Self {
            unit_id,
            drain_controller: Arc::new(SourceDrainController::new()),
        }
    }

    /// The source id this runner manages.
    #[must_use]
    pub fn unit_id(&self) -> &str {
        &self.unit_id
    }

    /// Access the per-source drain controller.
    #[must_use]
    pub fn drain_controller(&self) -> Arc<SourceDrainController> {
        Arc::clone(&self.drain_controller)
    }
}
