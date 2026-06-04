//! Source runner — assembles per-unit runtime handles before the SDK
//! [`NodeRunner`](crate::node_sdk::runtime::stream::NodeRunner) takes over.
//!
//! Each source gets:
//! - A per-unit [`SourceDrainController`] for the enhanced drain protocol
//! - Per-unit checkpoint isolation via the `--source` flag (routed through
//!   [`ServiceInfo::checkpoint_identity`](crate::node_sdk::runtime::stream::ServiceInfo))
//! - Per-unit health reporting (auto-enabled by [`SourceDriverRuntime`])
//!
//! The runner itself is thin — most lifecycle work is handled by the SDK's
//! [`NodeRunner`] and [`SourceDriverRuntime`]. The source host adds drain
//! protocol and recovery context on top.

use crate::sources::drain::SourceDrainController;
use std::sync::Arc;

/// Per-unit runtime context assembled during source startup.
///
/// Holds the enhanced drain controller for the unit. The SDK's `NodeRunner`
/// already provides checkpoint, health, NATS, and DB handles through
/// [`NodeHandles`](crate::node_sdk::runtime::stream::NodeHandles).
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

    /// Access the per-unit drain controller.
    #[must_use]
    pub fn drain_controller(&self) -> Arc<SourceDrainController> {
        Arc::clone(&self.drain_controller)
    }
}
