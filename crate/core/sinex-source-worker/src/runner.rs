//! Source-unit runner — assembles per-unit runtime handles before the SDK
//! [`NodeRunner`](sinex_node_sdk::runtime::stream::NodeRunner) takes over.
//!
//! Each source unit gets:
//! - A per-unit [`SourceWorkerDrainController`] for the enhanced drain protocol
//! - Per-unit checkpoint isolation via the `--source-unit` flag (routed through
//!   [`ServiceInfo::checkpoint_identity`](sinex_node_sdk::runtime::stream::ServiceInfo))
//! - Per-unit health reporting (auto-enabled by [`SourceUnitRuntime`])
//!
//! The runner itself is thin — most lifecycle work is handled by the SDK's
//! [`NodeRunner`] and [`SourceUnitRuntime`]. The source-worker adds drain
//! protocol and recovery context on top.

use crate::drain::SourceWorkerDrainController;
use std::sync::Arc;

/// Per-unit runtime context assembled during source-worker startup.
///
/// Holds the enhanced drain controller for the unit. The SDK's `NodeRunner`
/// already provides checkpoint, health, NATS, and DB handles through
/// [`NodeHandles`](sinex_node_sdk::runtime::stream::NodeHandles).
#[derive(Debug)]
pub struct SourceUnitRunner {
    unit_id: String,
    drain_controller: Arc<SourceWorkerDrainController>,
}

impl SourceUnitRunner {
    /// Create a new runner for the given source unit id.
    #[must_use]
    pub fn new(unit_id: String) -> Self {
        Self {
            unit_id,
            drain_controller: Arc::new(SourceWorkerDrainController::new()),
        }
    }

    /// The source unit id this runner manages.
    #[must_use]
    pub fn unit_id(&self) -> &str {
        &self.unit_id
    }

    /// Access the per-unit drain controller.
    #[must_use]
    pub fn drain_controller(&self) -> Arc<SourceWorkerDrainController> {
        Arc::clone(&self.drain_controller)
    }
}
