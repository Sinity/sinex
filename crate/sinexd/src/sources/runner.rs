//! Source-unit runner — assembles per-unit runtime handles before the SDK
//! [`NodeRunner`](crate::node_sdk::runtime::stream::NodeRunner) takes over.
//!
//! Each source unit gets:
//! - A per-unit [`SourceUnitDrainController`] for the enhanced drain protocol
//! - Per-unit checkpoint isolation via the `--source-unit` flag (routed through
//!   [`ServiceInfo::checkpoint_identity`](crate::node_sdk::runtime::stream::ServiceInfo))
//! - Per-unit health reporting (auto-enabled by [`SourceUnitRuntime`])
//!
//! The runner itself is thin — most lifecycle work is handled by the SDK's
//! [`NodeRunner`] and [`SourceUnitRuntime`]. The source-unit host adds drain
//! protocol and recovery context on top.

use crate::sources::drain::SourceUnitDrainController;
use std::sync::Arc;

/// Per-unit runtime context assembled during source-unit startup.
///
/// Holds the enhanced drain controller for the unit. The SDK's `NodeRunner`
/// already provides checkpoint, health, NATS, and DB handles through
/// [`NodeHandles`](crate::node_sdk::runtime::stream::NodeHandles).
#[derive(Debug)]
pub struct SourceUnitRunner {
    unit_id: String,
    drain_controller: Arc<SourceUnitDrainController>,
}

impl SourceUnitRunner {
    /// Create a new runner for the given source unit id.
    #[must_use]
    pub fn new(unit_id: String) -> Self {
        Self {
            unit_id,
            drain_controller: Arc::new(SourceUnitDrainController::new()),
        }
    }

    /// The source unit id this runner manages.
    #[must_use]
    pub fn unit_id(&self) -> &str {
        &self.unit_id
    }

    /// Access the per-unit drain controller.
    #[must_use]
    pub fn drain_controller(&self) -> Arc<SourceUnitDrainController> {
        Arc::clone(&self.drain_controller)
    }
}
