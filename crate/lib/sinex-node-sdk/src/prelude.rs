//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-node-sdk crate, allowing for more ergonomic imports:
//!
//! ```rust
//! use sinex_node_sdk::prelude::*;
//!
//! // Instead of:
//! // use sinex_node_sdk::{Node, CheckpointManager, NodeCoordination};
//! // use sinex_node_sdk::{NodeConfig, TimeHorizon, Checkpoint};
//! ```

// Core node traits and types
#[cfg(feature = "messaging")]
pub use crate::{Checkpoint, CheckpointManager, CheckpointState};
#[cfg(feature = "messaging")]
pub use crate::{Node, TimeHorizon};
#[cfg(feature = "messaging")]
pub use crate::{NodeCapabilities, NodeType, RunnerLifecycle, ScanArgs, ScanEstimate, ScanReport};

// Configuration and coordination
pub use crate::{AutomatonConfig, EventSourceConfig, NodeConfig};
#[cfg(feature = "messaging")]
pub use crate::{HandoffRequest, InstanceMode, NodeCoordination};
pub use crate::{NodeInstance, NodeVersion};

// Lifecycle management
#[cfg(feature = "messaging")]
pub use crate::{IngestorNode, IngestorNodeAdapter, IngestorState};
#[cfg(feature = "messaging")]
pub use crate::{LifecycleManager, ServiceStatus};

// Event handling and replay
#[cfg(feature = "messaging")]
pub use crate::{EventSender, EventStream};
#[cfg(all(feature = "db", feature = "messaging"))]
pub use crate::{
    MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters, ReplayMetrics, ReplayMode,
    ReplayProgress, ReplayResult, ReplayService, ReplayStats,
};

// CLI and utilities
#[cfg(feature = "messaging")]
pub use crate::NodeArgs;

// Error types
pub use crate::{NodeResult, SinexError};

// Core sinex types - using direct dependencies
pub use sinex_primitives::Ulid;
pub use sinex_primitives::{
    JsonValue,
    domain::{EventSource, EventType},
    // error::SinexError,
    events::{Event, payloads::*},
    ids::Id,
    temporal::Timestamp,
};

#[cfg(feature = "db")]
pub use sinex_db::{DbPool, DbPoolExt};

// Additional commonly used external types
pub use async_trait::async_trait;
pub use serde_json::json;
pub use time::OffsetDateTime;
pub use tokio::{sync::mpsc, time::Duration};
pub use tracing::{debug, error, info, instrument, trace, warn};
