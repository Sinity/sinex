//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-satellite-sdk crate, allowing for more ergonomic imports:
//!
//! ```rust
//! use sinex_node_sdk::prelude::*;
//!
//! // Instead of:
//! // use sinex_node_sdk::{Node, CheckpointManager, NodeCoordination};
//! // use sinex_node_sdk::{NodeConfig, TimeHorizon, Checkpoint};
//! ```

// Core processor traits and types
pub use crate::{Checkpoint, CheckpointManager, CheckpointState};
pub use crate::{Node, TimeHorizon};
pub use crate::{ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport};

// Configuration and coordination
pub use crate::{AutomatonConfig, EventSourceConfig, NodeConfig};
pub use crate::{HandoffRequest, InstanceMode, NodeCoordination};
pub use crate::{NodeInstance, NodeVersion};

// Lifecycle management
pub use crate::{LifecycleManager, ServiceStatus};

// Event handling and replay
pub use crate::{
    EventSender, EventStream, MetricsSnapshot, ProgressTracker, ReplayController, ReplayFilters,
    ReplayMetrics, ReplayMode, ReplayProgress, ReplayResult, ReplayService, ReplayStats,
};

// CLI and utilities
pub use crate::NodeArgs;

// Error types
pub use crate::{NodeError, NodeResult};

// Core sinex types - now using flattened imports from sinex-core
pub use sinex_core::{
    // Event payloads - using the new facade
    payloads::*,
    // Database operations
    DbPool,
    DbPoolExt,
    // Database models
    Event,
    EventSource,
    EventType,
    Id,
    JsonValue,
    // Error handling
    SinexError,
    Ulid,
};

// Additional commonly used external types
pub use async_trait::async_trait;
pub use color_eyre::eyre::{eyre, Result as EyreResult};
pub use serde_json::json;
pub use tokio::{sync::mpsc, time::Duration};
pub use tracing::{debug, error, info, instrument, trace, warn};
