//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-node-sdk crate, allowing for more ergonomic imports:
//!
//! ```rust
//! use crate::runtime::prelude::*;
//!
//! // Instead of:
//! // use crate::runtime::{Node, CheckpointManager, NodeCoordination};
//! // use crate::runtime::{NodeConfig, TimeHorizon, Checkpoint};
//! ```

// Core node traits and types
#[cfg(feature = "messaging")]
pub use crate::node_sdk::exploration::SourceState;
#[cfg(feature = "messaging")]
pub use crate::node_sdk::runtime::stream::{ContinuousStart, NodeRuntimeState};
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{
    ActivityEntry, IngestionHistoryEntry, NodeCapabilities, NodeType, RunnerLifecycle, ScanArgs,
    ScanEstimate, ScanReport,
};
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{Node, TimeHorizon};

// Configuration and coordination
pub use crate::node_sdk::{AutomatonConfig, EventSourceConfig, NodeConfig};
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{HandoffRequest, InstanceMode, NodeCoordination};
pub use crate::node_sdk::{NodeInstance, NodeVersion};

// Lifecycle management
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{IngestorState, SourceUnit, SourceUnitRuntime};

// Event handling
#[cfg(feature = "messaging")]
pub use crate::node_sdk::{EventSender, EventStream};

// CLI and utilities
#[cfg(feature = "messaging")]
pub use crate::node_sdk::NodeArgs;
pub use crate::node_sdk::{deterministic_event_id, deterministic_material_event_id};

// Error types
pub use crate::node_sdk::{NodeResult, SinexError};

// Core sinex types - using direct dependencies
pub use sinex_primitives::{
    JsonValue,
    domain::{EventSource, EventType},
    // error::SinexError,
    events::{Event, payloads::*},
    ids::Id,
    temporal::Timestamp,
};
pub use uuid::Uuid;

#[cfg(feature = "db")]
pub use sinex_db::{DbPool, DbPoolExt};

// Additional commonly used external types
pub use async_trait::async_trait;
pub use serde_json::json;
pub use time::OffsetDateTime;
pub use tokio::{sync::mpsc, time::Duration};
pub use tracing::{debug, error, info, instrument, trace, warn};
