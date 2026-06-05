//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinexd crate, allowing for more ergonomic imports:
//!
//! ```rust
//! use crate::runtime::prelude::*;
//!
//! // Instead of:
//! // use crate::runtime::{RuntimeModule, CheckpointManager, RuntimeCoordination};
//! // use crate::runtime::{RuntimeConfig, TimeHorizon, Checkpoint};
//! ```

// Core node traits and types
#[cfg(feature = "messaging")]
pub use crate::runtime::exploration::SourceState;
#[cfg(feature = "messaging")]
pub use crate::runtime::stream::{ContinuousStart, RuntimeContext};
#[cfg(feature = "messaging")]
pub use crate::runtime::{
    ActivityEntry, IngestionHistoryEntry, ModuleKind, RunnerLifecycle, RuntimeCapabilities,
    ScanArgs, ScanEstimate, ScanReport,
};
#[cfg(feature = "messaging")]
pub use crate::runtime::{Checkpoint, CheckpointManager, CheckpointState};
#[cfg(feature = "messaging")]
pub use crate::runtime::{RuntimeModule, TimeHorizon};

// Configuration and coordination
pub use crate::runtime::{AutomatonConfig, EventSourceConfig, RuntimeConfig};
#[cfg(feature = "messaging")]
pub use crate::runtime::{HandoffRequest, InstanceMode, RuntimeCoordination};
pub use crate::runtime::{RuntimeInstance, RuntimeVersion};

// Lifecycle management
#[cfg(feature = "messaging")]
pub use crate::runtime::{IngestorState, SourceDriver, SourceDriverRuntime};

// Event handling
#[cfg(feature = "messaging")]
pub use crate::runtime::{EventSender, EventStream};

// Error types
pub use crate::runtime::{RuntimeResult, SinexError};

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
