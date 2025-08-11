//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-satellite-sdk crate, allowing for more ergonomic imports:
//!
//! ```rust
//! use sinex_satellite_sdk::prelude::*;
//!
//! // Instead of:
//! // use sinex_satellite_sdk::{StatefulStreamProcessor, CheckpointManager, SatelliteCoordination};
//! // use sinex_satellite_sdk::{IngestClient, BatchResult, HealthStatus};
//! // use sinex_satellite_sdk::{SatelliteConfig, TimeHorizon, Checkpoint};
//! ```

// Core processor traits and types
pub use crate::{Checkpoint, CheckpointManager, CheckpointState};
pub use crate::{ProcessorCapabilities, ProcessorType, ScanArgs, ScanEstimate, ScanReport};
pub use crate::{StatefulStreamProcessor, StreamProcessorContext, TimeHorizon};

// Configuration and coordination
pub use crate::{AutomatonConfig, EventSourceConfig, SatelliteConfig};
pub use crate::{HandoffRequest, InstanceMode, SatelliteCoordination};
pub use crate::{SatelliteInstance, SatelliteVersion};

// gRPC client types
pub use crate::{BatchResult, GrpcClientConfig, HealthStatus, IngestClient};

// Lifecycle management
pub use crate::{LifecycleManager, ProcessorMode, ProcessorRunner, ServiceStatus};

// Event handling
pub use crate::{EventSender, EventStream, ReplayMode};

// CLI and utilities
pub use crate::{ProcessorCli, ProcessorCommand, SatelliteArgs};

// Error types
pub use crate::{SatelliteError, SatelliteResult};

// Common re-exports from dependencies
pub use crate::{RawEvent, SinexError, Ulid};
