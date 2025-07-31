//! Sinex - Unified facade for the Sinex event capture system
//!
//! This crate provides a single, well-organized entry point to the entire Sinex ecosystem.
//! Instead of importing from multiple crates, you can use `sinex::` for everything.
//!
//! # Quick Start
//!
//! ```rust
//! use sinex::prelude::*;
//! ```
//!
//! # Feature Flags
//!
//! - `default` = `["standard"]` - Common functionality with database
//! - `core` - Minimal: just types and events
//! - `standard` - Adds database, telemetry, preflight
//! - `satellite` - For building satellites (includes standard + SDK + NATS)
//! - `services` - Business logic layer (includes standard)
//! - `annex` - Blob storage support (includes standard)
//! - `test` - Test utilities (includes standard)
//! - `full` - Everything enabled

// External re-exports - always available
pub use chrono;
pub use serde;
pub use serde_json;
pub use ulid;

// External re-exports - conditional
#[cfg(feature = "standard")]
pub use anyhow;
#[cfg(feature = "standard")]
pub use async_trait;
#[cfg(feature = "standard")]
pub use futures;
#[cfg(feature = "standard")]
pub use sqlx;
#[cfg(feature = "standard")]
pub use thiserror;
#[cfg(feature = "standard")]
pub use tokio;
#[cfg(feature = "standard")]
pub use tracing;
#[cfg(feature = "standard")]
pub use uuid;

/// Core types from sinex-types
pub mod types {
    pub use sinex_types::*;
}

/// Domain-specific string types
pub mod domain {
    pub use sinex_types::domain::*;
}

/// Event system from sinex-events
pub mod events {
    pub use sinex_events::*;
}

/// Database layer (requires 'standard' or higher feature)
#[cfg(feature = "standard")]
pub mod db {
    pub use sinex_db::*;
}

/// Telemetry and observability (requires 'standard' or higher feature)
#[cfg(feature = "standard")]
pub mod telemetry {
    pub use sinex_telemetry::*;
}

/// Preflight checks (requires 'standard' or higher feature)
#[cfg(feature = "standard")]
pub mod preflight {
    pub use sinex_preflight::*;
}

/// Satellite SDK (requires 'satellite' feature)
#[cfg(feature = "satellite")]
pub mod satellite {
    pub use sinex_satellite_sdk::*;
}

/// NATS messaging (requires 'satellite' feature)
#[cfg(feature = "satellite")]
pub mod nats {
    pub use sinex_nats::*;
}

/// Business services layer (requires 'services' feature)
#[cfg(feature = "services")]
pub mod services {
    pub use sinex_services::*;
}

/// Blob/annex storage (requires 'annex' feature)
#[cfg(feature = "annex")]
pub mod annex {
    pub use sinex_annex::*;
}

/// Test utilities (requires 'test' feature)
#[cfg(feature = "test")]
pub mod test {
    pub use sinex_test_utils::*;
}

/// Error types
pub mod error {
    pub use sinex_types::error::*;
    pub use sinex_types::{Result, SinexError, SinexResult};
}

/// System constants
pub mod constants {
    pub use sinex_types::{
        buffers, filesystem, limits, redis, retry, services, timeouts, validation_constants,
    };
}

/// Utility functions
pub mod utils {
    pub use sinex_types::{
        json_utils, path_utils, sanitize_filename_component, validate_json, validate_path,
    };
}

/// Common imports for convenience
pub mod prelude {
    // Core types - always available
    pub use crate::domain::{EventSource, EventType, HostName};
    pub use crate::error::{Result, SinexError, SinexResult};
    pub use crate::events::{event::EventBuilder, Event, Provenance};
    pub use crate::types::{Id, JsonValue, OptionalTimestamp, Timestamp, Ulid};

    // External re-exports - always available
    pub use chrono::{DateTime, Utc};
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::{json, Value};

    // Database types - with standard feature
    #[cfg(feature = "standard")]
    pub use crate::db::repositories::DbPoolExt;
    #[cfg(feature = "standard")]
    pub use crate::db::{DbPool, DbPoolRef};
}

// Convenience re-exports at crate root for the most common types
pub use domain::{EventSource, EventType, HostName};
pub use error::{Result, SinexError, SinexResult};
pub use events::{event::EventBuilder, Event, Provenance};
pub use types::{Id, JsonValue, OptionalTimestamp, Timestamp, Ulid};

// Database re-exports at root (when feature enabled)
#[cfg(feature = "standard")]
pub use db::repositories::DbPoolExt;
#[cfg(feature = "standard")]
pub use db::{DbPool, DbPoolRef};
