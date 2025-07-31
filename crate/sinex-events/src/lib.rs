//! Core Event Types and Builders
//!
//! This crate provides the fundamental event types and builders used throughout
//! the Sinex system, extracted from sinex-core for focused responsibility.

// Re-export event-related macros
#[cfg(feature = "macros")]
pub use sinex_macros::{event_registry, typed_event_envelope, EventPayload};

pub mod blanket_impls;
pub mod event_payload;
pub mod payloads;
pub mod schema_registry;
pub mod version;

#[cfg(test)]
pub mod test_helpers;

// Re-export core event types
pub use event_payload::{EventPayload, JsonValue, OptionalTimestamp, Timestamp};
pub use sinex_types::domain::{EventSource, EventType};

// Re-export payloads
pub use payloads::*;

// Re-export version utilities
pub use version::{VersionRegistry, Versioned};

// Re-export schema registry initialization
pub use schema_registry::{initialize_schema_cache, preload_schemas};

// Note: Event type has been moved to sinex-db as it's fundamentally a database type
// Event sender/receiver types should be defined where Event is used

// Agent status and error types
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AutomatonStatus {
    Starting,
    Running,
    Stopping,
    Error,
}

pub type AgentStatus = AutomatonStatus;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}
