//! Unified Event Model
//!
//! Re-exported from sinex-primitives for legacy compatibility.

pub use crate::models::event_builder::{
    EventBuilder, HasProvenance, NoProvenance, OffsetKind, Operation, Provenance,
};
pub use sinex_primitives::events::{Event, EventId, OptionalTimestamp, SourceMaterial, Timestamp};

// Helper for legacy code that might expect get_hostname here
pub use sinex_primitives::events::builder::{get_hostname, get_ingestor_version};
