//! Domain models that are tightly coupled to database operations

pub mod blob;
pub mod event;
pub mod event_builder;

pub use blob::Blob;
pub use event::{Event, EventId, SourceMaterial};
pub use event_builder::{
    EventBuilder, HasProvenance, NoProvenance, OffsetKind, Operation, Provenance,
};
pub use sinex_primitives::domain::{Entity, EntityRelation};
pub use sinex_primitives::events::payload::DynamicPayload;
// For convenience when working with JSON events
pub use serde_json::Value as JsonValue;
