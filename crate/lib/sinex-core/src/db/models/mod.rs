//! Domain models that are tightly coupled to database operations

pub mod blob;
pub mod event;
pub mod knowledge_graph;

pub use blob::Blob;
pub use event::{
    Event, EventBuilder, EventId, HasProvenance, NoProvenance, Provenance, SourceMaterial,
};
pub use knowledge_graph::{Entity, EntityRelation};
// For convenience when working with JSON events
pub use serde_json::Value as JsonValue;
