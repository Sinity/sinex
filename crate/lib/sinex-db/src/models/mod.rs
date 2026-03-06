//! Domain models that are tightly coupled to database operations

pub mod blob;

pub use blob::Blob;
pub use sinex_primitives::domain::{Entity, EntityRelation};
pub use sinex_primitives::events::payload::DynamicPayload;
pub use sinex_primitives::events::{Event, EventId, SourceMaterial};
pub use sinex_primitives::events::{
    EventBuilder, HasProvenance, NoProvenance, OffsetKind, Operation, Provenance,
};
