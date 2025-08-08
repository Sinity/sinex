//! Domain models that are tightly coupled to database operations

pub mod blob;
pub mod event;
pub mod knowledge_graph;

pub use blob::{Blob, BlobRecord};
pub use event::{Provenance, RawEvent, SourceMaterial};
pub use knowledge_graph::{Entity, EntityRelation};
