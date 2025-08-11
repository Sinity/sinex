//! Domain models that are tightly coupled to database operations

pub mod event;
pub mod knowledge_graph;

pub use event::{Provenance, RawEvent, SourceMaterial};
pub use knowledge_graph::{Entity, EntityRelation};
