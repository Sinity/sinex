pub mod checkpoints;
/// Repository pattern implementation for database access
///
/// This module provides a clean, type-safe interface to the database using
/// a hybrid approach:
/// - Direct sqlx queries for static, performance-critical operations
/// - SeaQuery for dynamic query building
///
/// Each repository follows the same pattern and provides both approaches
/// where appropriate.
pub mod common;
pub mod events;
pub mod knowledge_graph;
pub mod source_materials;
pub mod state;

// Re-export main types
pub use checkpoints::{Checkpoint, CheckpointRepository, NewCheckpoint};
pub use common::{DbResult, EventSearchFilters, Repository, TransactionSupport};
pub use events::{
    CommandCount, EventAnnotation, EventPayloadSchema, EventRepository, EventTypeCount, NewEvent,
    NewSchema, SourceActivity,
};
pub use knowledge_graph::{
    Entity, EntityRelation, EntityType, KnowledgeGraphRepository, NewEntity, NewEntityRelation,
};
pub use source_materials::{NewSourceMaterial, SourceMaterial, SourceMaterialRepository};
pub use state::{
    NewOperation, Operation, OperationResult, OperationStatistics, OperationType, StateRepository,
};
