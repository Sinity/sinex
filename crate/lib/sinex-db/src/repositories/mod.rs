/// Repository pattern implementation for database access
///
/// This module provides a clean, type-safe interface to the database using
/// a hybrid approach:
/// - Direct sqlx queries for static, performance-critical operations
/// - SeaQuery for dynamic query building
///
/// Each repository follows the same pattern and provides both approaches
/// where appropriate.
pub mod blobs;
pub mod checkpoints;
pub mod common;
pub mod events;
pub mod knowledge_graph;
pub mod source_materials;
pub mod state;

#[cfg(test)]
mod common_test;

// Re-export main types
pub use blobs::{BlobRepository, StorageStats};
pub use checkpoints::{Checkpoint, CheckpointExt, CheckpointRecord, CheckpointRepository};
pub use common::{
    BatchRepository, DbResult, EnhancedRepository, EventSearchFilters, Repository, TableDef,
    TransactionSupport, TransactionalRepository,
};
pub use events::{
    CommandCount, EventAnnotation, EventPayloadSchema, EventRepository, EventTypeCount, NewSchema,
    SourceActivity,
};
pub use knowledge_graph::{
    CreateEntity, CreateEntityRelation, EntityExt, EntityRecord, EntityRelationExt,
    EntityRelationRecord, EntityType, KnowledgeGraphRepository,
};
pub use source_materials::{
    material_types, SourceMaterial, SourceMaterialExt, SourceMaterialRecord,
    SourceMaterialRepository,
};
pub use state::{
    NewOperation, Operation, OperationResult, OperationStatistics, OperationType, StateRepository,
};

use sqlx::PgPool;

/// Extension trait for PgPool to provide ergonomic repository access
///
/// This trait allows you to access repositories directly from a pool:
/// ```rust
/// let event = pool.events().get_by_id(event_id).await?;
/// let checkpoint = pool.checkpoints().get_latest(processor_name).await?;
/// ```
pub trait DbPoolExt {
    fn blobs(&self) -> blobs::BlobRepository;
    fn events(&self) -> events::EventRepository;
    fn checkpoints(&self) -> checkpoints::CheckpointRepository;
    fn source_materials(&self) -> source_materials::SourceMaterialRepository;
    fn knowledge_graph(&self) -> knowledge_graph::KnowledgeGraphRepository;
    fn state(&self) -> state::StateRepository;
}

impl DbPoolExt for PgPool {
    fn blobs(&self) -> blobs::BlobRepository {
        blobs::BlobRepository::new(self.clone())
    }

    fn events(&self) -> events::EventRepository {
        events::EventRepository::new(self)
    }

    fn checkpoints(&self) -> checkpoints::CheckpointRepository {
        checkpoints::CheckpointRepository::new(self)
    }

    fn source_materials(&self) -> source_materials::SourceMaterialRepository {
        source_materials::SourceMaterialRepository::new(self)
    }

    fn knowledge_graph(&self) -> knowledge_graph::KnowledgeGraphRepository {
        knowledge_graph::KnowledgeGraphRepository::new(self)
    }

    fn state(&self) -> state::StateRepository {
        state::StateRepository::new(self)
    }
}
