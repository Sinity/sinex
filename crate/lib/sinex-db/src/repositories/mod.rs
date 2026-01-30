#![doc = include_str!("../../docs/db_repositories.md")]
//! See `docs/db_repositories.md` for the repository architecture overview.
pub mod blobs;
// pub mod checkpoints; // Removed
pub mod common;
pub mod events;
pub mod events_extensions;
pub mod knowledge_graph;
pub mod schema_cache;
pub mod schema_management;
pub mod source_materials;
pub mod state;

// Re-export main types
pub use blobs::{BlobRepository, StorageStats};
// pub use checkpoints::{Checkpoint, CheckpointExt, CheckpointRecord, CheckpointRepository}; // Removed
pub use common::{
    DbResult, EnhancedRepository, EventSearchFilters, Repository, TableDef, TransactionSupport,
};
pub use events::{
    CommandCount, EventAnnotation, EventPayloadSchema, EventRepository, EventRepositoryTx,
    EventSearchRow, EventTypeCount, NewSchema, SourceActivity, StreamBatchInsertResult,
    StreamBatchRow,
};
pub use knowledge_graph::{
    CreateEntity, CreateEntityRelation, EntityExt, EntityRecord, EntityRelationExt,
    EntityRelationRecord, EntityType, KnowledgeGraphRepository,
};
pub use schema_cache::{CachedSchema, SchemaCacheRepository};
pub use schema_management::{
    NewEventSchema, SchemaManagementRepository, SchemaStatistics, ValidationError, ValidationResult,
};
pub use source_materials::{
    material_kinds, material_types, status as material_status, timing_info_types, SourceMaterial,
    SourceMaterialExt, SourceMaterialRepository, TemporalLedgerEntry,
};
pub use state::{
    Operation, OperationRecord, OperationStatistics, StateRepository, SystemHealthReport,
};

use sqlx::PgPool;

/// Extension trait for PgPool to provide ergonomic repository access
///
/// This trait allows you to access repositories directly from a pool:
/// ```rust
/// let event = pool.events().get_by_id(event_id).await?;
/// // let checkpoint = pool.checkpoints().get_latest(processor_name).await?; // Removed
/// let schema = pool.schemas().get_active_schema(source, event_type).await?;
/// ```
pub trait DbPoolExt {
    fn blobs(&self) -> blobs::BlobRepository;
    fn events(&self) -> events::EventRepository<'_>;
    // fn checkpoints(&self) -> checkpoints::CheckpointRepository<'_>; // Removed
    fn source_materials(&self) -> source_materials::SourceMaterialRepository<'_>;
    fn knowledge_graph(&self) -> knowledge_graph::KnowledgeGraphRepository<'_>;
    fn state(&self) -> state::StateRepository<'_>;
    fn schemas(&self) -> schema_management::SchemaManagementRepository<'_>;
    fn schema_cache(&self) -> schema_cache::SchemaCacheRepository<'_>;
}

impl DbPoolExt for PgPool {
    fn blobs(&self) -> blobs::BlobRepository {
        blobs::BlobRepository::new(self.clone())
    }

    fn events(&self) -> events::EventRepository<'_> {
        events::EventRepository::new(self)
    }

    // fn checkpoints(&self) -> checkpoints::CheckpointRepository<'_> {
    //     checkpoints::CheckpointRepository::new(self)
    // }

    fn source_materials(&self) -> source_materials::SourceMaterialRepository<'_> {
        source_materials::SourceMaterialRepository::new(self)
    }

    fn knowledge_graph(&self) -> knowledge_graph::KnowledgeGraphRepository<'_> {
        knowledge_graph::KnowledgeGraphRepository::new(self)
    }

    fn state(&self) -> state::StateRepository<'_> {
        state::StateRepository::new(self)
    }

    fn schemas(&self) -> schema_management::SchemaManagementRepository<'_> {
        schema_management::SchemaManagementRepository::new(self)
    }

    fn schema_cache(&self) -> schema_cache::SchemaCacheRepository<'_> {
        schema_cache::SchemaCacheRepository::new(self)
    }
}
