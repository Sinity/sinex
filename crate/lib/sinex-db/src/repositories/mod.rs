#![doc = include_str!("../../docs/db_repositories.md")]
//! See `docs/db_repositories.md` for the repository architecture overview.
pub mod blobs;
// pub mod checkpoints; // Removed
pub mod common;
pub mod embeddings;
pub mod events;
pub mod events_extensions;
pub mod gitops;
pub mod integrity;
pub mod knowledge_graph;
pub mod replay;
pub mod schema_cache;
pub mod schema_management;
pub mod source_materials;
pub mod state;

// Re-export main types
pub use blobs::{BlobRepository, StorageStats};
// pub use checkpoints::{Checkpoint, CheckpointExt, CheckpointRecord, CheckpointRepository}; // Removed
pub use common::{DbResult, EnhancedRepository, Repository, TableDef, TransactionSupport};
pub use embeddings::{
    CachedEmbeddingHit, EmbeddingModelRecord, EmbeddingRepository, HybridSearchResult,
    SimilarityResult,
};
pub use events::{
    COPY_BATCH_THRESHOLD, EventAnnotation, EventPayloadSchema, EventRepository, EventRepositoryTx,
    ReplacementKind, ReplacementRecord, StreamBatchInsertResult, StreamBatchRow,
};
pub use gitops::{GitOpsRepository, GitOpsSourceRecord};
pub use knowledge_graph::{
    CreateEntity, CreateEntityRelation, EntityExt, EntityRecord, EntityRelationExt,
    EntityRelationRecord, EntityType, KnowledgeGraphRepository,
};
pub use schema_cache::{CachedSchema, SchemaCacheRepository};
pub use schema_management::{
    NewEventSchema, SchemaManagementRepository, SchemaStatistics, ValidationError, ValidationResult,
};
pub use source_materials::{
    SourceMaterial, SourceMaterialExt, SourceMaterialLink, SourceMaterialLinkRecord,
    SourceMaterialRepository, TemporalLedgerEntry, material_kinds, material_types,
    relation_types as source_material_relation_types, status as material_status, timing_info_types,
};
pub use state::{
    Operation, OperationRecord, OperationStatistics, StateRepository, SystemHealthReport,
};
pub use integrity::IntegrityRepository;
pub use replay::ReplayRepository;

use sqlx::PgPool;

/// Extension trait for `PgPool` to provide ergonomic repository access
///
/// This trait allows you to access repositories directly from a pool:
/// ```rust
/// let event = pool.events().get_by_id(event_id).await?;
/// // let checkpoint = pool.checkpoints().get_latest(node_name).await?; // Removed
/// let schema = pool.schemas().get_active_schema(source, event_type).await?;
/// ```
pub trait DbPoolExt {
    fn blobs(&self) -> blobs::BlobRepository;
    fn embeddings(&self) -> embeddings::EmbeddingRepository<'_>;
    fn events(&self) -> events::EventRepository<'_>;
    fn gitops(&self) -> gitops::GitOpsRepository<'_>;
    fn source_materials(&self) -> source_materials::SourceMaterialRepository<'_>;
    fn knowledge_graph(&self) -> knowledge_graph::KnowledgeGraphRepository<'_>;
    fn state(&self) -> state::StateRepository<'_>;
    fn schemas(&self) -> schema_management::SchemaManagementRepository<'_>;
    fn schema_cache(&self) -> schema_cache::SchemaCacheRepository<'_>;
    fn replay(&self) -> replay::ReplayRepository<'_>;
    fn integrity(&self) -> integrity::IntegrityRepository<'_>;
    async fn with_transaction<F, T>(&self, f: F) -> crate::DbResult<T>
    where
        F: for<'tx> AsyncFnOnce(&'tx mut crate::DbTransaction<'_>) -> crate::DbResult<T>;
}

impl DbPoolExt for PgPool {
    fn blobs(&self) -> blobs::BlobRepository {
        blobs::BlobRepository::new(self.clone())
    }

    fn embeddings(&self) -> embeddings::EmbeddingRepository<'_> {
        embeddings::EmbeddingRepository::new(self)
    }

    fn events(&self) -> events::EventRepository<'_> {
        events::EventRepository::new(self)
    }

    fn gitops(&self) -> gitops::GitOpsRepository<'_> {
        gitops::GitOpsRepository::new(self)
    }

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

    fn replay(&self) -> replay::ReplayRepository<'_> {
        replay::ReplayRepository::new(self)
    }

    fn integrity(&self) -> integrity::IntegrityRepository<'_> {
        integrity::IntegrityRepository::new(self)
    }

    async fn with_transaction<F, T>(&self, f: F) -> crate::DbResult<T>
    where
        F: for<'tx> AsyncFnOnce(&'tx mut crate::DbTransaction<'_>) -> crate::DbResult<T>,
    {
        crate::query_helpers::with_transaction(self, f).await
    }
}
