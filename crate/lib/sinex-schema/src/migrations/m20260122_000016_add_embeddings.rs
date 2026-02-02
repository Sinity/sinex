use sea_orm_migration::prelude::*;

use crate::schema::{
    embeddings::{
        EmbeddingCache, EmbeddingModels, EventClusterMembers, EventClusters, EventEmbeddings,
    },
    TableDef,
};

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Enable pgvector extension
        manager
            .get_connection()
            .execute_unprepared("CREATE EXTENSION IF NOT EXISTS vector")
            .await?;

        // Create tables
        manager
            .create_table(EmbeddingModels::create_table_statement())
            .await?;
        manager
            .create_table(EmbeddingCache::create_table_statement())
            .await?;
        manager
            .create_table(EventEmbeddings::create_table_statement())
            .await?;
        manager
            .create_table(EventClusters::create_table_statement())
            .await?;
        manager
            .create_table(EventClusterMembers::create_table_statement())
            .await?;

        // Create all indexes via raw SQL with IF NOT EXISTS for idempotency
        let index_statements = vec![
            // EmbeddingModels indexes
            "CREATE UNIQUE INDEX IF NOT EXISTS uk_embedding_models_provider_model ON core.embedding_models (provider, model_name)",
            // EmbeddingCache indexes
            "CREATE UNIQUE INDEX IF NOT EXISTS uk_embedding_cache_hash_model ON core.embedding_cache (text_hash, embedding_model_id)",
            "CREATE INDEX IF NOT EXISTS ix_embedding_cache_vector ON core.embedding_cache USING hnsw (embedding vector_cosine_ops)",
            // EventEmbeddings indexes
            "CREATE UNIQUE INDEX IF NOT EXISTS uk_event_embeddings_event_model ON core.event_embeddings (event_id, embedding_model_id)",
            "CREATE INDEX IF NOT EXISTS ix_event_embeddings_vector ON core.event_embeddings USING hnsw (embedding vector_cosine_ops)",
        ];

        for sql in index_statements {
            manager.get_connection().execute_unprepared(sql).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop tables in reverse order of dependencies
        manager
            .drop_table(
                Table::drop()
                    .table(EventClusterMembers::table_iden())
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(EventClusters::table_iden()).to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(EventEmbeddings::table_iden())
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(EmbeddingCache::table_iden()).to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(EmbeddingModels::table_iden())
                    .to_owned(),
            )
            .await?;

        // We generally don't drop the extension as other things might rely on it,
        // but for a clean rollback of this migration specifically:
        // manager.get_connection().execute_unprepared("DROP EXTENSION IF EXISTS vector").await?;

        Ok(())
    }
}
