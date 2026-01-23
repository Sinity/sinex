use sea_orm_migration::prelude::*;

use crate::schema::{
    embeddings::{
        EmbeddingCache, EmbeddingModels, EventClusterMembers, EventClusters, EventEmbeddings,
    },
    TableDef,
};

#[derive(DeriveMigrationName)]
pub struct Migration;

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

        // Create standard indexes
        for index in EmbeddingModels::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in EmbeddingCache::create_indexes() {
            manager.create_index(index).await?;
        }
        for index in EventEmbeddings::create_indexes() {
            manager.create_index(index).await?;
        }

        // Create vector indexes (HNSW) via raw SQL
        for sql in EmbeddingCache::create_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(sql.as_str())
                .await?;
        }
        for sql in EventEmbeddings::create_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(sql.as_str())
                .await?;
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
