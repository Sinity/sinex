//! Schema definitions for embeddings and ML tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum EmbeddingCache {
    #[iden = "embedding_cache"]
    Table,
    Id,
}

#[derive(Iden)]
pub enum EmbeddingModels {
    Table,
    Id,
}

#[derive(Iden)]
pub enum EventEmbeddings {
    Table,
    Id,
}

#[derive(Iden)]
pub enum EventClusters {
    Table,
    Id,
    ClusterName,
    ClusterType,
    Metadata,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
pub enum EventClusterMembers {
    Table,
    Id,
}

impl EmbeddingCache {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EmbeddingCache::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EmbeddingCache::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EmbeddingModels {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EmbeddingModels::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EmbeddingModels::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventEmbeddings {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EventEmbeddings::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventEmbeddings::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventClusters {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EventClusters::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventClusters::Id)
                    .custom(Alias::new("ULID"))
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(EventClusters::ClusterName).text())
            .col(ColumnDef::new(EventClusters::ClusterType).text())
            .col(ColumnDef::new(EventClusters::Metadata).json_binary())
            .col(
                ColumnDef::new(EventClusters::CreatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .col(
                ColumnDef::new(EventClusters::UpdatedAt)
                    .timestamp_with_time_zone()
                    .not_null()
                    .default(Expr::current_timestamp()),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventClusterMembers {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("core"), EventClusterMembers::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(EventClusterMembers::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
