//! Schema definitions for embeddings and ML tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum EmbeddingCache {
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
}

#[derive(Iden)]
pub enum EventClusterMembers {
    Table,
    Id,
}

impl EmbeddingCache {
    pub fn create_table() -> String {
        Table::create()
            .table(EmbeddingCache::Table)
            .if_not_exists()
            .col(ColumnDef::new(EmbeddingCache::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EmbeddingModels {
    pub fn create_table() -> String {
        Table::create()
            .table(EmbeddingModels::Table)
            .if_not_exists()
            .col(ColumnDef::new(EmbeddingModels::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventEmbeddings {
    pub fn create_table() -> String {
        Table::create()
            .table(EventEmbeddings::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventEmbeddings::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventClusters {
    pub fn create_table() -> String {
        Table::create()
            .table(EventClusters::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventClusters::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EventClusterMembers {
    pub fn create_table() -> String {
        Table::create()
            .table(EventClusterMembers::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventClusterMembers::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}