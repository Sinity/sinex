//! Schema definitions for schema management tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum EventPayloadSchemas {
    Table,
    Id,
}

#[derive(Iden)]
pub enum SchemaCompatibility {
    Table,
    Id,
}

#[derive(Iden)]
pub enum GitopsSchemaSource {
    Table,
    Id,
}

#[derive(Iden)]
pub enum ValidationCache {
    Table,
    Id,
}

impl EventPayloadSchemas {
    pub fn create_table() -> String {
        Table::create()
            .table(EventPayloadSchemas::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventPayloadSchemas::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl SchemaCompatibility {
    pub fn create_table() -> String {
        Table::create()
            .table(SchemaCompatibility::Table)
            .if_not_exists()
            .col(ColumnDef::new(SchemaCompatibility::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl GitopsSchemaSource {
    pub fn create_table() -> String {
        Table::create()
            .table(GitopsSchemaSource::Table)
            .if_not_exists()
            .col(ColumnDef::new(GitopsSchemaSource::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl ValidationCache {
    pub fn create_table() -> String {
        Table::create()
            .table(ValidationCache::Table)
            .if_not_exists()
            .col(ColumnDef::new(ValidationCache::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}