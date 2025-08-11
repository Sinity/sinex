//! Schema definitions for entities and knowledge graph tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum Entities {
    Table,
    Id,
}

#[derive(Iden)]
pub enum EntityRelations {
    Table,
    Id,
}

impl Entities {
    pub fn create_table() -> String {
        Table::create()
            .table(Entities::Table)
            .if_not_exists()
            .col(ColumnDef::new(Entities::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl EntityRelations {
    pub fn create_table() -> String {
        Table::create()
            .table(EntityRelations::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(EntityRelations::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
