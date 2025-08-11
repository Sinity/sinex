//! Schema definitions for knowledge graph tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum ArchivedEvents {
    Table,
    Id,
}

impl ArchivedEvents {
    pub fn create_table() -> String {
        Table::create()
            .table(ArchivedEvents::Table)
            .if_not_exists()
            .col(ColumnDef::new(ArchivedEvents::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}