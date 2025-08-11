//! Schema definitions for event relations tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum EventRelations {
    Table,
    Id,
}

impl EventRelations {
    pub fn create_table() -> String {
        Table::create()
            .table(EventRelations::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventRelations::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}