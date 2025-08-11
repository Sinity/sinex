//! Schema definitions for outbox pattern tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum Outbox {
    Table,
    Id,
}

impl Outbox {
    pub fn create_table() -> String {
        Table::create()
            .table(Outbox::Table)
            .if_not_exists()
            .col(ColumnDef::new(Outbox::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
