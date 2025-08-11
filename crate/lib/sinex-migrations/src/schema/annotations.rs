//! Schema definitions for annotations tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum EventAnnotations {
    Table,
    Id,
}

impl EventAnnotations {
    pub fn create_table() -> String {
        Table::create()
            .table(EventAnnotations::Table)
            .if_not_exists()
            .col(ColumnDef::new(EventAnnotations::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

#[derive(Iden)]
pub enum Tags {
    Table,
    Id,
}

impl Tags {
    pub fn create_table() -> String {
        Table::create()
            .table(Tags::Table)
            .if_not_exists()
            .col(ColumnDef::new(Tags::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}