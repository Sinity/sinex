//! Schema definitions for processors and coordination tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum ProcessorCheckpoints {
    Table,
    Id,
}

#[derive(Iden)]
pub enum ProcessorManifests {
    Table,
    Id,
}

#[derive(Iden)]
pub enum OperationsLog {
    Table,
    Id,
}

impl ProcessorCheckpoints {
    pub fn create_table() -> String {
        Table::create()
            .table(ProcessorCheckpoints::Table)
            .if_not_exists()
            .col(ColumnDef::new(ProcessorCheckpoints::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl ProcessorManifests {
    pub fn create_table() -> String {
        Table::create()
            .table(ProcessorManifests::Table)
            .if_not_exists()
            .col(ColumnDef::new(ProcessorManifests::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}

impl OperationsLog {
    pub fn create_table() -> String {
        Table::create()
            .table(OperationsLog::Table)
            .if_not_exists()
            .col(ColumnDef::new(OperationsLog::Id).uuid().not_null().primary_key())
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}