//! Schema definitions for processors and coordination tables

use sea_query::{ColumnDef, Iden, Table};
use crate::schema::TableDef;

#[derive(Iden, Copy, Clone)]
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
            .col(
                ColumnDef::new(ProcessorCheckpoints::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(sea_query::PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }

    pub fn create_constraints() -> Vec<String> {
        vec![]
    }
}

impl ProcessorManifests {
    pub fn create_table() -> String {
        Table::create()
            .table(ProcessorManifests::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(ProcessorManifests::Id)
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

impl TableDef for ProcessorCheckpoints {
    fn table_name() -> &'static str {
        "processor_checkpoints"
    }

    fn schema_name() -> &'static str {
        "core"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

impl OperationsLog {
    pub fn create_table() -> String {
        Table::create()
            .table(OperationsLog::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(OperationsLog::Id)
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
