//! Schema definitions for source materials tables

use sea_query::{ColumnDef, Iden, Table};

#[derive(Iden)]
pub enum SourceMaterials {
    Table,
    Id,
}

impl SourceMaterials {
    pub fn create_table() -> String {
        Table::create()
            .table(SourceMaterials::Table)
            .if_not_exists()
            .col(
                ColumnDef::new(SourceMaterials::Id)
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
