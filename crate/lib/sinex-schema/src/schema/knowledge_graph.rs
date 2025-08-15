//! Schema definitions for knowledge graph tables

use sea_orm_migration::prelude::*;

#[derive(Iden)]
pub enum ArchivedEvents {
    Table,
    Id,
}

impl ArchivedEvents {
    pub fn create_table() -> String {
        Table::create()
            .table((Alias::new("audit"), ArchivedEvents::Table))
            .if_not_exists()
            .col(
                ColumnDef::new(ArchivedEvents::Id)
                    .uuid()
                    .not_null()
                    .primary_key(),
            )
            .to_string(PostgresQueryBuilder)
    }

    pub fn create_indexes() -> Vec<String> {
        vec![]
    }
}
