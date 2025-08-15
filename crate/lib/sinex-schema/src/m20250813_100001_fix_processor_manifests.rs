//! Fix processor_manifests table - add missing columns

use sea_query::{ColumnDef, Iden, PostgresQueryBuilder, Query, Table};
use sea_query_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add processor_type column
        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(ProcessorManifests::ProcessorType)
                            .text()
                            .not_null()
                            .default("automation")
                    )
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        // Add description column
        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(ProcessorManifests::Description)
                            .text()
                    )
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        // Add updated_at column
        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(ProcessorManifests::UpdatedAt)
                            .timestamp_with_time_zone()
                            .default("CURRENT_TIMESTAMP")
                    )
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        // Add created_at column
        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .add_column_if_not_exists(
                        ColumnDef::new(ProcessorManifests::CreatedAt)
                            .timestamp_with_time_zone()
                            .not_null()
                            .default("CURRENT_TIMESTAMP")
                    )
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop columns in reverse order
        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .drop_column(ProcessorManifests::CreatedAt)
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .drop_column(ProcessorManifests::UpdatedAt)
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .drop_column(ProcessorManifests::Description)
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(ProcessorManifests::Table)
                    .drop_column(ProcessorManifests::ProcessorType)
                    .to_string(PostgresQueryBuilder),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum ProcessorManifests {
    Table,
    ProcessorType,
    Description,
    UpdatedAt,
    CreatedAt,
}