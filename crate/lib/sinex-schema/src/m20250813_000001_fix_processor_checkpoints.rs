use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing columns to processor_checkpoints table
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_checkpoints")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("processor_name"))
                            .text()
                            .not_null()
                            .default("unnamed"),
                    )
                    .add_column_if_not_exists(ColumnDef::new(Alias::new("consumer_group")).text())
                    .add_column_if_not_exists(ColumnDef::new(Alias::new("consumer_name")).text())
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("last_processed_id")).custom(Alias::new("ULID")),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("last_processed_ts")).timestamp_with_time_zone(),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("processed_count"))
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("checkpoint_data")).json_binary(),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("state_data")).json_binary(),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("checkpoint_version"))
                            .integer()
                            .not_null()
                            .default(1),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("last_activity"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("created_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("updated_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Create index on processor_name
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_processor_checkpoints_processor_name")
                    .table((Alias::new("core"), Alias::new("processor_checkpoints")))
                    .col(Alias::new("processor_name"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the added columns (except id which was already there)
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_checkpoints")))
                    .drop_column(Alias::new("processor_name"))
                    .drop_column(Alias::new("consumer_group"))
                    .drop_column(Alias::new("consumer_name"))
                    .drop_column(Alias::new("last_processed_id"))
                    .drop_column(Alias::new("last_processed_ts"))
                    .drop_column(Alias::new("processed_count"))
                    .drop_column(Alias::new("checkpoint_data"))
                    .drop_column(Alias::new("state_data"))
                    .drop_column(Alias::new("checkpoint_version"))
                    .drop_column(Alias::new("last_activity"))
                    .drop_column(Alias::new("created_at"))
                    .drop_column(Alias::new("updated_at"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
