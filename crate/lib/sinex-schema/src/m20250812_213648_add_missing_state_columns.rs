use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add missing columns to core.operations_log table
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("operations_log")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("approved_by")).text().null(),
                    )
                    .to_owned(),
            )
            .await?;

        // Add missing columns to core.processor_manifests table
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_manifests")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("input_schemas")).json().null(),
                    )
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("output_schemas")).json().null(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop added columns
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("operations_log")))
                    .drop_column(Alias::new("approved_by"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("processor_manifests")))
                    .drop_column(Alias::new("input_schemas"))
                    .drop_column(Alias::new("output_schemas"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
