use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add metadata column to event_annotations
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("event_annotations")))
                    .add_column_if_not_exists(ColumnDef::new(Alias::new("metadata")).json_binary())
                    .to_owned(),
            )
            .await?;

        // Add name column to entities
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entities")))
                    .add_column_if_not_exists(
                        ColumnDef::new(Alias::new("name"))
                            .text()
                            .not_null()
                            .default("unnamed"),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the added columns
        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("event_annotations")))
                    .drop_column(Alias::new("metadata"))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table((Alias::new("core"), Alias::new("entities")))
                    .drop_column(Alias::new("name"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
