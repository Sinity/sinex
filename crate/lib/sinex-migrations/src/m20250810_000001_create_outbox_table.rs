use crate::schema::Outbox;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create the outbox table using schema definition
        manager
            .get_connection()
            .execute_unprepared(&Outbox::create_table())
            .await?;

        // Create indexes
        for index_sql in Outbox::create_indexes() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        // Also create index on event_id for potential lookups
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS idx_outbox_event_id ON core.outbox (event_id);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS core.outbox")
            .await?;

        Ok(())
    }
}
