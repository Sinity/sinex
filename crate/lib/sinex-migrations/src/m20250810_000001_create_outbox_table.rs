use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create the outbox table for transactional outbox pattern
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS core.outbox (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    event_id ULID NOT NULL,
                    subject TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                "#,
            )
            .await?;

        // Create index on created_at for efficient processing order
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON core.outbox (created_at);
                "#,
            )
            .await?;

        // Create index on event_id for potential lookups
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