use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the problematic index that includes 'id' field, defeating idempotency
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS core.idx_events_material_anchor;")
            .await?;

        // Create the correct partial unique index on (source_material_id, anchor_byte)
        // without the 'id' field to properly enforce idempotency
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE UNIQUE INDEX idx_events_material_anchor_idempotent 
                ON core.events (source_material_id, anchor_byte) 
                WHERE source_material_id IS NOT NULL AND anchor_byte IS NOT NULL;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the corrected index
        manager
            .get_connection()
            .execute_unprepared("DROP INDEX IF EXISTS core.idx_events_material_anchor_idempotent;")
            .await?;

        // Restore the original problematic index (for rollback compatibility)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE UNIQUE INDEX idx_events_material_anchor 
                ON core.events (source_material_id, anchor_byte, id) 
                WHERE source_material_id IS NOT NULL AND anchor_byte IS NOT NULL;
                "#,
            )
            .await?;

        Ok(())
    }
}
