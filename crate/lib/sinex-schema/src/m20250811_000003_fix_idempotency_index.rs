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

        // TimescaleDB requires the partitioning column (id) to be included in unique indexes
        // Instead, we'll create a trigger to enforce true idempotency by preventing
        // duplicate (source_material_id, anchor_byte) pairs regardless of id
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION prevent_duplicate_material_anchor()
                RETURNS TRIGGER AS $$
                BEGIN
                    -- Check if there's already an event with the same source_material_id and anchor_byte
                    IF EXISTS (
                        SELECT 1 FROM core.events 
                        WHERE source_material_id = NEW.source_material_id 
                        AND anchor_byte = NEW.anchor_byte
                        AND id != NEW.id
                        AND source_material_id IS NOT NULL 
                        AND anchor_byte IS NOT NULL
                    ) THEN
                        RAISE EXCEPTION 'Duplicate event for source_material_id % and anchor_byte % already exists', 
                            NEW.source_material_id, NEW.anchor_byte;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER trg_prevent_duplicate_material_anchor
                    BEFORE INSERT OR UPDATE ON core.events
                    FOR EACH ROW
                    WHEN (NEW.source_material_id IS NOT NULL AND NEW.anchor_byte IS NOT NULL)
                    EXECUTE FUNCTION prevent_duplicate_material_anchor();
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the trigger and function
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TRIGGER IF EXISTS trg_prevent_duplicate_material_anchor ON core.events;
                DROP FUNCTION IF EXISTS prevent_duplicate_material_anchor();
                "#,
            )
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
