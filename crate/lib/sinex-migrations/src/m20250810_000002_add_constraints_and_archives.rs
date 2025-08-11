use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Phase 1.3: Minimal Database Constraints and Archive Infrastructure

        // 1. Add missing audit fields to existing core.archived_events table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.archived_events 
                ADD COLUMN IF NOT EXISTS archived_by TEXT,
                ADD COLUMN IF NOT EXISTS superseded_by_event_id ULID,
                ADD COLUMN IF NOT EXISTS operation_id ULID;
                "#,
            )
            .await?;

        // 2. Add Provenance XOR CHECK constraint to core.events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'events_provenance_xor'
                    ) THEN
                        ALTER TABLE core.events
                        ADD CONSTRAINT events_provenance_xor CHECK (
                            (source_material_id IS NOT NULL AND source_event_ids IS NULL)
                            OR
                            (source_material_id IS NULL AND source_event_ids IS NOT NULL)
                        );
                    END IF;
                END $$;
                "#,
            )
            .await?;

        // 3. Add index for idempotency checks on first-order events
        // Note: TimescaleDB hypertables don't support unique constraints without partitioning column
        // Idempotency must be enforced at the application level (in ingestd)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS ix_events_material_anchor
                ON core.events(source_material_id, anchor_byte)
                WHERE source_material_id IS NOT NULL;
                "#,
            )
            .await?;

        // 4. Add GIN index for provenance traversal and cascade planning
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS ix_events_source_event_ids
                ON core.events USING GIN (source_event_ids);
                "#,
            )
            .await?;

        // 5. Add indexes for archived_events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS ix_archived_events_operation_id 
                ON core.archived_events (operation_id) 
                WHERE operation_id IS NOT NULL;
                
                CREATE INDEX IF NOT EXISTS ix_archived_events_superseded_by 
                ON core.archived_events (superseded_by_event_id) 
                WHERE superseded_by_event_id IS NOT NULL;
                
                CREATE INDEX IF NOT EXISTS ix_archived_events_archived_by 
                ON core.archived_events (archived_by) 
                WHERE archived_by IS NOT NULL;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove constraints and indexes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS events_provenance_xor;
                DROP INDEX IF EXISTS ix_events_material_anchor;
                DROP INDEX IF EXISTS ix_events_source_event_ids;
                "#,
            )
            .await?;

        // Remove added columns from archived_events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.archived_events 
                DROP COLUMN IF EXISTS archived_by,
                DROP COLUMN IF EXISTS superseded_by_event_id,
                DROP COLUMN IF EXISTS operation_id;
                "#,
            )
            .await?;

        Ok(())
    }
}
