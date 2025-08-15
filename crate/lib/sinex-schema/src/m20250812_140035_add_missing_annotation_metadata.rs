use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Add metadata column to event_annotations
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.event_annotations 
                ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb
            "#,
            )
            .await?;

        // 2. Add missing columns to entities table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.entities 
                ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'unknown',
                ADD COLUMN IF NOT EXISTS canonical_name TEXT NOT NULL DEFAULT 'unknown',
                ADD COLUMN IF NOT EXISTS aliases TEXT[] DEFAULT '{}',
                ADD COLUMN IF NOT EXISTS description TEXT,
                ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS merged_into_id UUID
            "#,
            )
            .await?;

        // 3. Add missing columns to entity_relations table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.entity_relations 
                ADD COLUMN IF NOT EXISTS from_entity_id UUID,
                ADD COLUMN IF NOT EXISTS to_entity_id UUID,
                ADD COLUMN IF NOT EXISTS relation_type TEXT NOT NULL DEFAULT 'related',
                ADD COLUMN IF NOT EXISTS properties JSONB DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS confidence REAL DEFAULT 1.0,
                ADD COLUMN IF NOT EXISTS valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS created_from_event_id ULID
            "#,
            )
            .await?;

        // 4. Add missing columns to operations_log table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.operations_log 
                ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'system',
                ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'unknown',
                ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'pending',
                ADD COLUMN IF NOT EXISTS outcome TEXT,
                ADD COLUMN IF NOT EXISTS preview_summary TEXT,
                ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS error_details JSONB
            "#,
            )
            .await?;

        // 5. Fix source_material_registry to use 'id' instead of 'blob_id'
        // First check if blob_id exists and rename it
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DO $$ 
                BEGIN 
                    IF EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_schema = 'raw' 
                        AND table_name = 'source_material_registry' 
                        AND column_name = 'blob_id'
                    ) THEN
                        ALTER TABLE raw.source_material_registry 
                        RENAME COLUMN blob_id TO id;
                    END IF;
                END $$;
            "#,
            )
            .await?;

        // 6. Add material_type column
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD COLUMN IF NOT EXISTS material_type TEXT NOT NULL DEFAULT 'generic',
                ADD COLUMN IF NOT EXISTS content_preview TEXT
            "#,
            )
            .await?;

        // 7. Add unique constraint for processor_checkpoints
        let _ = manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_checkpoints 
                ADD CONSTRAINT unique_processor_consumer 
                UNIQUE (processor_name, consumer_group, consumer_name)
            "#,
            )
            .await; // Ignore if already exists

        // 8. Add missing columns to processor_manifests
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.processor_manifests 
                ADD COLUMN IF NOT EXISTS processor_name TEXT NOT NULL DEFAULT 'unknown',
                ADD COLUMN IF NOT EXISTS capabilities JSONB DEFAULT '{}'::jsonb
            "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Reverse all the changes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.event_annotations DROP COLUMN IF EXISTS metadata;
                ALTER TABLE core.entities 
                    DROP COLUMN IF EXISTS name,
                    DROP COLUMN IF EXISTS canonical_name,
                    DROP COLUMN IF EXISTS aliases,
                    DROP COLUMN IF EXISTS description,
                    DROP COLUMN IF EXISTS metadata,
                    DROP COLUMN IF EXISTS created_at,
                    DROP COLUMN IF EXISTS updated_at,
                    DROP COLUMN IF EXISTS merged_into_id;
                ALTER TABLE core.entity_relations 
                    DROP COLUMN IF EXISTS from_entity_id,
                    DROP COLUMN IF EXISTS to_entity_id,
                    DROP COLUMN IF EXISTS relation_type,
                    DROP COLUMN IF EXISTS properties,
                    DROP COLUMN IF EXISTS confidence,
                    DROP COLUMN IF EXISTS valid_from,
                    DROP COLUMN IF EXISTS valid_until,
                    DROP COLUMN IF EXISTS created_at,
                    DROP COLUMN IF EXISTS updated_at,
                    DROP COLUMN IF EXISTS created_from_event_id;
                ALTER TABLE core.operations_log 
                    DROP COLUMN IF EXISTS actor,
                    DROP COLUMN IF EXISTS scope,
                    DROP COLUMN IF EXISTS state,
                    DROP COLUMN IF EXISTS outcome,
                    DROP COLUMN IF EXISTS preview_summary,
                    DROP COLUMN IF EXISTS started_at,
                    DROP COLUMN IF EXISTS completed_at,
                    DROP COLUMN IF EXISTS error_details;
                ALTER TABLE raw.source_material_registry 
                    RENAME COLUMN id TO blob_id;
                ALTER TABLE raw.source_material_registry 
                    DROP COLUMN IF EXISTS material_type,
                    DROP COLUMN IF EXISTS content_preview;
                ALTER TABLE core.processor_checkpoints 
                    DROP CONSTRAINT IF EXISTS unique_processor_consumer;
                ALTER TABLE core.processor_manifests 
                    DROP COLUMN IF EXISTS processor_name,
                    DROP COLUMN IF EXISTS capabilities;
            "#,
            )
            .await?;

        Ok(())
    }
}
