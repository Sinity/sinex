//! Add missing columns for source_material_registry, temporal_ledger, and event_payload_schemas

use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add proximity_hint and metadata columns to temporal_ledger
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.temporal_ledger 
                ADD COLUMN IF NOT EXISTS proximity_hint JSONB DEFAULT '{}',
                ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'
                "#,
            )
            .await?;

        // Add content_hash and event_types columns to event_payload_schemas
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE sinex_schemas.event_payload_schemas 
                ADD COLUMN IF NOT EXISTS content_hash TEXT,
                ADD COLUMN IF NOT EXISTS event_types TEXT[] DEFAULT ARRAY[]::TEXT[],
                ADD COLUMN IF NOT EXISTS description TEXT
                "#,
            )
            .await?;

        // Add missing columns to source_material_registry if needed
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Ensure source_material_id column exists (rename if needed)
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'raw' 
                        AND table_name = 'source_material_registry' 
                        AND column_name = 'source_material_id'
                    ) THEN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw' 
                            AND table_name = 'source_material_registry' 
                            AND column_name = 'blob_id'
                        ) THEN
                            ALTER TABLE raw.source_material_registry 
                            RENAME COLUMN blob_id TO source_material_id;
                        ELSIF EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'raw' 
                            AND table_name = 'source_material_registry' 
                            AND column_name = 'id'
                        ) THEN
                            ALTER TABLE raw.source_material_registry 
                            RENAME COLUMN id TO source_material_id;
                        END IF;
                    END IF;
                END $$;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove added columns
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.temporal_ledger 
                DROP COLUMN IF EXISTS proximity_hint,
                DROP COLUMN IF EXISTS metadata;
                
                ALTER TABLE sinex_schemas.event_payload_schemas 
                DROP COLUMN IF EXISTS content_hash,
                DROP COLUMN IF EXISTS event_types,
                DROP COLUMN IF EXISTS description;
                "#,
            )
            .await?;

        Ok(())
    }
}
