//! Migration to separate source material IDs from blob IDs
//!
//! This migration:
//! 1. Adds source_material_id as the new primary key
//! 2. Makes blob_id an optional foreign key to core.blobs
//! 3. Removes redundant fields that duplicate blob data
//! 4. Updates foreign key constraints

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Step 1: Add new source_material_id column
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD COLUMN IF NOT EXISTS source_material_id ULID DEFAULT gen_ulid() NOT NULL;
                "#,
            )
            .await?;

        // Step 2: Create unique constraint on source_material_id
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD CONSTRAINT unique_source_material_id UNIQUE (source_material_id);
                "#,
            )
            .await?;

        // Step 3: Drop foreign key constraints first, then drop the old primary key constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events 
                DROP CONSTRAINT IF EXISTS fk_events_source_material;
                "#,
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                DROP CONSTRAINT IF EXISTS source_material_registry_pkey;
                "#,
            )
            .await?;

        // Step 4: Set source_material_id as new primary key
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD CONSTRAINT source_material_registry_pkey PRIMARY KEY (source_material_id);
                "#,
            )
            .await?;

        // Step 5: Rename blob_id to optional_blob_id to make its purpose clear
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                RENAME COLUMN blob_id TO optional_blob_id;
                "#,
            )
            .await?;

        // Step 6: Make optional_blob_id nullable
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ALTER COLUMN optional_blob_id DROP NOT NULL;
                "#,
            )
            .await?;

        // Step 7: Drop redundant columns when blob exists
        // These will be available from core.blobs via the foreign key
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                DROP COLUMN IF EXISTS file_size_bytes,
                DROP COLUMN IF EXISTS checksum_blake3,
                DROP COLUMN IF EXISTS mime_type;
                "#,
            )
            .await?;

        // Step 8: Add foreign key constraint to core.blobs
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE raw.source_material_registry 
                ADD CONSTRAINT fk_source_material_blob 
                FOREIGN KEY (optional_blob_id) 
                REFERENCES core.blobs(id) 
                ON DELETE SET NULL;
                "#,
            )
            .await?;

        // Step 9: Update foreign key from core.events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Add new constraint pointing to source_material_id
                -- (old constraint was already dropped in step 3)
                ALTER TABLE core.events 
                ADD CONSTRAINT fk_events_source_material 
                FOREIGN KEY (source_material_id) 
                REFERENCES raw.source_material_registry(source_material_id);
                "#,
            )
            .await?;

        // Step 10: Create indexes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Index on optional_blob_id for joins
                CREATE INDEX IF NOT EXISTS idx_source_material_blob_id 
                ON raw.source_material_registry(optional_blob_id) 
                WHERE optional_blob_id IS NOT NULL;
                
                -- Index on material_type for filtering
                CREATE INDEX IF NOT EXISTS idx_source_material_type 
                ON raw.source_material_registry(material_type);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // This is a destructive migration that cannot be easily reversed
        // The down migration would need to:
        // 1. Ensure all source materials have blobs (which might not be true)
        // 2. Restore the redundant columns
        // 3. Restore blob_id as primary key

        // For safety, we'll just error out
        Err(DbErr::Custom(
            "This migration cannot be reversed automatically. Manual intervention required."
                .to_string(),
        ))
    }
}
