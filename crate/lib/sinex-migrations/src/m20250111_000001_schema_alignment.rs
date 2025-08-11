use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 1. Fix operations_log table structure
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            -- Drop old columns if they exist
            ALTER TABLE core.operations_log 
            DROP COLUMN IF EXISTS operation_id,
            DROP COLUMN IF EXISTS operation_type,
            DROP COLUMN IF EXISTS operator,
            DROP COLUMN IF EXISTS target_table,
            DROP COLUMN IF EXISTS target_schema,
            DROP COLUMN IF EXISTS target_ids,
            DROP COLUMN IF EXISTS operation_params,
            DROP COLUMN IF EXISTS status,
            DROP COLUMN IF EXISTS completed_at,
            DROP COLUMN IF EXISTS error_message,
            DROP COLUMN IF EXISTS affected_rows,
            DROP COLUMN IF EXISTS metadata,
            DROP COLUMN IF EXISTS justification,
            DROP COLUMN IF EXISTS approval_id;
            
            -- Add new columns if they don't exist
            ALTER TABLE core.operations_log 
            ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT 'system',
            ADD COLUMN IF NOT EXISTS scope JSONB NOT NULL DEFAULT '{}',
            ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'planning',
            ADD COLUMN IF NOT EXISTS preview_summary JSONB,
            ADD COLUMN IF NOT EXISTS checkpoint JSONB,
            ADD COLUMN IF NOT EXISTS approved_by TEXT,
            ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS executor_node TEXT,
            ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS outcome TEXT,
            ADD COLUMN IF NOT EXISTS error_details TEXT;
            
            -- Remove defaults after adding columns
            ALTER TABLE core.operations_log 
            ALTER COLUMN actor DROP DEFAULT,
            ALTER COLUMN scope DROP DEFAULT,
            ALTER COLUMN state DROP DEFAULT;
        "#,
            )
            .await?;

        // 2. Fix processor_manifests table - just add missing columns, keep 'name' as is
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            -- Add missing columns (but keep 'name' column as is)
            ALTER TABLE core.processor_manifests
            ADD COLUMN IF NOT EXISTS processor_version TEXT,
            ADD COLUMN IF NOT EXISTS processor_type TEXT,
            ADD COLUMN IF NOT EXISTS hostname TEXT,
            ADD COLUMN IF NOT EXISTS start_time TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS end_time TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS config JSONB,
            ADD COLUMN IF NOT EXISTS metadata JSONB;
        "#,
            )
            .await?;

        // 3. Fix source_material_registry table - keep original column names
        manager.get_connection().execute_unprepared(r#"
            -- Keep original column names, just add/modify as needed
            DO $$ 
            BEGIN
                -- Rename source to source_uri if needed
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'source') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN source TO source_uri;
                END IF;
                
                -- Rename acquisition_time to ingestion_time if needed
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'acquisition_time') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN acquisition_time TO ingestion_time;
                END IF;
                
                -- Rename blob_storage_id to optional_blob_id if needed
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'blob_storage_id') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN blob_storage_id TO optional_blob_id;
                END IF;
            END $$;
            
            -- Add material_type column if it doesn't exist
            ALTER TABLE raw.source_material_registry 
            ADD COLUMN IF NOT EXISTS material_type TEXT NOT NULL DEFAULT 'file';
            
            -- Add missing columns
            ALTER TABLE raw.source_material_registry
            ADD COLUMN IF NOT EXISTS content_preview TEXT,
            ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT false,
            ADD COLUMN IF NOT EXISTS archive_time TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS retention_policy TEXT,
            ADD COLUMN IF NOT EXISTS encoding TEXT;
            
            -- Drop unused columns
            ALTER TABLE raw.source_material_registry
            DROP COLUMN IF EXISTS path,
            DROP COLUMN IF EXISTS format,
            DROP COLUMN IF EXISTS compression,
            DROP COLUMN IF EXISTS size_bytes,
            DROP COLUMN IF EXISTS checksum_sha256,
            DROP COLUMN IF EXISTS processing_status,
            DROP COLUMN IF EXISTS processing_error,
            DROP COLUMN IF EXISTS file_metadata,
            DROP COLUMN IF EXISTS extraction_metadata,
            DROP COLUMN IF EXISTS content_type,
            DROP COLUMN IF EXISTS parent_id,
            DROP COLUMN IF EXISTS data;
            
            -- Remove default after adding
            ALTER TABLE raw.source_material_registry 
            ALTER COLUMN material_type DROP DEFAULT;
        "#).await?;

        // 4. Create core.entities table
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            CREATE TABLE IF NOT EXISTS core.entities (
                id ULID PRIMARY KEY DEFAULT gen_ulid(),
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                aliases TEXT[] NOT NULL DEFAULT '{}',
                description TEXT,
                metadata JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                merged_into_id ULID,
                UNIQUE(type, canonical_name)
            );
            
            CREATE INDEX IF NOT EXISTS idx_entities_type ON core.entities(type);
            CREATE INDEX IF NOT EXISTS idx_entities_canonical_name ON core.entities(canonical_name);
            CREATE INDEX IF NOT EXISTS idx_entities_metadata ON core.entities USING GIN (metadata);
        "#,
            )
            .await?;

        // 5. Create core.entity_relations table
        manager.get_connection().execute_unprepared(r#"
            CREATE TABLE IF NOT EXISTS core.entity_relations (
                id ULID PRIMARY KEY DEFAULT gen_ulid(),
                from_entity_id ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
                to_entity_id ULID NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
                relation_type TEXT NOT NULL,
                strength FLOAT NOT NULL DEFAULT 1.0,
                metadata JSONB NOT NULL DEFAULT '{}',
                valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                valid_until TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_from_event_id ULID
            );
            
            CREATE INDEX IF NOT EXISTS idx_entity_relations_from ON core.entity_relations(from_entity_id);
            CREATE INDEX IF NOT EXISTS idx_entity_relations_to ON core.entity_relations(to_entity_id);
            CREATE INDEX IF NOT EXISTS idx_entity_relations_from_type ON core.entity_relations(from_entity_id, relation_type);
            CREATE INDEX IF NOT EXISTS idx_entity_relations_type ON core.entity_relations(relation_type);
        "#).await?;

        // 6. Add unique_processor_consumer constraint
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            -- Drop existing unique index if it exists
            DROP INDEX IF EXISTS core.idx_processor_checkpoints_unique;
            
            -- Add named constraint for ON CONFLICT handling
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'unique_processor_consumer'
                ) THEN
                    ALTER TABLE core.processor_checkpoints 
                    ADD CONSTRAINT unique_processor_consumer 
                    UNIQUE (processor_name, consumer_group, consumer_name);
                END IF;
            END $$;
        "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Reverse the changes in opposite order

        // 1. Remove unique_processor_consumer constraint
        manager.get_connection().execute_unprepared(r#"
            ALTER TABLE core.processor_checkpoints DROP CONSTRAINT IF EXISTS unique_processor_consumer;
        "#).await?;

        // 2. Drop entity tables
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            DROP TABLE IF EXISTS core.entity_relations;
            DROP TABLE IF EXISTS core.entities;
        "#,
            )
            .await?;

        // 3. Revert source_material_registry changes
        manager.get_connection().execute_unprepared(r#"
            -- Note: This is a simplified reversion. 
            -- Full reversion would require knowing the exact original schema
            DO $$ 
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'source_material_id') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN source_material_id TO id;
                END IF;
                
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'source_uri') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN source_uri TO source;
                END IF;
                
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'ingestion_time') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN ingestion_time TO acquisition_time;
                END IF;
                
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'raw' 
                          AND table_name = 'source_material_registry' 
                          AND column_name = 'optional_blob_id') THEN
                    ALTER TABLE raw.source_material_registry RENAME COLUMN optional_blob_id TO blob_storage_id;
                END IF;
            END $$;
            
            ALTER TABLE raw.source_material_registry
            DROP COLUMN IF EXISTS material_type,
            DROP COLUMN IF EXISTS content_preview,
            DROP COLUMN IF EXISTS is_archived,
            DROP COLUMN IF EXISTS archive_time,
            DROP COLUMN IF EXISTS retention_policy;
        "#).await?;

        // 4. Revert processor_manifests changes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
            DO $$ 
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns 
                          WHERE table_schema = 'core' 
                          AND table_name = 'processor_manifests' 
                          AND column_name = 'processor_name') THEN
                    ALTER TABLE core.processor_manifests RENAME COLUMN processor_name TO name;
                END IF;
            END $$;
        "#,
            )
            .await?;

        // 5. Revert operations_log changes
        // Note: This is complex and would need the original schema

        Ok(())
    }
}
