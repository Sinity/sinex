use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create validation function (migration 13)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION sinex_schemas.is_payload_valid(
                    p_payload JSONB,
                    p_schema_id ULID
                ) RETURNS BOOLEAN AS $$
                DECLARE
                    v_schema JSONB;
                BEGIN
                    -- Get the schema content
                    SELECT schema_content INTO v_schema
                    FROM sinex_schemas.event_payload_schemas
                    WHERE id = p_schema_id;
                    
                    -- If schema not found, consider invalid
                    IF v_schema IS NULL THEN
                        RETURN FALSE;
                    END IF;
                    
                    -- Use pg_jsonschema to validate
                    RETURN json_matches_schema(v_schema, p_payload);
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Log the error and return false
                        RAISE WARNING 'Schema validation error: %', SQLERRM;
                        RETURN FALSE;
                END;
                $$ LANGUAGE plpgsql STABLE;
                "#,
            )
            .await?;

        // Add CHECK constraint (migration 14)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events
                ADD CONSTRAINT payload_must_be_valid CHECK (
                    payload_schema_id IS NULL OR
                    sinex_schemas.is_payload_valid(payload::jsonb, payload_schema_id)
                );
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events DROP CONSTRAINT IF EXISTS payload_must_be_valid;
                DROP FUNCTION IF EXISTS sinex_schemas.is_payload_valid;
                "#,
            )
            .await?;
        Ok(())
    }
}
