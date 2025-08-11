use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Ensure pg_jsonschema extension is available
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE EXTENSION IF NOT EXISTS pg_jsonschema;
                "#,
            )
            .await?;

        // Create the validation function
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
                    -- Fetch the schema content for the given ID
                    SELECT json_schema INTO v_schema
                    FROM sinex_schemas.event_payload_schemas
                    WHERE id = p_schema_id;
                    
                    -- If schema not found, return false
                    IF v_schema IS NULL THEN
                        RETURN FALSE;
                    END IF;
                    
                    -- Use pg_jsonschema to validate the payload against the schema
                    -- json_matches_schema expects json type, not jsonb, so we cast
                    RETURN json_matches_schema(v_schema::json, p_payload::json);
                EXCEPTION
                    -- If validation throws an error (malformed schema, etc), return false
                    WHEN OTHERS THEN
                        RETURN FALSE;
                END;
                $$ LANGUAGE plpgsql STABLE;
                "#,
            )
            .await?;

        // Add comment explaining the function
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                COMMENT ON FUNCTION sinex_schemas.is_payload_valid(JSONB, ULID) IS 
                'Validates an event payload against its registered JSON schema using pg_jsonschema extension. Returns false if schema not found or validation fails.';
                "#
            )
            .await?;

        // Create an index on schema lookups if not already present
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .table((
                        Alias::new("sinex_schemas"),
                        Alias::new("event_payload_schemas"),
                    ))
                    .name("idx_schemas_id")
                    .col(Alias::new("id"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the validation function
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP FUNCTION IF EXISTS sinex_schemas.is_payload_valid(JSONB, ULID);
                "#,
            )
            .await?;

        Ok(())
    }
}
