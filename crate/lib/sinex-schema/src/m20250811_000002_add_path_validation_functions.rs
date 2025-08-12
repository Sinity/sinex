use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create path validation functions for database-level security
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Function to validate file paths and prevent path traversal attacks
                CREATE OR REPLACE FUNCTION validate_file_path(path_input TEXT)
                RETURNS BOOLEAN AS $$
                DECLARE
                    normalized_path TEXT;
                    path_components TEXT[];
                    component TEXT;
                    depth INTEGER := 0;
                BEGIN
                    -- Check for null bytes (using chr(0) to avoid encoding issues)
                    IF position(chr(0) in path_input) > 0 THEN
                        RETURN FALSE;
                    END IF;
                    
                    -- Check path length (max 4096 characters)
                    IF length(path_input) > 4096 THEN
                        RETURN FALSE;
                    END IF;
                    
                    -- Check for dangerous URL-encoded sequences
                    IF path_input ILIKE '%2e%2e%' OR 
                       path_input ILIKE '%252e%252e%' OR
                       path_input ILIKE '%..' OR
                       path_input ILIKE '%../%' OR
                       path_input ILIKE '%..\\%' THEN
                        RETURN FALSE;
                    END IF;
                    
                    -- Normalize the path by removing redundant separators
                    normalized_path := regexp_replace(path_input, '/+', '/', 'g');
                    normalized_path := regexp_replace(normalized_path, '\\+', '/', 'g');
                    
                    -- Split path into components and check for traversal
                    path_components := string_to_array(normalized_path, '/');
                    
                    FOREACH component IN ARRAY path_components LOOP
                        IF component = '..' THEN
                            depth := depth - 1;
                            -- Prevent traversal above root
                            IF depth < 0 THEN
                                RETURN FALSE;
                            END IF;
                        ELSIF component != '' AND component != '.' THEN
                            depth := depth + 1;
                        END IF;
                    END LOOP;
                    
                    RETURN TRUE;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
                "#,
            )
            .await?;

        // Create function to sanitize file paths
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Function to sanitize file paths by removing dangerous sequences
                CREATE OR REPLACE FUNCTION sanitize_file_path(path_input TEXT)
                RETURNS TEXT AS $$
                DECLARE
                    sanitized TEXT;
                BEGIN
                    -- Remove null bytes
                    sanitized := replace(path_input, chr(0), '');
                    
                    -- Remove path traversal sequences
                    sanitized := regexp_replace(sanitized, '\.\./|\.\.\\', '', 'g');
                    sanitized := regexp_replace(sanitized, '\.\.', '', 'g');
                    
                    -- Remove URL-encoded traversal sequences
                    sanitized := regexp_replace(sanitized, '%2e%2e/?', '', 'gi');
                    sanitized := regexp_replace(sanitized, '%252e%252e/?', '', 'gi');
                    
                    -- Normalize path separators
                    sanitized := regexp_replace(sanitized, '\\+', '/', 'g');
                    sanitized := regexp_replace(sanitized, '/+', '/', 'g');
                    
                    -- Trim length to safe maximum
                    IF length(sanitized) > 4096 THEN
                        sanitized := left(sanitized, 4096);
                    END IF;
                    
                    RETURN sanitized;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
                "#,
            )
            .await?;

        // Create function to validate JSON payload paths
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Function to validate all path-like fields in JSON payloads
                CREATE OR REPLACE FUNCTION validate_payload_paths(payload JSONB)
                RETURNS BOOLEAN AS $$
                DECLARE
                    path_fields TEXT[] := ARRAY['path', 'file', 'directory', 'filename', 'filepath', 'dir', 'target', 'source_path', 'dest_path'];
                    field TEXT;
                    path_value TEXT;
                BEGIN
                    -- Check each potential path field
                    FOREACH field IN ARRAY path_fields LOOP
                        IF payload ? field THEN
                            -- Extract string value if it exists
                            IF jsonb_typeof(payload -> field) = 'string' THEN
                                path_value := payload ->> field;
                                
                                -- Validate the path
                                IF NOT validate_file_path(path_value) THEN
                                    RETURN FALSE;
                                END IF;
                            END IF;
                        END IF;
                    END LOOP;
                    
                    RETURN TRUE;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
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
                DROP FUNCTION IF EXISTS validate_payload_paths(JSONB);
                DROP FUNCTION IF EXISTS sanitize_file_path(TEXT);
                DROP FUNCTION IF EXISTS validate_file_path(TEXT);
                "#,
            )
            .await?;

        Ok(())
    }
}
