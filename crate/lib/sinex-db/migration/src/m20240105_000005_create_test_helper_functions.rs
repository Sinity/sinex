use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Start an operation and return its ID
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.start_operation(
                    p_operation_type TEXT,
                    p_operator TEXT,
                    p_parameters JSONB
                ) RETURNS ULID AS $$
                DECLARE
                    v_operation_id ULID;
                BEGIN
                    v_operation_id := gen_ulid();
                    
                    INSERT INTO core.operations_log (
                        operation_id,
                        operation_type,
                        operator,
                        target_table,
                        operation_data,
                        result_status
                    ) VALUES (
                        v_operation_id,
                        p_operation_type,
                        p_operator,
                        'operations',  -- Default target table
                        p_parameters,
                        'success'      -- Initial status
                    );
                    
                    RETURN v_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Complete an operation
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.complete_operation(
                    p_operation_id ULID,
                    p_summary JSONB
                ) RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET result_status = 'success',
                        result_message = p_summary->>'message',
                        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - operation_ts)),
                        metadata = COALESCE(metadata, '{}'::jsonb) || p_summary
                    WHERE operation_id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Fail an operation
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.fail_operation(
                    p_operation_id ULID,
                    p_error JSONB
                ) RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET result_status = 'failure',
                        result_message = p_error->>'error',
                        duration_ms = EXTRACT(MILLISECONDS FROM (NOW() - operation_ts)),
                        metadata = COALESCE(metadata, '{}'::jsonb) || p_error
                    WHERE operation_id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Find dependent events (for provenance tracking)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.find_dependent_events(
                    p_event_id UUID
                ) RETURNS TABLE(event_id UUID, dependency_depth INTEGER) AS $$
                BEGIN
                    RETURN QUERY
                    WITH RECURSIVE dependent_events AS (
                        -- Base case: the starting event
                        SELECT 
                            e.event_id::uuid AS event_id,
                            0 AS dependency_depth
                        FROM core.events e
                        WHERE e.event_id::uuid = p_event_id
                        
                        UNION ALL
                        
                        -- Recursive case: find events that reference this event
                        SELECT 
                            e.event_id::uuid AS event_id,
                            de.dependency_depth + 1
                        FROM core.events e
                        INNER JOIN dependent_events de ON e.source_event_ids @> ARRAY[de.event_id::ulid]
                        WHERE de.dependency_depth < 10 -- Prevent infinite recursion
                    )
                    SELECT * FROM dependent_events
                    WHERE dependency_depth > 0;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Find root events (for provenance tracking)
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.find_root_events(
                    p_event_id UUID
                ) RETURNS TABLE(event_id UUID, dependency_depth INTEGER) AS $$
                BEGIN
                    RETURN QUERY
                    WITH RECURSIVE root_events AS (
                        -- Base case: the starting event
                        SELECT 
                            e.event_id::uuid AS event_id,
                            e.source_event_ids,
                            0 AS dependency_depth
                        FROM core.events e
                        WHERE e.event_id::uuid = p_event_id
                        
                        UNION ALL
                        
                        -- Recursive case: find source events
                        SELECT 
                            e.event_id::uuid AS event_id,
                            e.source_event_ids,
                            re.dependency_depth + 1
                        FROM root_events re
                        CROSS JOIN LATERAL unnest(re.source_event_ids) AS source_id
                        INNER JOIN core.events e ON e.event_id = source_id
                        WHERE re.dependency_depth < 10 -- Prevent infinite recursion
                    )
                    SELECT event_id, dependency_depth FROM root_events
                    WHERE source_event_ids IS NULL OR array_length(source_event_ids, 1) = 0;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Add compatibility views for tests that expect different column names
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE VIEW core.operations_log_compat AS
                SELECT 
                    operation_id,
                    operation_ts AS started_at,
                    operation_ts + (COALESCE(duration_ms, 0) || ' milliseconds')::interval AS completed_at,
                    operation_type,
                    operator AS invoked_by_user,
                    operation_data AS parameters,
                    result_status AS status,
                    result_message AS summary,
                    duration_ms,
                    metadata
                FROM core.operations_log;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop view
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP VIEW IF EXISTS core.operations_log_compat;
                "#,
            )
            .await?;

        // Drop test helper functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP FUNCTION IF EXISTS core.start_operation(TEXT, TEXT, JSONB);
                DROP FUNCTION IF EXISTS core.complete_operation(ULID, JSONB);
                DROP FUNCTION IF EXISTS core.fail_operation(ULID, JSONB);
                DROP FUNCTION IF EXISTS core.find_dependent_events(UUID);
                DROP FUNCTION IF EXISTS core.find_root_events(UUID);
                "#,
            )
            .await?;

        Ok(())
    }
}
