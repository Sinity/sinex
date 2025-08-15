use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Test helper functions for the current operations_log schema

        // Start an operation and return its ID
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.start_operation(
                    p_operation_type TEXT,
                    p_actor TEXT,
                    p_scope TEXT DEFAULT 'test',
                    p_context JSONB DEFAULT '{}'::jsonb
                ) RETURNS UUID AS $$
                DECLARE
                    v_operation_id UUID;
                BEGIN
                    v_operation_id := gen_random_uuid();

                    INSERT INTO core.operations_log (
                        id,
                        operation_type,
                        actor,
                        scope,
                        context,
                        state,
                        started_at,
                        created_at
                    ) VALUES (
                        v_operation_id,
                        p_operation_type,
                        p_actor,
                        p_scope,
                        p_context,
                        'running',
                        NOW(),
                        NOW()
                    );

                    RETURN v_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Complete an operation successfully
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.complete_operation(
                    p_operation_id UUID,
                    p_outcome JSONB DEFAULT '{}'::jsonb,
                    p_preview_summary TEXT DEFAULT NULL
                ) RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET 
                        state = 'completed',
                        outcome = p_outcome,
                        preview_summary = p_preview_summary,
                        completed_at = NOW(),
                        finished_at = NOW()
                    WHERE id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Fail an operation with error details
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.fail_operation(
                    p_operation_id UUID,
                    p_error_details JSONB
                ) RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET 
                        state = 'failed',
                        error_details = p_error_details,
                        finished_at = NOW()
                    WHERE id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Checkpoint an operation
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.checkpoint_operation(
                    p_operation_id UUID,
                    p_checkpoint JSONB
                ) RETURNS VOID AS $$
                BEGIN
                    UPDATE core.operations_log
                    SET 
                        checkpoint = p_checkpoint
                    WHERE id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Get operation status
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.get_operation_status(
                    p_operation_id UUID
                ) RETURNS TABLE (
                    state TEXT,
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    outcome JSONB,
                    error_details JSONB
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT 
                        o.state,
                        o.started_at,
                        o.completed_at,
                        o.finished_at,
                        o.outcome,
                        o.error_details
                    FROM core.operations_log o
                    WHERE o.id = p_operation_id;
                END;
                $$ LANGUAGE plpgsql STABLE;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop test helper functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP FUNCTION IF EXISTS core.start_operation(TEXT, TEXT, TEXT, JSONB);
                DROP FUNCTION IF EXISTS core.complete_operation(UUID, JSONB, TEXT);
                DROP FUNCTION IF EXISTS core.fail_operation(UUID, JSONB);
                DROP FUNCTION IF EXISTS core.checkpoint_operation(UUID, JSONB);
                DROP FUNCTION IF EXISTS core.get_operation_status(UUID);
                "#,
            )
            .await?;

        Ok(())
    }
}
