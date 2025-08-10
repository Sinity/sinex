use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create metrics schema
        manager
            .get_connection()
            .execute_unprepared("CREATE SCHEMA IF NOT EXISTS metrics")
            .await?;

        // Event count by type and time bucket
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE MATERIALIZED VIEW metrics.event_counts_by_type_1h AS
                SELECT 
                    time_bucket('1 hour', ts_ingest) AS bucket,
                    source,
                    event_type,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT host) as unique_hosts
                FROM core.events
                GROUP BY bucket, source, event_type
                WITH NO DATA
                "#,
            )
            .await?;

        // Process heartbeat analysis
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE MATERIALIZED VIEW metrics.process_heartbeats_1h AS
                SELECT 
                    time_bucket('1 hour', ts_ingest) AS bucket,
                    source as process_name,
                    host,
                    COUNT(*) as heartbeat_count,
                    AVG((payload->>'uptime_seconds')::numeric) as avg_uptime_seconds,
                    MAX((payload->>'memory_mb')::numeric) as max_memory_mb
                FROM core.events
                WHERE event_type = 'process.heartbeat'
                GROUP BY bucket, process_name, host
                WITH NO DATA
                "#,
            )
            .await?;

        // File activity analytics
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE MATERIALIZED VIEW metrics.file_activity_1h AS
                SELECT 
                    time_bucket('1 hour', ts_ingest) AS bucket,
                    event_type,
                    COUNT(*) as operation_count,
                    COUNT(DISTINCT payload->>'path') as unique_files,
                    SUM((payload->>'size')::numeric) as total_bytes
                FROM core.events
                WHERE source = 'fs-watcher'
                    AND event_type IN ('file.created', 'file.modified', 'file.deleted')
                GROUP BY bucket, event_type
                WITH NO DATA
                "#,
            )
            .await?;

        // Terminal command analytics
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE MATERIALIZED VIEW metrics.terminal_commands_1h AS
                SELECT 
                    time_bucket('1 hour', ts_ingest) AS bucket,
                    payload->>'command' as command,
                    COUNT(*) as execution_count,
                    COUNT(DISTINCT host) as unique_hosts,
                    AVG((payload->>'duration_ms')::numeric) as avg_duration_ms
                FROM core.events
                WHERE event_type = 'command.executed'
                GROUP BY bucket, command
                WITH NO DATA
                "#,
            )
            .await?;

        // Create indexes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX idx_event_counts_bucket ON metrics.event_counts_by_type_1h (bucket);
                CREATE INDEX idx_heartbeats_bucket ON metrics.process_heartbeats_1h (bucket);
                CREATE INDEX idx_file_activity_bucket ON metrics.file_activity_1h (bucket);
                CREATE INDEX idx_terminal_commands_bucket ON metrics.terminal_commands_1h (bucket);
                "#,
            )
            .await?;

        // Create refresh function
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION metrics.refresh_all_materialized_views()
                RETURNS void AS $$
                BEGIN
                    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.event_counts_by_type_1h;
                    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.process_heartbeats_1h;
                    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.file_activity_1h;
                    REFRESH MATERIALIZED VIEW CONCURRENTLY metrics.terminal_commands_1h;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop views and schema
        manager
            .get_connection()
            .execute_unprepared("DROP SCHEMA IF EXISTS metrics CASCADE")
            .await?;

        Ok(())
    }
}
