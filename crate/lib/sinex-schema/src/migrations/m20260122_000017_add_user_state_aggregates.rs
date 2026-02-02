//! User-facing continuous aggregates for current state tracking
//!
//! Creates `TimescaleDB` continuous aggregates for tracking current state,
//! addressing the architectural gap where Sinex tracks "what happened"
//! but not efficiently "what is".
//!
//! **Architecture Decision**:
//! Rather than adopting a streaming database (Materialize/RisingWave),
//! we use `TimescaleDB` continuous aggregates for time-series current state
//! and `PostgreSQL` materialized views for entity-level current state.
//! See: docs/current/analysis/streaming-database-evaluation.md
//!
//! # Continuous Aggregates (Time-Series State)
//!
//! - `current_window_focus` - Workspace/window activity (5-min buckets)
//! - `command_frequency_hourly` - Shell command execution patterns
//! - `file_activity_summary` - Filesystem activity aggregation
//! - `current_system_state` - System resource usage trends
//!
//! # Materialized Views (Entity-Level State)
//!
//! - `current_entity_state` - Last known state for each entity
//! - `current_device_state` - Current state of tracked devices
//!
//! # Design Philosophy
//!
//! **Synthesis Events vs Continuous Aggregates**:
//! - Synthesis events: Business-meaningful derivations (e.g., "user became idle")
//! - Continuous aggregates: Query optimization for current state (e.g., "active window")
//! - These serve different purposes and can coexist
//!
//! **Refresh Strategy**:
//! - 5-minute buckets with 10-minute refresh interval
//! - 2-hour lag window for late-arriving events
//! - Balance freshness vs query efficiency

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Check if TimescaleDB continuous aggregates are supported
        let hypertable_check = conn
            .execute_unprepared(
                "SELECT 1 FROM timescaledb_information.dimensions d
                 JOIN timescaledb_information.hypertables h
                   ON d.hypertable_schema = h.hypertable_schema
                  AND d.hypertable_name = h.hypertable_name
                 WHERE h.hypertable_schema = 'core'
                   AND h.hypertable_name = 'events'
                   AND d.column_name = 'ts_ingest'
                   AND d.dimension_type = 'Time'",
            )
            .await;

        let can_use_caggs = match hypertable_check {
            Ok(result) => result.rows_affected() > 0,
            Err(_) => false,
        };

        if !can_use_caggs {
            tracing::info!(
                "Continuous aggregates not supported - creating only materialized views"
            );
        }

        // Only create continuous aggregates if TimescaleDB is properly configured
        if can_use_caggs {
            // ─────────────────────────────────────────────────────────────────
            // Continuous Aggregate: Current Window Focus
            // ─────────────────────────────────────────────────────────────────
            // Tracks workspace/window activity in 5-minute buckets
            // Use case: "What window is the user focused on right now?"
            //
            // Query pattern:
            //   SELECT * FROM sinex_telemetry.current_window_focus
            //   WHERE bucket >= NOW() - INTERVAL '1 hour'
            //   ORDER BY bucket DESC LIMIT 1;
            conn.execute_unprepared(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_window_focus
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('5 minutes', ts_ingest) AS bucket,
                 payload->>'workspace' AS workspace,
                 last(payload->>'window_class', ts_ingest) AS window_class,
                 last(payload->>'window_title', ts_ingest) AS window_title,
                 last(payload->>'window_id', ts_ingest) AS window_id,
                 last(ts_orig, ts_ingest) AS last_focus_time,
                 COUNT(*) AS focus_event_count
             FROM core.events
             WHERE event_type = 'focus.window'
               AND source LIKE 'desktop.%'
             GROUP BY bucket, payload->>'workspace'
             WITH NO DATA",
            )
            .await?;

            // Refresh every 5 minutes, with 10-minute lag for late events
            conn.execute_unprepared(
                "SELECT add_continuous_aggregate_policy('sinex_telemetry.current_window_focus',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '5 minutes',
                 schedule_interval => INTERVAL '5 minutes',
                 if_not_exists => true)",
            )
            .await?;

            // ─────────────────────────────────────────────────────────────────
            // Continuous Aggregate: Command Frequency (Hourly)
            // ─────────────────────────────────────────────────────────────────
            // Tracks shell command execution patterns
            // Use case: "What commands am I running most frequently?"
            //
            // Query pattern:
            //   SELECT command, total_executions
            //   FROM sinex_telemetry.command_frequency_hourly
            //   WHERE bucket >= NOW() - INTERVAL '24 hours'
            //   ORDER BY total_executions DESC LIMIT 20;
            conn.execute_unprepared(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.command_frequency_hourly
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 payload->>'command' AS command,
                 payload->>'shell' AS shell,
                 COUNT(*) AS total_executions,
                 COUNT(*) FILTER (WHERE (payload->>'exit_code')::int = 0) AS successful_executions,
                 COUNT(*) FILTER (WHERE (payload->>'exit_code')::int != 0) AS failed_executions,
                 AVG((payload->>'duration_ms')::float) AS avg_duration_ms
             FROM core.events
             WHERE event_type IN ('shell.command', 'shell.command.canonical')
               AND source LIKE 'terminal.%'
             GROUP BY bucket, payload->>'command', payload->>'shell'
             WITH NO DATA",
            )
            .await?;

            // Refresh every 10 minutes
            conn.execute_unprepared(
                "SELECT add_continuous_aggregate_policy('sinex_telemetry.command_frequency_hourly',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
            )
            .await?;

            // ─────────────────────────────────────────────────────────────────
            // Continuous Aggregate: File Activity Summary (Hourly)
            // ─────────────────────────────────────────────────────────────────
            // Tracks filesystem activity patterns
            // Use case: "What directories have the most activity?"
            //
            // Query pattern:
            //   SELECT directory, total_events
            //   FROM sinex_telemetry.file_activity_summary
            //   WHERE bucket >= NOW() - INTERVAL '24 hours'
            //   ORDER BY total_events DESC LIMIT 20;
            conn.execute_unprepared(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.file_activity_summary
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 -- Extract directory from path (everything before last /)
                 regexp_replace(payload->>'path', '/[^/]*$', '') AS directory,
                 event_type,
                 COUNT(*) AS total_events,
                 COUNT(DISTINCT payload->>'path') AS unique_files
             FROM core.events
             WHERE event_type IN ('file.created', 'file.modified', 'file.deleted')
               AND source = 'fs-watcher'
             GROUP BY bucket, regexp_replace(payload->>'path', '/[^/]*$', ''), event_type
             WITH NO DATA",
            )
            .await?;

            // Refresh every 10 minutes
            conn.execute_unprepared(
                "SELECT add_continuous_aggregate_policy('sinex_telemetry.file_activity_summary',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
            )
            .await?;

            // ─────────────────────────────────────────────────────────────────
            // Continuous Aggregate: Current System State (5-minute buckets)
            // ─────────────────────────────────────────────────────────────────
            // Tracks system resource usage trends
            // Use case: "What's the current system load?"
            //
            // Query pattern:
            //   SELECT * FROM sinex_telemetry.current_system_state
            //   WHERE bucket >= NOW() - INTERVAL '1 hour'
            //   ORDER BY bucket DESC LIMIT 1;
            conn.execute_unprepared(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_system_state
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('5 minutes', ts_ingest) AS bucket,
                 AVG((payload->>'cpu_percent')::float) AS avg_cpu_percent,
                 MAX((payload->>'cpu_percent')::float) AS max_cpu_percent,
                 AVG((payload->>'memory_percent')::float) AS avg_memory_percent,
                 MAX((payload->>'memory_percent')::float) AS max_memory_percent,
                 AVG((payload->>'disk_percent')::float) AS avg_disk_percent,
                 last((payload->>'active_units')::int, ts_ingest) AS current_active_units,
                 COUNT(*) AS sample_count
             FROM core.events
             WHERE event_type IN ('system.resources', 'systemd.units_summary')
               AND source = 'system-ingestor'
             GROUP BY bucket
             WITH NO DATA",
            )
            .await?;

            // Refresh every 5 minutes
            conn.execute_unprepared(
                "SELECT add_continuous_aggregate_policy('sinex_telemetry.current_system_state',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '5 minutes',
                 schedule_interval => INTERVAL '5 minutes',
                 if_not_exists => true)",
            )
            .await?;
        } // End of continuous aggregates (only created if TimescaleDB available)

        // ─────────────────────────────────────────────────────────────────
        // Materialized View: Current Entity State
        // ─────────────────────────────────────────────────────────────────
        // NOTE: Commented out until entities schema is implemented
        // The entities schema and tables don't exist yet in the current migration history.
        // This will be uncommented when the knowledge graph schema is added.
        //
        // conn.execute_unprepared(
        //     "CREATE MATERIALIZED VIEW IF NOT EXISTS entities.current_entity_state AS
        //      SELECT DISTINCT ON (entity_id)
        //          entity_id,
        //          entity_type,
        //          entity_name,
        //          metadata,
        //          created_at,
        //          updated_at
        //      FROM entities.entities
        //      ORDER BY entity_id, updated_at DESC",
        // )
        // .await?;
        //
        // conn.execute_unprepared(
        //     "CREATE UNIQUE INDEX IF NOT EXISTS ix_current_entity_state_entity_id
        //      ON entities.current_entity_state (entity_id)",
        // )
        // .await?;

        // ─────────────────────────────────────────────────────────────────
        // Materialized View: Current Device State
        // ─────────────────────────────────────────────────────────────────
        // Tracks current state of systemd units and devices
        // Use case: "Which systemd units are currently active?"
        //
        // Query pattern:
        //   SELECT * FROM sinex_telemetry.current_device_state
        //   WHERE state = 'active';
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_device_state AS
             SELECT DISTINCT ON (payload->>'unit_name')
                 payload->>'unit_name' AS unit_name,
                 payload->>'unit_type' AS unit_type,
                 payload->>'state' AS state,
                 payload->>'sub_state' AS sub_state,
                 ts_ingest AS last_update
             FROM core.events
             WHERE event_type IN ('systemd.unit_changed', 'udev.device_changed')
               AND source = 'system-ingestor'
               AND ts_ingest > NOW() - INTERVAL '7 days'
             ORDER BY payload->>'unit_name', ts_ingest DESC",
        )
        .await?;

        // Create index for fast lookups by unit_name and state
        conn.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS ix_current_device_state_unit_name
             ON sinex_telemetry.current_device_state (unit_name)",
        )
        .await?;

        conn.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS ix_current_device_state_state
             ON sinex_telemetry.current_device_state (state)",
        )
        .await?;

        // ─────────────────────────────────────────────────────────────────
        // Helper View: Recent Activity Summary
        // ─────────────────────────────────────────────────────────────────
        // Convenience view combining multiple current state sources
        // Only created if continuous aggregates are available
        if can_use_caggs {
            conn.execute_unprepared(
                "CREATE OR REPLACE VIEW sinex_telemetry.recent_activity_summary AS
                 SELECT
                     'window_focus' AS activity_type,
                     workspace AS context,
                     window_class AS detail,
                     last_focus_time AS timestamp
                 FROM sinex_telemetry.current_window_focus
                 WHERE bucket >= NOW() - INTERVAL '30 minutes'
                 ORDER BY bucket DESC
                 LIMIT 1

                 UNION ALL

                 SELECT
                     'system_load' AS activity_type,
                     'cpu' AS context,
                     ROUND(avg_cpu_percent::numeric, 2)::text AS detail,
                     bucket AS timestamp
                 FROM sinex_telemetry.current_system_state
                 WHERE bucket >= NOW() - INTERVAL '30 minutes'
                 ORDER BY bucket DESC
                 LIMIT 1

                 UNION ALL

                 SELECT
                     'command_execution' AS activity_type,
                     shell AS context,
                     command AS detail,
                     bucket AS timestamp
                 FROM sinex_telemetry.command_frequency_hourly
                 WHERE bucket >= NOW() - INTERVAL '1 hour'
                 ORDER BY total_executions DESC
                 LIMIT 5",
            )
            .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Drop views first
        conn.execute_unprepared("DROP VIEW IF EXISTS sinex_telemetry.recent_activity_summary")
            .await?;

        // Drop indexes on materialized views
        conn.execute_unprepared(
            "DROP INDEX IF EXISTS sinex_telemetry.ix_current_device_state_state",
        )
        .await?;
        conn.execute_unprepared(
            "DROP INDEX IF EXISTS sinex_telemetry.ix_current_device_state_unit_name",
        )
        .await?;
        // Note: entities.ix_current_entity_state_entity_id not created yet (entities schema pending)

        // Drop materialized views
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.current_device_state",
        )
        .await?;
        // Note: entities.current_entity_state not created yet (entities schema pending)

        // Remove continuous aggregate policies
        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.current_system_state', if_exists => true)",
        )
        .await
        .ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.file_activity_summary', if_exists => true)",
        )
        .await
        .ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.command_frequency_hourly', if_exists => true)",
        )
        .await
        .ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.current_window_focus', if_exists => true)",
        )
        .await
        .ok();

        // Drop continuous aggregates
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.current_system_state CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.file_activity_summary CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.command_frequency_hourly CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.current_window_focus CASCADE",
        )
        .await?;

        Ok(())
    }
}
