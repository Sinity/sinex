//! Self-observation continuous aggregates for metrics
//!
//! Creates `TimescaleDB` continuous aggregates for Sinex self-observation events,
//! enabling efficient queries on internal metrics without requiring external
//! observability infrastructure (Prometheus, OpenTelemetry).
//!
//! **Issues addressed**:
//! - Issue 3: Stream Capacity Monitoring → `stream_stats_1h`
//! - Issue 16: Assembly Metrics → `assembly_stats_1h`
//! - Issue 24/29: Event Processing Metrics → `node_stats_1h`
//! - Issue 133: Load Shedding Metrics → `gateway_stats_1h`
//! - Issue 145: Replay Control Metrics → `gateway_stats_1h`
//! - Issue 147: Prometheus Endpoint → Can query these aggregates
//!
//! # Design Philosophy
//!
//! Rather than external telemetry, Sinex observes itself:
//! - Metrics become events with `source LIKE 'sinex.%'`
//! - Continuous aggregates pre-compute common queries
//! - Same query interface for all data (user events + system metrics)

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Create schema for self-observation aggregates
        conn.execute_unprepared("CREATE SCHEMA IF NOT EXISTS sinex_telemetry")
            .await?;

        // Add comment explaining the self-observation architecture
        conn.execute_unprepared(
            "COMMENT ON SCHEMA sinex_telemetry IS
            'Self-observation metrics aggregates. Sinex observes itself using its own event system.
             Events with source LIKE ''sinex.%'' are internal telemetry, aggregated here for efficiency.'",
        )
        .await?;

        // Index for efficient self-observation queries
        // This index helps filter telemetry events from user events
        conn.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS ix_events_sinex_telemetry
             ON core.events (source, event_type, ts_ingest DESC)
             WHERE source LIKE 'sinex.%'",
        )
        .await?;

        // Check if TimescaleDB is installed and the hypertable supports continuous aggregates
        // The events table uses ts_ingest as the time dimension for partitioning
        // Continuous aggregates require this to work properly
        //
        // Check: Does the hypertable have ts_ingest as the time column?
        // If not (e.g., it uses id-based ULID partitioning), continuous aggregates won't work
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
            Err(_) => false, // TimescaleDB extension not installed or error
        };

        if !can_use_caggs {
            tracing::info!(
                "Continuous aggregates not supported for core.events - skipping (self-observation will use direct queries)"
            );
            return Ok(());
        }

        // Create continuous aggregate: Request statistics per hour
        // Addresses Issues 133, 145 (gateway metrics)
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.gateway_stats_1h
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 source,
                 COUNT(*) FILTER (WHERE event_type = 'request.stats') AS stat_events,
                 AVG((payload->>'total_requests')::bigint) AS avg_total_requests,
                 SUM((payload->>'rate_limited_requests')::bigint) AS total_rate_limited,
                 AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
                 MAX((payload->>'p99_latency_ms')::float) AS max_p99_latency_ms
             FROM core.events
             WHERE source LIKE 'sinex.gateway%'
               AND event_type IN ('request.stats', 'rate_limit.exceeded', 'replay.stats')
             GROUP BY bucket, source
             WITH NO DATA",
        )
        .await?;

        // Refresh policy for gateway stats (every 10 minutes)
        conn.execute_unprepared(
            "SELECT add_continuous_aggregate_policy('sinex_telemetry.gateway_stats_1h',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
        )
        .await?;

        // Continuous aggregate: Stream statistics per hour
        // Addresses Issue 3 (stream capacity)
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.stream_stats_1h
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 payload->>'stream' AS stream_name,
                 AVG((payload->>'fill_pct')::float) AS avg_fill_pct,
                 MAX((payload->>'fill_pct')::float) AS max_fill_pct,
                 AVG((payload->>'messages')::bigint) AS avg_messages,
                 MAX((payload->>'messages')::bigint) AS max_messages,
                 COUNT(*) AS sample_count
             FROM core.events
             WHERE source = 'sinex.ingestd'
               AND event_type = 'stream.stats'
             GROUP BY bucket, payload->>'stream'
             WITH NO DATA",
        )
        .await?;

        // Refresh policy for stream stats
        conn.execute_unprepared(
            "SELECT add_continuous_aggregate_policy('sinex_telemetry.stream_stats_1h',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
        )
        .await?;

        // Continuous aggregate: Assembly statistics per hour
        // Addresses Issue 16 (assembly metrics)
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.assembly_stats_1h
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 MAX((payload->>'active_assemblies')::int) AS max_active_assemblies,
                 SUM((payload->>'total_completed')::bigint) AS total_completed,
                 SUM((payload->>'total_failed')::bigint) AS total_failed,
                 SUM((payload->>'total_timed_out')::bigint) AS total_timed_out,
                 AVG((payload->>'avg_duration_ms')::float) AS avg_duration_ms,
                 COUNT(*) AS sample_count
             FROM core.events
             WHERE source = 'sinex.ingestd'
               AND event_type = 'assembly.stats'
             GROUP BY bucket
             WITH NO DATA",
        )
        .await?;

        // Refresh policy for assembly stats
        conn.execute_unprepared(
            "SELECT add_continuous_aggregate_policy('sinex_telemetry.assembly_stats_1h',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
        )
        .await?;

        // Continuous aggregate: Node processing statistics per hour
        // Addresses Issues 24, 29 (event processing metrics)
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.node_stats_1h
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 payload->>'node_type' AS node_type,
                 SUM((payload->>'events_processed')::bigint) AS total_events_processed,
                 SUM((payload->>'events_dropped')::bigint) AS total_events_dropped,
                 AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
                 MAX((payload->>'queue_depth')::int) AS max_queue_depth,
                 SUM((payload->>'error_count')::bigint) AS total_errors,
                 COUNT(*) AS sample_count
             FROM core.events
             WHERE source = 'sinex.node'
               AND event_type = 'processing.stats'
             GROUP BY bucket, payload->>'node_type'
             WITH NO DATA",
        )
        .await?;

        // Refresh policy for node stats
        conn.execute_unprepared(
            "SELECT add_continuous_aggregate_policy('sinex_telemetry.node_stats_1h',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
        )
        .await?;

        // Continuous aggregate: Generic metric counters per hour
        // For custom metrics emitted via emit_counter/emit_gauge
        conn.execute_unprepared(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.metric_counters_1h
             WITH (timescaledb.continuous) AS
             SELECT
                 time_bucket('1 hour', ts_ingest) AS bucket,
                 payload->>'component' AS component,
                 payload->>'name' AS metric_name,
                 SUM((payload->>'value')::bigint) AS total_value,
                 MAX((payload->>'value')::bigint) AS max_value,
                 COUNT(*) AS sample_count
             FROM core.events
             WHERE source = 'sinex'
               AND event_type = 'metric.counter'
             GROUP BY bucket, payload->>'component', payload->>'name'
             WITH NO DATA",
        )
        .await?;

        // Refresh policy for metric counters
        conn.execute_unprepared(
            "SELECT add_continuous_aggregate_policy('sinex_telemetry.metric_counters_1h',
                 start_offset => INTERVAL '2 hours',
                 end_offset => INTERVAL '10 minutes',
                 schedule_interval => INTERVAL '10 minutes',
                 if_not_exists => true)",
        )
        .await?;

        // Create a view for current system health (real-time)
        conn.execute_unprepared(
            "CREATE OR REPLACE VIEW sinex_telemetry.current_health AS
             SELECT
                 e.source,
                 e.event_type,
                 e.payload->>'component' AS component,
                 e.payload->>'current_status' AS status,
                 e.payload->>'reason' AS reason,
                 e.ts_ingest AS last_update
             FROM core.events e
             INNER JOIN (
                 SELECT source, MAX(ts_ingest) AS max_ts
                 FROM core.events
                 WHERE source = 'sinex'
                   AND event_type = 'health.status'
                   AND ts_ingest > NOW() - INTERVAL '1 hour'
                 GROUP BY source
             ) latest ON e.source = latest.source AND e.ts_ingest = latest.max_ts
             WHERE e.event_type = 'health.status'",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Drop views and aggregates in reverse order
        conn.execute_unprepared("DROP VIEW IF EXISTS sinex_telemetry.current_health")
            .await?;

        // Remove refresh policies first (required before dropping aggregates)
        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.metric_counters_1h', if_exists => true)",
        )
        .await.ok(); // Ignore if not exists

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.node_stats_1h', if_exists => true)",
        )
        .await.ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.assembly_stats_1h', if_exists => true)",
        )
        .await.ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.stream_stats_1h', if_exists => true)",
        )
        .await.ok();

        conn.execute_unprepared(
            "SELECT remove_continuous_aggregate_policy('sinex_telemetry.gateway_stats_1h', if_exists => true)",
        )
        .await.ok();

        // Drop continuous aggregates
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.metric_counters_1h CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.node_stats_1h CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.assembly_stats_1h CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.stream_stats_1h CASCADE",
        )
        .await?;
        conn.execute_unprepared(
            "DROP MATERIALIZED VIEW IF EXISTS sinex_telemetry.gateway_stats_1h CASCADE",
        )
        .await?;

        // Drop index and schema
        conn.execute_unprepared("DROP INDEX IF EXISTS core.ix_events_sinex_telemetry")
            .await?;
        conn.execute_unprepared("DROP SCHEMA IF EXISTS sinex_telemetry CASCADE")
            .await?;

        Ok(())
    }
}
