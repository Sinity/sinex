# TIM-TimescaleDBConfiguration: `raw.events` Hypertable

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (TimescaleDB hypertable creation and basic configuration working, compression pending)
**Dependencies**: TimescaleDB PostgreSQL extension, NixOS PostgreSQL configuration
**Blocks**: Time-series event storage, efficient time-based queries, data compression

## MVP Specification
- TimescaleDB extension installation and activation
- raw.events hypertable creation with ts_ingest partitioning
- Basic chunk interval configuration (1 day)
- Automatic data migration support
- Core time-series query optimization

## Enhanced Features
- Adaptive chunk sizing based on data volume
- Native compression for older chunks
- Automated retention policies
- Advanced time-series analytics functions
- Parallel query optimization

## Implementation Checklist
- [x] TimescaleDB extension available in NixOS
- [x] Database extension activation
- [x] Hypertable creation for raw.events
- [x] Time-based partitioning configuration
- [x] Basic chunk interval setup
- [x] Data migration support
- [ ] Adaptive chunk sizing
- [ ] Native compression setup
- [ ] Automated retention policies
- [ ] Advanced analytics functions

*   **Relevant ADR:** (Implicitly supported by choice of TimescaleDB in Vision Doc III.3.1.2)
*   **Original UG Context:** Section 1.2

This TIM details the configuration of TimescaleDB for managing the `raw.events` table as a hypertable, optimized for time-series data.

## 1. Rationale Summary

TimescaleDB is used for `raw.events` due to its ability to efficiently partition large time-series tables, provide performant time-based queries, and offer features like native compression. This is essential for handling the high volume of events the Exocortex will ingest over time.

## 2. Installation and Setup

*   **NixOS:** The `timescaledb` extension should be packaged with PostgreSQL.
    ```nix
    # Example: in configuration.nix
    # services.postgresql = {
    #   enable = true;
    #   package = pkgs.postgresql_16; # Ensure this package has TimescaleDB
    //   extraPlugins = [ pkgs.timescaledb_toolkit ]; // If toolkit needed separately
    //   settings = {
    //     shared_preload_libraries = "timescaledb"; // Add to existing, e.g., "pg_stat_statements,timescaledb"
    //   };
    //   # It's also common for NixOS PostgreSQL modules to have a direct
    //   # services.postgresql.timescaledb.enable = true; option.
    // };
    ```
*   **Database Activation:**
    ```sql
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE; -- CASCADE installs dependencies like tsl
    ```
    This typically needs to be run by a superuser. `timescaledb_tune` utility can be run on the server to suggest optimal `postgresql.conf` settings based on system resources.

## 3. Hypertable Creation for `raw.events`

The `raw.events` table (DDL in `TIM-EventSubstrateDDL.md` - *assuming this TIM would be generated later*) is converted into a hypertable partitioned by `ts_ingest`.

*   **DDL (from UG Sec 1.2.3, Primary Document Appendix A):**
    ```sql
    -- Assuming raw.events table already exists
    SELECT create_hypertable(
      'raw.events',
      'ts_ingest',                   -- Time partitioning column
      if_not_exists => TRUE,
      chunk_time_interval => INTERVAL '1 day', -- Initial interval, adjust based on volume
      migrate_data => TRUE            -- If table already has data
    );
    ```

## 4. Optimal Chunk Intervals and Sizing Guidelines

*   **Chunk Interval [SR1]:** Initially `1 day`. This should be reviewed based on actual daily write volume. If daily volume is very high (e.g., >10-20GB uncompressed), shorter intervals (e.g., `12 hours` or `6 hours`) might be better. If volume is low, `7 days` might be acceptable.
*   **Chunk Sizing Guideline [SA1, TimescaleDB Docs]:** Aim for each chunk (data + indexes) to be approximately 10-25% of the host's available RAM for PostgreSQL. This helps keep recent, "hot" chunks in memory.
    *   Avoid thousands of tiny chunks (degrades query planner).
    *   Avoid excessively large chunks (slows down maintenance, compression, reordering).
*   **Monitoring Chunk Size:** Use TimescaleDB functions like `chunk_relation_size_pretty('chunk_name')` or query `timescaledb_information.chunks`.

## 5. Compression for `raw.events`

TimescaleDB's native columnar compression can significantly reduce storage for older data.

*   **Techniques & Effectiveness [SR1, SA1]:** Uses algorithms like Gorilla (floats), Delta-Delta (timestamps), Simple-8b/RLE (integers), Dictionary (low-cardinality strings). Can achieve 90-95% storage reduction on typical time-series data.
*   **JSONB Compression [SR1, SA1]:**
    *   PostgreSQL's TOAST already compresses JSONB (default `lz4`).
    *   TimescaleDB's specialized columnar compression is less effective on opaque JSONB blobs compared to structured native columns.
    *   **Recommendation:** Extract frequently queried, common, or high-cardinality fields from `raw.events.payload` into separate, strongly-typed columns in `raw.events` itself or in promoted domain tables. These will benefit fully from TimescaleDB compression. Use GIN indexes on the JSONB `payload` for querying truly dynamic fields.
*   **Configuring Compression Policy (from UG Sec 1.2.3):**
    ```sql
    -- Enable compression on the hypertable
    ALTER TABLE raw.events SET (
      timescaledb.compress,
      timescaledb.compress_orderby = 'ts_orig DESC, id', -- Order data within segments for better compression/querying
      timescaledb.compress_segmentby = 'source, host' -- Columns defining segments; choose high-cardinality but not too many
    );

    -- Add a policy to compress chunks older than a certain age (e.g., 7 days)
    SELECT add_compression_policy('raw.events', INTERVAL '7 days');
    ```
    *   `compress_orderby`: Sorts data within each segment. Ordering by time first, then a unique key like `id` is common.
    *   `compress_segmentby`: Groups data into segments for compression. Columns used here should ideally be those frequently used in `WHERE` clauses or `GROUP BY` clauses on compressed data, as TimescaleDB can sometimes skip decompressing segments that don't match the `segmentby` filters.
*   **Querying Compressed Data [SA1]:** Incurs decompression overhead. Indexing `segmentby` columns and columns used in `WHERE` clauses on compressed data is crucial.

