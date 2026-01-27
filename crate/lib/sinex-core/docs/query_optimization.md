# Database Query Optimization

Performance is a first-class citizen in the Sinex data layer. The system employs several strategies to ensure high-throughput event processing and low-latency analytics across large datasets.

## Read-Optimized Schema Cache

To avoid redundant database round-trips during event validation, the system utilizes a read-optimized schema cache repository.

- **Read/Write Split**: Schema mutations are restricted to the `SchemaManagementRepository`, while high-frequency lookups are served by the `SchemaCacheRepository`.
- **Bulk Loading**: On startup, services use the `fetch_latest_active_schemas` pattern to pre-load all current schemas into memory in a single database round-trip.
- **Efficient Filtering**: Lookups utilize PostgreSQL-specific `DISTINCT ON` clauses to quickly resolve the latest version of a schema for any given source/type pair.

## Indexing Strategies

### Functional Indexes
Standard indexes are insufficient for the case-insensitive and pattern-based searches required by the Knowledge Graph.
- **Normalization**: `LOWER(name)` and `LOWER(canonical_name)` functional indexes enable O(log n) lookups for case-insensitive searches.
- **GIN Indexes**: Full-text search and JSONB containment filters (`@>`) are accelerated via Generalized Inverted Indexes (GIN).

### Composite Indexes
Analytics queries frequently filter by multiple dimensions (e.g., source + event_type + time). Composite indexes on `(source, event_type, ts_orig DESC)` ensure that these multi-dimensional queries can be answered without scanning the entire table.

## TimescaleDB Hypertable Optimization

The events table is managed as a TimescaleDB hypertable, providing several performance benefits:
- **Chunk Exclusion**: Queries with time range filters automatically skip data chunks that fall outside the requested window.
- **Retention Management**: Large volumes of old events can be dropped at the chunk level without impacting write performance.
- **Time-Series Aggregation**: The `time_bucket` function is natively optimized for hypertable chunks, enabling efficient histogram and trend analysis.

## Efficient Counting & Estimation

For UI dashboards and status reports, the system prefers O(1) estimation over O(n) exact counts.
- **pg_class Estimates**: The `count_all_estimate` query reads from PostgreSQL's internal statistics, providing a near-instant row count for large tables.
- **Plan-based Estimation**: For filtered queries, the system can extract row counts from the PostgreSQL query planner (`EXPLAIN`) without executing the full query.
