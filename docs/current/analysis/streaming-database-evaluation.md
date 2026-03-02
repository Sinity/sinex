# Streaming Database Evaluation for Sinex

Status: exploratory
Last Updated: 2026-01-22
Author: Architecture Analysis

> **Purpose:** Evaluate streaming databases (Materialize, RisingWave) for addressing the current state tracking gap in Sinex's event-sourced architecture.

---

## Executive Summary

Sinex is an event-sourcing system that stores immutable events in PostgreSQL + TimescaleDB. A critical architectural gap exists: **events are stored but no efficient mechanism exists for tracking current state**. This document evaluates streaming databases as a potential solution.

**Recommendation:** Do not adopt a streaming database at this time. Instead:
1. Leverage TimescaleDB continuous aggregates for time-series current state
2. Use PostgreSQL materialized views for entity-level current state
3. Reserve synthesis events for business-logic-derived insights
4. Re-evaluate streaming databases when scale exceeds single-node PostgreSQL capacity

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Streaming Database Overview](#streaming-database-overview)
3. [Materialize vs RisingWave](#materialize-vs-risingwave)
4. [Fit Analysis for Sinex](#fit-analysis-for-sinex)
5. [Synthesis Events vs Materialized Views](#synthesis-events-vs-materialized-views)
6. [Implementation Considerations](#implementation-considerations)
7. [Architectural Recommendation](#architectural-recommendation)
8. [Migration Path](#migration-path)

---

## Problem Statement

### Current Architecture

```
Nodes (Ingestors)          NATS JetStream           sinex-ingestd           PostgreSQL
  fs, terminal,     -->    Event Transport    -->   Batch writes,    -->   core.events
  desktop, system                                   validation             (hypertable)
                                                         |
                                                         v
                                                    Automata Layer
                                                    (analytics, search, pkm)
```

### The Gap

Sinex captures **what happened** (events) but lacks efficient tracking of **what is** (current state):

| Capability | Current Status | Gap |
|------------|---------------|-----|
| Event history | Excellent (TimescaleDB hypertable) | None |
| Time-series aggregation | Good (continuous aggregates) | Limited to telemetry |
| Entity current state | Missing | Major |
| Real-time derived views | Partial (automata emit synthesis events) | Inconsistent |

### Concrete Examples of Missing Current State

1. **File system state:** "What files currently exist in /path/to/dir?"
   - Events: `file.created`, `file.modified`, `file.deleted`
   - Missing: Current file count, latest modification per file

2. **Terminal session state:** "What's the current working directory for session X?"
   - Events: `shell.command` with `cwd` field
   - Missing: Latest CWD per session

3. **Window focus state:** "What window is currently focused?"
   - Events: `focus.window` with timestamps
   - Missing: Current focus (latest event per workspace)

4. **Health status:** "What's the current health of each component?"
   - Events: `health.status` per component
   - Missing: Current status snapshot (existing `current_health` view addresses this partially)

---

## Streaming Database Overview

### What is a Streaming Database?

A streaming database maintains **incrementally updated query results** as underlying data changes. Unlike traditional databases where views are refreshed on-demand or periodically, streaming databases update views continuously with sub-second latency.

### Core Concepts

**Incremental View Maintenance (IVM):**
- Traditional: `REFRESH MATERIALIZED VIEW` recomputes entire result
- IVM: Only delta changes are computed and applied
- Result: Views stay up-to-date with minimal compute overhead

**Streaming SQL:**
```sql
-- Streaming database maintains this in real-time
CREATE MATERIALIZED VIEW current_file_counts AS
SELECT
    directory,
    COUNT(*) FILTER (WHERE event_type = 'file.created') -
    COUNT(*) FILTER (WHERE event_type = 'file.deleted') AS file_count,
    MAX(ts_orig) AS last_change
FROM events
WHERE event_type IN ('file.created', 'file.deleted')
GROUP BY directory;
```

### Key Players

| System | Type | Primary Use Case | License |
|--------|------|------------------|---------|
| **Materialize** | Streaming database | Strong consistency, mutable data | BSL / Self-Managed |
| **RisingWave** | Streaming database | High-throughput analytics | Apache 2.0 |
| **Apache Flink** | Stream processor | Complex event processing | Apache 2.0 |
| **pg_ivm** | PostgreSQL extension | Simple IVM for Postgres | BSD |
| **TimescaleDB** | Time-series extension | Continuous aggregates | Apache 2.0 (core) |

---

## Materialize vs RisingWave

### Architecture Comparison

| Aspect | Materialize | RisingWave |
|--------|-------------|------------|
| **Architecture** | Differential dataflow (Timely) | Cloud-native, decoupled compute/storage |
| **Storage** | Coupled compute/storage (self-managed) or cloud | S3-native (decoupled) |
| **Consistency** | Strong (ACID) | Eventual (optimized for throughput) |
| **CDC Support** | Native PostgreSQL CDC (no Kafka/Debezium needed) | CDC via connectors |
| **Protocol** | PostgreSQL wire protocol | PostgreSQL wire protocol |
| **Language** | Rust | Rust |
| **Self-hosted** | Kubernetes + PostgreSQL metadata DB | Kubernetes + S3-compatible storage |
| **License** | BSL (self-managed requires license > 24GB) | Apache 2.0 |

### Performance Characteristics

**Materialize:**
- Sub-millisecond view updates for consistent views
- Optimized for mutable operational data with CDC
- 13% speed boost, 7% less memory in recent releases
- Strong consistency guarantees (no partial states)

**RisingWave:**
- <100ms end-to-end freshness
- 10-20ms p99 query latency
- Optimized for append-only event streams
- 10x cost efficiency vs Flink (their claim)
- Bounded correctness for mutable CDC data

### Hardware Requirements

**RisingWave Minimum (Single Node):**
| Component | Minimum | Recommended |
|-----------|---------|-------------|
| Compute | 2 CPU, 8GB RAM | 4+ CPU, 16GB+ RAM |
| Compactor | 1 CPU, 1GB RAM | 2 CPU, 2GB RAM |
| Meta | 2 CPU, 1GB RAM | 2 CPU, 4GB RAM |
| Storage | SSD | High-performance SSD |

Memory-to-CPU ratio of 4:1 or higher recommended.

**Materialize Self-Managed:**
- Kubernetes 1.31+
- PostgreSQL for metadata
- Community license: 24GB memory, 48GB disk limit
- Enterprise license required above limits

### Fit Assessment

| Criterion | Materialize | RisingWave | Winner |
|-----------|-------------|------------|--------|
| Append-only event streams | Good | Excellent | RisingWave |
| Mutable entity state | Excellent | Limited | Materialize |
| Self-hosted simplicity | Moderate | Moderate | Tie |
| License flexibility | Restrictive | Permissive | RisingWave |
| PostgreSQL integration | Native CDC | Connector-based | Materialize |
| Consistency guarantees | Strong | Eventual | Materialize |
| Memory efficiency | Moderate | Good (S3 offload) | RisingWave |

---

## Fit Analysis for Sinex

### Current Architecture Context

Sinex already uses several technologies that overlap with streaming database capabilities:

1. **NATS JetStream:** Event transport with replay, backpressure
2. **PostgreSQL:** Primary event store
3. **TimescaleDB:** Time-series partitioning, continuous aggregates
4. **Automata:** Event nodes that emit synthesis events

### Where Would a Streaming Database Fit?

```
Option A: Replace PostgreSQL reads
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   NATS       │ --> │  ingestd     │ --> │  PostgreSQL  │
│  JetStream   │     │              │     │  (events)    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │ CDC
                                                  v
                                          ┌──────────────┐
                                          │  Streaming   │ <-- Queries
                                          │   Database   │
                                          └──────────────┘

Option B: Parallel ingestion
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   NATS       │ --> │  ingestd     │ --> │  PostgreSQL  │
│  JetStream   │     │              │     │  (events)    │
└──────┬───────┘     └──────────────┘     └──────────────┘
       │
       │ (parallel)
       v
┌──────────────┐
│  Streaming   │ <-- Queries
│   Database   │
└──────────────┘

Option C: Replace automata
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   NATS       │ --> │  ingestd     │ --> │  PostgreSQL  │
│  JetStream   │     │              │     │  (events)    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                                                  v
                                          ┌──────────────┐
                                          │  Streaming   │ --> Derived events
                                          │   Database   │     back to NATS
                                          └──────────────┘
```

### Integration Challenges

1. **Schema Duplication:**
   - Events stored in PostgreSQL with JSONB payloads
   - Streaming DB would need schema definitions for those payloads
   - Two systems to maintain schema consistency

2. **Provenance Complexity:**
   - Sinex tracks event provenance (material → events → synthesis)
   - Streaming DB views don't naturally fit this model
   - Would need to "eventualize" view changes for provenance

3. **Operational Overhead:**
   - New system to deploy, monitor, backup
   - Kubernetes requirement for both Materialize and RisingWave
   - Additional memory/compute resources

4. **Query Interface Split:**
   - Historical queries: PostgreSQL
   - Current state queries: Streaming DB
   - Client complexity increases

### What Current State Needs Does It Solve?

| Use Case | Streaming DB Solution | Alternative |
|----------|----------------------|-------------|
| Current file count per directory | Materialized view with GROUP BY | PostgreSQL materialized view + trigger refresh |
| Latest event per entity | Materialized view with DISTINCT ON | TimescaleDB last() aggregate |
| Running aggregates (counts, sums) | Incremental aggregation | Continuous aggregate |
| Complex joins across event types | Real-time multi-way joins | Batch job or synthesis event |
| Point-in-time state reconstruction | Time-travel queries | Event replay (existing capability) |

---

## Synthesis Events vs Materialized Views

This is the core architectural question: **when to emit a derived event vs. when to use a view?**

### Current Synthesis Event Pattern

Sinex automata emit **synthesis events** - derived events that represent processed/aggregated data:

```rust
// analytics-automaton: Emits every 100 events
if state.recent_events.len() % 100 == 0 {
    Ok(Some(json!({
        "top_events": state.event_counts,
        "window_size": state.recent_events.len(),
    })))
}

// health-aggregator: Emits periodic health reports
if state.component_health.len() % 5 == 0 {
    Ok(Some(serde_json::to_value(&state.component_health).unwrap()))
}
```

These become events with provenance:
- `analytics.insight` derived from many raw events
- `health.aggregated_report` derived from `health.status` events

### Comparison Matrix

| Dimension | Synthesis Events | Materialized Views |
|-----------|------------------|-------------------|
| **Data Model** | First-class events with provenance | Query results, no provenance |
| **Latency** | Depends on automaton emit frequency | Sub-second (streaming DB) |
| **Storage** | Events stored permanently | View state only (not historical) |
| **Query Pattern** | Query event table with filters | Direct view query |
| **Replayability** | Full audit trail, can recompute | No history, current state only |
| **Schema Evolution** | Versioned event payloads | View definition changes |
| **Downstream Processing** | Other automata can consume | Application queries only |

### Decision Framework

**Use Synthesis Events When:**
1. The derived data has **business meaning** (insight, alert, summary)
2. **Audit trail** matters (who computed what, when)
3. **Downstream processing** is expected (event chains)
4. Result should be **immutable** once computed
5. **Versioning** of the derivation logic is important

**Use Materialized Views When:**
1. You need **current state** only, not history
2. **Query performance** is the primary concern
3. The logic is **simple aggregation** (counts, sums, latest)
4. No downstream processing needed
5. **Storage efficiency** matters (don't duplicate events)

### Example Analysis

**"Current file count per directory"**

| Approach | Synthesis Event | Materialized View |
|----------|-----------------|-------------------|
| Implementation | Automaton emits `fs.directory_stats` event on file changes | `CREATE MATERIALIZED VIEW current_file_counts AS SELECT...` |
| Query | `SELECT * FROM events WHERE event_type = 'fs.directory_stats' ORDER BY ts_orig DESC LIMIT 1` | `SELECT * FROM current_file_counts WHERE directory = '/path'` |
| Latency | Depends on automaton (could be every N events or time-based) | Sub-second with streaming DB |
| History | Full history of directory stats | Current state only |
| Provenance | Linked to triggering file events | None |

**Recommendation:** Use materialized view. File counts are ephemeral operational state, not business insights. No downstream processing expected. Historical counts rarely queried.

**"Health status alert"**

| Approach | Synthesis Event | Materialized View |
|----------|-----------------|-------------------|
| Implementation | Automaton emits `health.alert` when status crosses threshold | View shows components with `status = 'Critical'` |
| Downstream | Alert triggers notification automaton | Application polls view |
| Audit | "When did component X go critical?" answerable | No historical alerts |

**Recommendation:** Use synthesis event. Alerts are business events that may trigger workflows. Audit trail matters. Historical analysis valuable.

### Can They Coexist?

**Yes, and they should.** The pattern:

1. **Raw events** in PostgreSQL (immutable, with provenance)
2. **Materialized views** for current state queries (operational efficiency)
3. **Synthesis events** for business-meaningful derivations (audit, workflows)

```
Raw Events (fs.created, fs.deleted)
         │
         ├──> Materialized View: current_file_counts (query optimization)
         │
         └──> Synthesis Event: fs.directory_anomaly (when file count spikes)
```

---

## Implementation Considerations

### Option 1: Streaming Database (RisingWave/Materialize)

**Pros:**
- Purpose-built for incremental view maintenance
- Sub-second view freshness
- Complex joins/aggregations supported
- SQL-based, familiar interface

**Cons:**
- Additional infrastructure (Kubernetes, storage)
- Schema synchronization burden
- Operational complexity (backups, monitoring, upgrades)
- License restrictions (Materialize) or eventual consistency (RisingWave)
- Query interface split between PostgreSQL and streaming DB

**Resource Requirements:**
- Minimum 8GB RAM for RisingWave compute node
- Kubernetes cluster
- S3-compatible storage or high-performance SSD

### Option 2: PostgreSQL Materialized Views + Triggers

**Pros:**
- No new infrastructure
- Same query interface
- Well-understood operational model
- Works with existing backups

**Cons:**
- Full refresh is expensive
- Triggers add write latency
- Limited to simple aggregations
- No sub-second freshness

**Implementation:**
```sql
-- Current file count per directory
CREATE MATERIALIZED VIEW current_file_counts AS
SELECT
    payload->>'directory' as directory,
    SUM(CASE WHEN event_type = 'file.created' THEN 1 ELSE 0 END) -
    SUM(CASE WHEN event_type = 'file.deleted' THEN 1 ELSE 0 END) as file_count,
    MAX(ts_orig) as last_change
FROM core.events
WHERE event_type IN ('file.created', 'file.deleted')
GROUP BY payload->>'directory';

-- Refresh periodically or on-demand
CREATE OR REPLACE FUNCTION refresh_file_counts()
RETURNS trigger AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY current_file_counts;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

### Option 3: TimescaleDB Continuous Aggregates (Existing)

**Pros:**
- Already deployed and operational
- Automatic incremental refresh
- Hypertable-aware (efficient time-series)
- No additional infrastructure

**Cons:**
- Time-bucketed only (not per-entity)
- Requires hypertable time column
- Limited to aggregation functions

**Current Usage:**
```sql
-- Existing: Gateway stats per hour
CREATE MATERIALIZED VIEW sinex_telemetry.gateway_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts_ingest) AS bucket,
    source,
    COUNT(*) FILTER (WHERE event_type = 'request.stats') AS stat_events,
    ...
FROM core.events
WHERE source LIKE 'sinex.%'
GROUP BY bucket, source;
```

**Extension Opportunity:**
```sql
-- New: Current state per entity using last()
CREATE MATERIALIZED VIEW current_window_focus
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts_ingest) AS bucket,
    payload->>'workspace' as workspace,
    last(payload->>'window_class', ts_ingest) as current_window,
    last(payload->>'window_title', ts_ingest) as current_title
FROM core.events
WHERE event_type = 'focus.window'
GROUP BY bucket, payload->>'workspace';
```

### Option 4: pg_ivm Extension

**Pros:**
- True incremental view maintenance in PostgreSQL
- Immediate updates on base table changes
- No external infrastructure

**Cons:**
- Not production-ready (explicitly stated by maintainers)
- Cannot be used with managed PostgreSQL
- Limited operator support (no OUTER JOIN)
- "Still a bit of a batch processor in disguise"

**Not Recommended** for production use.

### Option 5: Hybrid - Extend Automata Pattern

**Pros:**
- Builds on existing architecture
- Full provenance tracking
- Familiar development model
- No new infrastructure

**Cons:**
- Custom implementation per use case
- May not achieve sub-second latency
- State management complexity

**Pattern:**
```rust
// StateTrackingAutomaton: Maintains current state, emits on query
struct FileCountTracker {
    counts: HashMap<String, i64>,  // directory -> count
}

impl AutomatonNode for FileCountTracker {
    async fn process(&mut self, state: &mut State, input: JsonValue, ctx: &Context)
        -> Result<Option<JsonValue>, Error>
    {
        let dir = input.get("directory").and_then(|v| v.as_str())?;
        match ctx.event_type.as_str() {
            "file.created" => *state.counts.entry(dir.to_string()).or_insert(0) += 1,
            "file.deleted" => *state.counts.entry(dir.to_string()).or_insert(0) -= 1,
            _ => {}
        }
        // State is checkpointed via NATS KV
        // Query current state via gateway endpoint, not via event emission
        Ok(None)
    }
}
```

---

## Architectural Recommendation

### Primary Recommendation: Extend Existing Infrastructure

Do **not** adopt a streaming database at this time. Instead:

#### 1. Expand TimescaleDB Continuous Aggregates

Use for time-series current state (already deployed):

```sql
-- Example: Latest event per entity within time window
CREATE MATERIALIZED VIEW sinex_telemetry.current_window_focus
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', ts_ingest) AS bucket,
    payload->>'workspace' as workspace,
    last(payload->>'window_class', ts_ingest) as window_class,
    last(ts_orig, ts_ingest) as last_focus_time
FROM core.events
WHERE event_type = 'focus.window'
GROUP BY bucket, payload->>'workspace'
WITH NO DATA;
```

#### 2. Add PostgreSQL Materialized Views for Entity State

Use for entity-level current state (no time dimension):

```sql
-- Current state views with periodic or triggered refresh
CREATE MATERIALIZED VIEW current_file_inventory AS
SELECT
    payload->>'path' as path,
    payload->>'directory' as directory,
    last(event_type, ts_ingest) as last_operation,
    last(ts_orig, ts_ingest) as last_change
FROM core.events
WHERE event_type IN ('file.created', 'file.modified', 'file.deleted')
GROUP BY payload->>'path', payload->>'directory'
HAVING last(event_type, ts_ingest) != 'file.deleted';

-- Refresh strategy: cron job or post-batch trigger
```

#### 3. Reserve Synthesis Events for Business Logic

Keep automata for events with business meaning:

| Automaton | Output | Rationale |
|-----------|--------|-----------|
| analytics-automaton | `analytics.insight` | Business insight, triggers dashboards |
| health-aggregator | `health.alert` | Operational alert, triggers notifications |
| search-automaton | `search.indexed` | Index state change, audit trail |
| pkm-automaton | `entity.discovered` | Knowledge graph update, provenance |

#### 4. Add Gateway Endpoints for Current State

Expose materialized views via gateway:

```rust
// New handler: /state/files?directory=/path
async fn get_current_files(
    pool: DbPool,
    directory: String
) -> Result<Vec<FileState>> {
    sqlx::query_as!(
        FileState,
        "SELECT * FROM current_file_inventory WHERE directory = $1",
        directory
    )
    .fetch_all(&pool)
    .await
}
```

### When to Re-evaluate Streaming Databases

Consider adopting a streaming database when:

1. **Scale exceeds single-node PostgreSQL**
   - Event rate > 10K/second sustained
   - View refresh takes > 1 minute
   - Query latency > 100ms for current state

2. **Complex real-time joins required**
   - Multi-way joins across event types
   - Temporal joins with time windows
   - Pattern detection across streams

3. **Sub-second freshness is critical**
   - Real-time alerting with <1s latency
   - Live dashboards requiring instant updates
   - Interactive applications needing current state

4. **Operational capacity increases**
   - Kubernetes already deployed for other services
   - Team has streaming system experience
   - Budget for additional infrastructure

### If Adopting a Streaming Database

**RisingWave is recommended** over Materialize for Sinex:

| Factor | RisingWave Advantage |
|--------|---------------------|
| License | Apache 2.0 (no restrictions) |
| Event streams | Optimized for append-only (Sinex pattern) |
| Cost | S3-native storage, scale to zero |
| Simplicity | Single binary for development |

**Integration Pattern:**
```
NATS JetStream --> sinex-ingestd --> PostgreSQL (source of truth)
                        |
                        v (CDC via Debezium or native)
                   RisingWave --> Current state views
                        ^
                        |
                   Gateway queries
```

---

## Migration Path

### Phase 1: Immediate (Current Architecture)

1. **Audit existing continuous aggregates**
   - Review `sinex_telemetry.*` views
   - Ensure refresh policies are active
   - Add missing aggregates for operational queries

2. **Add PostgreSQL materialized views for current state**
   - `current_file_inventory` (file system state)
   - `current_session_state` (terminal sessions)
   - `current_component_health` (extend existing `current_health`)

3. **Implement refresh strategy**
   - Cron-based refresh for low-frequency views
   - Post-batch trigger for high-frequency needs

4. **Gateway integration**
   - Add `/state/*` endpoints
   - Document query patterns

### Phase 2: Near-term (If Needed)

1. **Evaluate pg_ivm**
   - Test stability on development database
   - Benchmark against materialized view refresh

2. **Prototype RisingWave**
   - Single-node Docker deployment
   - CDC from PostgreSQL
   - Evaluate latency and correctness

### Phase 3: Future (Scale Triggers)

1. **RisingWave production deployment**
   - Kubernetes setup
   - S3-compatible storage integration
   - Monitoring and alerting

2. **Query routing**
   - Historical queries → PostgreSQL
   - Current state queries → RisingWave

3. **Schema synchronization**
   - Shared schema definitions
   - Migration coordination

---

## Conclusion

Streaming databases offer powerful capabilities for maintaining current state views, but Sinex's current scale and architecture don't justify the additional complexity. The existing PostgreSQL + TimescaleDB stack, combined with disciplined use of synthesis events, can address the current state tracking gap.

**Key Takeaways:**

1. **Current state ≠ synthesis events** - They serve different purposes
2. **TimescaleDB continuous aggregates** are underutilized - expand their use
3. **PostgreSQL materialized views** are sufficient for entity-level state
4. **Streaming databases** are a future option when scale demands it
5. **RisingWave** is the recommended choice if/when adoption is warranted

---

## References

### Streaming Databases

- [Materialize vs RisingWave Comparison](https://materialize.com/guides/materialize-vs-risingwave/)
- [RisingWave GitHub](https://github.com/risingwavelabs/risingwave)
- [Materialize GitHub](https://github.com/MaterializeInc/materialize)
- [Stream Processing Systems 2025](https://risingwave.com/blog/stream-processing-systems-2025-risingwave-flink-spark-trends/)

### PostgreSQL Extensions

- [pg_ivm - Incremental View Maintenance](https://github.com/sraoss/pg_ivm)
- [TimescaleDB Continuous Aggregates](https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/)
- [Everything About IVM](https://materializedview.io/p/everything-to-know-incremental-view-maintenance)

### Event Sourcing Patterns

- [PostgreSQL Event Sourcing Reference](https://github.com/eugene-khyst/postgresql-event-sourcing)
- [CQRS and Event Sourcing](https://www.upsolver.com/blog/cqrs-event-sourcing-build-database-architecture)

### Sinex Internal

- [Core Architecture](../architecture/Core_Architecture.md)
- [Distributed Patterns](../architecture/distributed-patterns.md)
- [Event Taxonomy](../../lib/sinex-schema/docs/event-taxonomy.md)
- [Self-Observation Aggregates Migration](../../lib/sinex-schema/src/migrations/m20250117_000011_add_self_observation_aggregates.rs)
