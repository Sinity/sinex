# Sinex Implementation Plan (Streamlined; see the full one at ./comprehensive_implementation_plan.md in case of any doubts)

## Core Architecture

### Unified Event Store

- Single `raw.events` table for both raw observations and synthesis conclusions
- `source_event_ids`: NULL = raw event, NOT NULL = synthesis event  
- `audit.archived_events` table with BEFORE DELETE trigger for safe archival
- No data is ever truly lost - only moved between tables

### Event Provenance Principles

1. **Non-deterministic sources as events**: LLM calls, RNG values stored as their own events
2. **Complete dependency chains**: Every synthesis event lists all source events in `source_event_ids`
3. **Immutable history**: Events are never updated, only archived and replaced
4. **Cascading replays are mandatory**: Can't archive an event without archiving its dependents

### Processor Registry

- Generalize `automaton_manifests` → `processor_manifests` table
- Every event has `processor_manifest_id` (4-byte integer FK)
- Massive storage savings vs inline metadata
- Tracks exact version/commit of code that created each event

## Critical Implementation Tasks

### Phase 1: Foundation Fixes (Week 1)

1. **Fix checkpoint persistence bug** in `HotlogAutomatonRunner`
   - Add `checkpoint_manager.save_checkpoint()` calls
   - Create crash/restart integration test

2. **Complete health monitoring**
   - Implement health-aggregator automaton
   - Generate system.health.summary synthesis events

3. **Verify GitOps schema system**
   - Confirm CI auto-generates schemas
   - Test deployment to `sinex_schemas.event_payload_schemas`

### Phase 2: Core Architecture (Week 2-3)

1. **Implement unified events table**

   ```sql
   ALTER TABLE raw.events ADD COLUMN source_event_ids ULID[];
   
   CREATE TABLE audit.archived_events (
       archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
       archived_by TEXT,
       archive_reason TEXT,
       superseded_by_event_id ULID,
       -- all columns from events table
   );
   
   -- Trigger to move deleted events to archive
   CREATE TRIGGER archive_events_trigger
   BEFORE DELETE ON raw.events
   FOR EACH ROW EXECUTE FUNCTION archive_event();
   ```

2. **Add provenance tracking**
   - Update automata to populate `source_event_ids`
   - Create SDK helpers for provenance management

3. **Complete Deep Symmetry migration**
   - Migrate all satellites to StatefulStreamProcessor
   - Standardize CLI: `service | scan | explore`

### Phase 3: Advanced Features (Week 4-5)

1. **Implement replay system**
   - `exo replay --automaton <name> --since <ts> --until <ts>`
   - Trace full dependency graph
   - Present impact visualization
   - Archive entire subtree atomically

2. **Build exploration commands**
   - Per-satellite `explore` subcommand
   - Coverage analysis
   - Source state inspection

3. **Implement restore/rollback**
   - `exo restore --event-id <ARCHIVED_EVENT_ID>`
   - Symmetric opposite of replay
   - Swap archived/live subtrees transactionally

## Production Hardening

### Concurrency Control

- Use `SELECT ... FOR UPDATE` for micro-replays
- Prevents race conditions in event evolution
- Database handles serialization

### Performance Optimization

- GIN index on `source_event_ids` array
- Optional provenance_cache table for hot paths
- Partition `audit.archived_events` by month/year

### Schema Evolution Strategy

1. Version schemas in registry with `is_active` flag
2. Automata read old formats, write only new format
3. Lazy migration via replay when needed
4. No special "migration automata" - just replay

### Blob Storage Integration

```sql
CREATE TABLE raw.blob_registry (
    blob_id ULID PRIMARY KEY,
    checksum TEXT NOT NULL UNIQUE,
    source_identifier TEXT NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    event_count INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- Link events to blobs via ts_orig overlap
- Idempotency via checksum detection

## Key Invariants

1. **Events are immutable** - no UPDATE operations
2. **All IDs are ULIDs** - time-sortable, distributed-safe
3. **Provenance integrity** - active events never reference archived ones
4. **ts_orig for business logic** - ULID timestamp for system debugging only
5. **Eventually consistent** - accept brief inconsistency during cascades

## Success Metrics

- [ ] Zero data loss through crashes/restarts
- [ ] Full provenance chains queryable
- [ ] Replay with complex dependencies works
- [ ] Schema evolution without breaking replays
- [ ] P99 dependency traversal < 100ms
- [ ] Archive growth sustainable via partitioning

## Architecture Decisions

1. **Unified table over split**: Query simplicity trumps theoretical purity
2. **Trigger-based archival**: Safe deletes without application complexity
3. **CLI as replay coordinator**: User control over large-scale operations
4. **Normalized processor metadata**: 1000x storage savings
5. **Lazy schema migration**: Evolution without big-bang updates

This architecture creates a true "sentient archive" - a system that remembers everything and understands how its knowledge evolved over time.

