# Data Lifecycle: Principled Forgetting

Sinex embraces **principled forgetting** - explicit, auditable data lifecycle management. No silent deletion.

## Philosophy

The manifesto promises an "immutable event log" with "complete history" and "rebuildability via replay." But data grows, and storage is finite. The solution is not to silently drop data, but to **consciously transition data through tiers**.

Key insight: **Tombstones relate to Archive as Archive relates to Live**. Each tier transition preserves provenance integrity.

## Three-Tier Model

```
┌─────────────────────────────────────────────────────────────┐
│                    LIVE (core.events)                        │
│  Full data, full indexing, real-time queries                │
│  No automatic expiration (user controls lifecycle)          │
└─────────────────────────────────────────────────────────────┘
                            │
              [CASCADE ARCHIVE]    [CASCADE RESTORE]
                    ↓                    ↑
                    └────────────────────┘
                         (bidirectional)
                            │
┌─────────────────────────────────────────────────────────────┐
│               ARCHIVE (audit.archived_events)                │
│  Full data preserved, queryable via archive queries         │
│  Can be RESTORED back to live (bidirectional)               │
└─────────────────────────────────────────────────────────────┘
                            │
                  [CASCADE TOMBSTONE]
                  - One-way operation (data lost)
                  - Tombstones entire archived lineage
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              TOMBSTONE (core.event_tombstones)               │
│  Minimal: id, source, event_type, ts_orig, ts_purged        │
│  Preserves provenance chain structure (~100 bytes/event)    │
│  Permanent (trivial storage)                                │
│  ⚠️  NO RESTORE - data is gone, only skeleton remains       │
└─────────────────────────────────────────────────────────────┘
```

## Tier Details

### Live Tier (`core.events`)

- **Full data**: Complete event payloads, all columns indexed
- **Real-time**: Optimized for queries, aggregations, search
- **Retention policy**: No automatic TimescaleDB retention policy is configured; retention is handled via explicit lifecycle operations
- **TimescaleDB**: Hypertable for time-series performance

### Archive Tier (`audit.archived_events`)

- **Full preservation**: Same data as live, just in different table
- **Queryable**: Can search archived events
- **Reversible**: CASCADE RESTORE brings entire lineage back to live
- **Trigger-based**: DELETE on live with `sinex.operation_id` archives

### Tombstone Tier (`core.event_tombstones`)

- **Skeleton only**: `id`, `source`, `event_type`, `ts_orig`, `ts_purged`, `reason`
- **Permanent**: One-way operation, **data is gone**
- **Provenance intact**: Chain structure preserved for audit
- **Negligible storage**: ~100 bytes per tombstoned event

## The Cascade Invariant

**Tables contain COMPLETE provenance chains.** No cross-table references.

When an event moves between tiers, all events in its provenance chain move together:

```
core.events:           [Chain A]  [Chain B]  (all live)
audit.archived_events: [Chain C]  [Chain D]  (all archived)
core.event_tombstones: [Chain E]  [Chain F]  (all tombstoned)
```

This ensures:
- No live event references an archived event
- No archived event references a tombstoned event
- No orphans at any tier

### Why Cascade?

Events have **provenance**: either `source_material_id` (raw input) or `source_event_ids` (derived from other events).

Without cascade, archiving a parent would orphan children. With cascade:
1. Analyzer finds all dependent events (depth-first traversal)
2. Entire lineage moves together
3. Provenance integrity maintained

## Tier Transition Rules

| Transition | Direction | Cascade? | Data Preserved? |
|------------|-----------|----------|-----------------|
| Live → Archive | ✓ | Yes | Full data |
| Archive → Live | ✓ (restore) | Yes | Full data |
| Archive → Tombstone | ✓ | Yes | **Minimal skeleton only** |
| Tombstone → Archive | ✗ | N/A | Data lost, cannot restore |
| Tombstone → Live | ✗ | N/A | Data lost, cannot restore |

## CLI Commands

```bash
# View status of all tiers
sinexctl lifecycle status

# Archive old events (requires gateway)
sinexctl lifecycle archive --before 30d --source terminal

# Restore archived events to live
sinexctl lifecycle restore <event_id_1> <event_id_2> --confirm

# Tombstone archived events (PERMANENT!)
sinexctl lifecycle tombstone --before 365d --yes-i-understand-data-is-gone
```

### Dry Run by Default

Archive, restore, and tombstone operations default to **dry run**. Add `--confirm` to execute:

```bash
# Dry run - shows what would happen
sinexctl lifecycle restore 01HQ2KM...

# Actually execute
sinexctl lifecycle restore 01HQ2KM... --confirm
```

## Automatic Retention: Current State

Schema apply explicitly removes any TimescaleDB retention policy for `core.events`:

- `remove_retention_policy('core.events', if_exists => true)`

Retention is managed through explicit lifecycle commands (`archive` / `restore` / `tombstone`) so transitions remain auditable and cascade-preserving.

## Database Schema

### Tombstones Table

```sql
CREATE TABLE core.event_tombstones (
    id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    ts_orig TIMESTAMPTZ NOT NULL,
    ts_purged TIMESTAMPTZ NOT NULL DEFAULT now(),
    purge_reason TEXT,
    purge_operation_id UUID,
    archived_at TIMESTAMPTZ
);
```

### Key Functions

```sql
-- Get lifecycle tier status
SELECT * FROM core.lifecycle_tier_status();

-- Execute cascade tombstone
SELECT core.execute_cascade_tombstone(
    archived_ids := ARRAY['01HQ2KM...']::UUID[],
    reason := 'Data retention policy',
    operation_id := '01HQ2KN...'::UUID
);

-- Execute cascade restore
SELECT core.execute_cascade_restore(
    archived_ids := ARRAY['01HQ2KM...']::UUID[],
    operation_id := '01HQ2KN...'
);
```

## Implementation Notes

### Archive Trigger

The existing archive-on-delete trigger remains unchanged:
- DELETE from `core.events` with `sinex.operation_id` set
- Copies event to `audit.archived_events` before delete
- Cascade analyzer ensures dependent events archived too

### Replay Relationship

**Archive is a prerequisite for replay**, not separate:

1. Archive events from source material X
2. Reprocess source material X through ingestors
3. Fresh (identical) events created
4. Can restore archived events if replay fails

Replay archives first, ensuring original events preserved before re-derivation.

## Source Of Truth

Lifecycle behavior is managed in declarative schema apply SQL:
1. `TOMBSTONE_LIFECYCLE_SQL` in `crate/lib/sinex-schema/src/apply.rs` creates `core.event_tombstones` and lifecycle functions.
2. `configure_timescaledb()` in the same file configures hypertable/chunk behavior and removes automatic retention policy.

If retention policy semantics change, update this document and `apply.rs` in the same change to prevent policy drift.

## Summary

| Aspect | Current State |
|--------|---------------|
| Retention | No automatic Timescale policy on `core.events`; explicit lifecycle APIs control retention |
| Archive | Trigger-backed archive table with cascade operations via lifecycle functions |
| Tombstone | Minimal skeleton preservation in `core.event_tombstones` |
| Provenance | Cascade operations preserve chain integrity across explicit transitions |
| Audit trail | Explicit lifecycle operations are auditable via operation IDs |
