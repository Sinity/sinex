# Cascade Analyzer

Memory-efficient algorithms for analyzing event dependencies and planning safe cascade operations
during replay.

## Algorithm Overview

The cascade analyzer uses an iterative deepening approach to build dependency graphs:

1. **Initialization** – create a temporary table with initial events at depth 0.
2. **Iterative Deepening** – for each depth level, find all events that depend on the current depth,
   up to a configurable maximum depth.
3. **Memory Management** – process events in batches to avoid memory exhaustion.
4. **Integrity Analysis** – detect violations where live events would reference archived events.
5. **Circular Dependency Detection** – use recursive CTEs to find potential cycles.

## Transaction Management

The analyzer operates within a **single transaction** to ensure a consistent snapshot of the event graph.
- **Temp Tables**: Scoped to the transaction, ensuring automatic cleanup on commit or rollback.
- **Timeouts**: A strict timeout (default 60s) prevents long-running analyses from blocking vacuum operations or causing table bloat.
- **Rollback**: Any error during analysis triggers an explicit rollback.

## Performance Analysis

### Scalability
- **Time Complexity**: `O(V + E)` where `V` is events and `E` is dependencies.
- **Recursion**: SQL recursion depth is capped (default 100) to prevent infinite loops in pathological graphs.
- **Real-World**: Designed for provenance chains with moderate fanout. Expected to handle thousands of events within the default timeout.

### Memory Usage
- **Database**: Uses `TEMP` tables, which spill to disk if they exceed `temp_buffers`, keeping database memory usage predictable.
- **Application**: The topological sort (`plan_cascade_order`) loads the relevant dependency subgraph into memory. For 10k events, this consumes ~500KB, which is well within safe limits.

## Security Considerations

- All SQL queries use parameterized binding where possible.
- Table names are generated using controlled timestamp-based session IDs.
- Memory limits prevent resource exhaustion attacks.
- Advisory locks prevent concurrent analysis conflicts.
