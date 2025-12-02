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

## Security Considerations

- All SQL queries use parameterized binding where possible.
- Table names are generated using controlled timestamp-based session IDs.
- Memory limits prevent resource exhaustion attacks.
- Advisory locks prevent concurrent analysis conflicts.

## Performance Characteristics

- Time complexity: `O(V + E)` where `V` is vertices (events) and `E` is edges (dependencies).
- Space complexity: `O(V)` for the temporary analysis table.
- Batch processing prevents memory spikes for large dependency graphs.
- Early termination on depth or memory limits keeps runs predictable.
