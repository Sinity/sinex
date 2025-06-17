# ADR-011: Clock Regression Handling

## Status
Accepted

## Context

ULID generation relies on system time to create time-ordered identifiers. When system clocks go backwards (due to NTP corrections, DST changes, or manual adjustments), this can break ULID ordering assumptions and cause events to appear out of sequence.

We investigated several approaches:
1. Complex monotonic generators with mutex locks
2. Time validation layers that refuse to operate with "bad" time
3. Lock-free atomic timestamp tracking
4. System-level enforcement via chrony configuration

## Decision

**We will handle clock regression by not caring about it.**

Instead, we will:
1. Use standard `Ulid::new()` without modification
2. Rely on the operating system to maintain reasonable time
3. Recommend (but not require) chrony for time synchronization
4. Accept that minor clock regressions may occasionally cause out-of-order ULIDs

## Rationale

1. **Complexity vs Benefit**: The elaborate solutions add significant complexity for a rare edge case
2. **Performance Impact**: Monotonic generators require synchronization (mutex or atomics) that slow down ULID generation
3. **OS Responsibility**: Timekeeping is the operating system's job, not the application's
4. **Real-world Impact**: With modern NTP clients (chrony), significant clock regression is extremely rare
5. **Failure Mode**: If time goes backwards, having slightly out-of-order events is preferable to refusing to operate

## Consequences

### Positive
- Simple, fast ULID generation with no synchronization overhead
- No complex time validation logic to maintain
- System continues operating even during time anomalies
- Clear separation of concerns (OS handles time, app handles events)

### Negative  
- Events may occasionally have out-of-order ULIDs during clock regression
- No application-level detection of time anomalies
- Relies on proper OS configuration for time accuracy

### Mitigations
- Document that Sinex requires a properly synchronized system clock
- Recommend chrony with `makestep 1 3` configuration
- The `ts_ingest` derived from ULID provides a consistent timestamp even if system time is wrong
- Database indexes on both `id` and `ts_ingest` allow efficient querying by either order

## Implementation

No changes required. Continue using:

```rust
impl Default for Ulid {
    fn default() -> Self {
        Self(ulid::Ulid::new())
    }
}
```

## References
- [ULID Specification](https://github.com/ulid/spec)
- [Chrony Documentation](https://chrony.tuxfamily.org/)
- Discussion in PR #XXX about monotonic ULID generation