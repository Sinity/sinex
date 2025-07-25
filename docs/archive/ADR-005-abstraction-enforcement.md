# ADR-005: Mandatory Abstraction Usage

Date: 2025-01-20

## Status

Accepted

## Context

The Sinex codebase has developed sophisticated abstractions to handle common patterns:

1. **QueryBuilder** - Handles ULID↔UUID conversion, type safety, and consistent error handling for database operations
2. **CoreError** - Provides structured, typed error handling with rich context
3. **Constants** - Centralizes string literals for event types, sources, and table names
4. **ValidationChain** - Offers composable, reusable validation logic

However, analysis shows these abstractions are frequently bypassed:
- 37 files use raw `sqlx::query!` instead of QueryBuilder
- 33+ instances of `anyhow!` in production code
- Hardcoded strings like `"process.heartbeat"` scattered throughout
- Manual validation logic instead of ValidationChain

This inconsistency leads to:
- ULID/UUID conversion bugs
- Difficult refactoring when schemas change
- Inconsistent error messages
- Duplicated validation logic
- Harder code review and maintenance

## Decision

We will enforce mandatory use of Sinex abstractions through automated tooling:

1. **Technical Controls**
   - Clippy configuration to disallow raw SQL and anyhow
   - Pre-commit hooks to catch violations locally
   - CI/CD checks that block merging with violations
   
2. **Developer Experience**
   - Comprehensive examples in `examples/` directory
   - IDE snippets for common patterns
   - Clear error messages pointing to alternatives

3. **Cultural Reinforcement**
   - Code review checklist including abstraction compliance
   - Architecture decision records documenting patterns
   - Performance benchmarks proving zero overhead

## Consequences

### Positive

1. **Consistency** - All code follows the same patterns
2. **Safety** - Automatic ULID conversion prevents runtime errors
3. **Maintainability** - Schema changes only require updating query builders
4. **Discoverability** - Constants make available options clear
5. **Quality** - Structured errors provide better debugging information

### Negative

1. **Learning Curve** - New developers must learn abstractions
2. **Initial Friction** - Existing code requires migration
3. **Flexibility** - Some edge cases may require extending abstractions

### Mitigation

1. **Gradual Migration** - Use automated tools for simple cases, manual review for complex
2. **Escape Hatches** - Document process for approved exceptions
3. **Continuous Improvement** - Abstractions evolve based on real usage

## Implementation

### Phase 1: Detection (Week 1)
- Deploy clippy.toml configuration
- Add pre-commit hooks
- Enable CI checks in warning mode

### Phase 2: Migration (Weeks 2-3)
- Run automated migration scripts
- Manual review of complex cases
- Update all tests to use abstractions

### Phase 3: Enforcement (Week 4)
- Switch CI to blocking mode
- Deploy IDE tools
- Team training session

### Phase 4: Maintenance (Ongoing)
- Weekly metrics review
- Abstraction improvements
- Incorporate developer feedback

## Alternatives Considered

1. **Documentation Only** - Rejected: Past experience shows guidelines alone are insufficient
2. **Partial Enforcement** - Rejected: Inconsistency undermines the benefits
3. **Manual Code Review** - Rejected: Human review misses violations and doesn't scale

## References

- [ABSTRACTION_ENFORCEMENT.md](../../docs/ABSTRACTION_ENFORCEMENT.md) - Detailed implementation guide
- [Query Builder Design](../STAD.md#query-builder) - QueryBuilder architecture
- [Error Handling RFC](../RFC-002-error-handling.md) - CoreError design rationale

## Examples

### Before (Anti-pattern)
```rust
// Raw SQL with manual UUID conversion
let event = sqlx::query_as!(
    Event,
    "SELECT * FROM core.events WHERE id = $1",
    event_id.to_uuid()
)
.fetch_one(pool)
.await
.map_err(|e| anyhow!("Failed to fetch event: {}", e))?;

// Hardcoded strings
if event.event_type == "process.heartbeat" {
    // ...
}
```

### After (Correct pattern)
```rust
// Using QueryBuilder
let event = EventQueries::get_by_id(event_id)
    .fetch_one(pool)
    .await
    .context(CoreError::NotFound { 
        entity: "event".to_string() 
    })?;

// Using constants
if event.event_type == event_types::sinex::PROCESS_HEARTBEAT {
    // ...
}
```