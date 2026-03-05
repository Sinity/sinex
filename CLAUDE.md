# Sinex Agent Memory

> Event-driven data capture platform. Rust, NATS JetStream, PostgreSQL (TimescaleDB + pgvector).

---

## Agent Identity

@.claude/includes/identity/_index.md

---

## Commands

@.claude/includes/commands/_index.md

---

## Architecture

@.claude/includes/architecture/_index.md

---

## Patterns

@.claude/includes/patterns/_index.md

---

## Reference

@.claude/includes/reference/_index.md

---

## Maintenance Protocol

### When to Update This File

Update CLAUDE.md or its transclusions when you:

1. Add a new crate or binary
2. Add a new shared utility intended for cross-crate use
3. Change architectural patterns (data flow, dependencies)
4. Add new test macros or fixtures
5. Change database schema significantly
6. Discover a pattern/anti-pattern worth documenting
7. Add new documentation files (update the Documentation Map)
8. Notice friction that should be captured as identity trait

### How to Update

1. Edit modular files in `.claude/includes/` directly
2. Keep format consistent (tables for reference, prose for explanation)
3. Verify accuracy against current code
4. Include in the same commit as the change being documented

### Verification

Before committing CLAUDE.md changes:

- Ensure paths mentioned still exist
- Ensure type names are accurate
- Ensure commands work as documented
- Ensure documentation paths in the map are current
