# Architecture Documentation

Current system architecture references.

## Core Architecture

- [Core_Architecture.md](./Core_Architecture.md) — High-level system flow, component overview, and system diagram

## Domain Architecture

- [UserInteraction_And_Query_Architecture.md](./UserInteraction_And_Query_Architecture.md) — Query layer design
- [SystemOperations_And_Integrity_Architecture.md](./SystemOperations_And_Integrity_Architecture.md) — Operational patterns

## Security

- [security-architecture.md](./security-architecture.md) — Threat model and security controls

## Patterns

- [type-system-patterns.md](./type-system-patterns.md) — Newtypes, validated types, state machines, compile-time safety
- [distributed-patterns.md](./distributed-patterns.md) — Event sourcing, CQRS, concurrency, idempotency, backpressure
- [observability.md](./observability.md) — Journald monitoring, checkpoint system
- [patterns/](./patterns/) — Additional pattern documentation

**Crate-specific patterns and diagrams:**
- Testing: `xtask/docs/sandbox/` (patterns.md, diagrams.md)
- Database: `crate/lib/sinex-db/docs/` (patterns.md, diagrams.md)
- Pipeline: `crate/core/sinex-ingestd/docs/` (patterns.md, diagrams.md)
- Primitives: `crate/lib/sinex-primitives/docs/` (types, validation, error handling)

## See Also

- Crate-level docs: `crate/**/docs/`
- Exploration: `docs/exploration/architecture-validation.md`
