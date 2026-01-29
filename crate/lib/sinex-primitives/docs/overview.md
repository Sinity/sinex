# sinex-core Overview

`sinex-core` is the shared foundation for every Rust crate in the workspace. It exposes:

- strong types that encode Sinex invariants;
- database repositories and helpers built on `sqlx`;
- environment/namespace utilities shared by binaries and nodes.

The crate deliberately contains no runtime services—only reusable building blocks.

## Responsibilities

- **Types & IDs** – canonical [`Ulid`-backed](../../sinex-schema/docs/ulid.md) identifiers, event payload enums, error types, and helper traits.
- **Database Access** – connection pooling, unit-of-work helpers, and repositories (events, source material, checkpoints, operations log, etc.).
- **Validation & Utilities** – filesystem sanitisation, JSON schema helpers, `Result` aliases, and telemetry glue used by higher layers.
- **Environment Namespacing** – the `SinexEnvironment` helper used to scope schemas, stream names, sockets, and file paths per deployment.

## When to Depend on sinex-core

Reach for this crate whenever you need to:

- query or mutate persistent state in Postgres using the established repositories;
- emit or interpret canonical Sinex events;
- interact with shared configuration, namespaces, or filesystem validation helpers;
- implement new binaries/automata that need the same type system as the rest of the workspace.

## Related Documents

- `crate/lib/sinex-core/docs/db_repositories.md` – repository pattern and usage examples.
- `crate/lib/sinex-core/docs/types_overview.md` – catalog of major type families (events, errors, IDs, utilities).
- `docs/current/architecture/Core_Architecture.md` – the system-level flow that these abstractions support.
- `docs/documentation-guidelines.md` – expectations when updating or extending crate docs.
