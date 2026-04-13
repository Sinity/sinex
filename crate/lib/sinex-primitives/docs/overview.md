# sinex-primitives Overview

`sinex-primitives` is the shared domain foundation for the workspace. It exposes:

- strong types that encode Sinex invariants;
- the canonical event model and transport-facing payload helpers;
- validation, namespace, and utility helpers shared by binaries and nodes.

It does not own database repositories or runtime services.

## Responsibilities

- **Types & IDs** – canonical `Uuid`-backed identifiers, event payload enums, error types, and helper traits.
- **Validation & Utilities** – filesystem sanitisation, JSON schema helpers, `Result` aliases, and telemetry glue used by higher layers.
- **Deployment & Transport Contracts** – `SinexEnvironment`, NATS subject naming helpers, deployment-readiness descriptor types, and RPC surface types.
- **Environment Namespacing** – the `SinexEnvironment` helper used to scope schemas, stream names, sockets, and file paths per deployment.

## When to Depend on sinex-primitives

Reach for this crate whenever you need to:

- emit or interpret canonical Sinex events;
- interact with shared configuration, namespaces, or filesystem validation helpers;
- implement new binaries/automata that need the same type system as the rest of the workspace.

Reach for `sinex-db` when you need persistence, repositories, or transaction helpers.

## Related Documents

- `crate/lib/sinex-db/docs/db_repositories.md` – repository pattern and usage examples.
- `README.md#architecture` – the system-level flow that these abstractions support.
- `crate/lib/sinex-primitives/docs/type_system_patterns.md` – richer doctrine for domain typing, validation, and state machines.
- `crate/lib/sinex-primitives/docs/newtypes.md` – typed units, config wrappers, and validation notes.
- `crate/lib/sinex-primitives/docs/nats_subjects.md` – subject naming and transport contracts.
- `README.md#development` – workspace development loop and validation entrypoints.
