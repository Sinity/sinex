# Sinex Services Crate

The services layer exposes higher-level workflows on top of the raw database
primitives in `sinex-core`. Each module provides a cohesive API that teams can
call from gateways, satellites, or automation suites without re-implementing
business logic.

- `analytics` aggregates events across time ranges and sources to power the
  operational dashboards and health reports documented in
  `docs/architecture/SystemOperations_And_Integrity_Architecture.md`.
- `content` manages large binary assets, performing metadata extraction,
  storage routing, and access control checks before delegating to the blob
  backends described in `docs/architecture/Core_Architecture.md`.
- `pkm` curates knowledge graph entities and relations.
- `search` wraps the indexing and query flow, standardising request/response
  contracts for gateways.

When contributing new service modules:

1. Extend this directory with `<module>.md` explaining the external contract,
   primary flows, and dependencies.
2. Reference any cross-cutting design assets under `docs/` so rustdoc readers
   land on the broader system narrative.
