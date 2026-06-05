# Authority Surfaces

Status: cross-cutting review rule for #1206. This is not an implementation
roadmap. Keep it only as the map of which current surface owns a mutation or
truth decision; move implementation detail to the owning crate docs or issues.

Each concern should have one authority surface: the place that decides truth or
performs mutation for that concern. Other surfaces may wrap, project, inspect, or
orchestrate that authority, but they should not become independent writers with
their own semantics.

Layering is allowed. Duplication is not. An API over database persistence is
layering when it adds auth, rate limits, and audit while persistence remains the
write authority. A second command that writes the same conceptual state through
ad hoc SQL is authority duplication.

## Current Map

| Concern | Authority | Wrappers / projections | Demote or remove |
|---|---|---|---|
| Event persistence | `sinexd::event_engine` writing `core.events` through `sinex-db` repositories | `sinexd::api` query/RPC, `sinexctl query`, telemetry views | Direct ad hoc event inserts outside tests and explicit repair tooling. |
| Source material registration | source contract material registration paths backed by `raw.source_material_registry` | source adapters, parser jobs, workbench inspection | Parser-local material rows that bypass registration policy. |
| Schema convergence | `sinex-schema apply` desired-state engine | `xtask ci schema-only`, `xtask schema strict-diff` | Hand-written DDL in runtime code; test-only DDL outside isolated fixtures. |
| Event schema inventory | derived `EventPayload` registry and checked-in schema bundle | schema bundle, payload tests, runtime schema registration | Dormant active schemas without producers, unless marked advisory/future. |
| Runtime deployment | NixOS module under `services.sinex` | `/etc/sinex/deployment-readiness.json`, systemd units, VM tests | Manual systemd edits as durable config; unchecked env-only deployment contracts. |
| Runtime operation | `sinexd::api`/`sinexctl` authenticated runtime commands | `sinexctl status`, `sinexctl replay`, `sinexctl lifecycle` | `xtask` as production control plane. |
| Developer verification | `xtask` local/CI workflows | GitHub Actions and concrete test/check commands | Raw cargo invocations, generated declaration catalogs, and one-off shell gates that bypass history. |
| Source-adapter declaration | in-repo source contract registrations and NixOS source binding manifest | `sinexd` source binding loader, NixOS module checks, parser tests | Parallel generated source catalogs maintained as authority. |
| Privacy/admission policy | DB/user policy applied by the event-engine admission chokepoint | audit/export/delete CLI surfaces, source-record field metadata | Parser/source/automaton code that redacts, suppresses, or classifies fields through its own policy. |
| External integrations | integration authority records and adapter contracts | Polylogue/Lynchpin/hledger/task bridge docs | Treating external formats as ontology by convenience. |

## Rules

1. A new mutating surface must name the authority it wraps. If it cannot, open a
   design issue before implementation.
2. `xtask` may orchestrate development and CI evidence, but production mutation
   belongs to NixOS activation, `sinexd::api`/runtime commands, the event
   engine, or explicit repair tools.
3. `sinexctl` may inspect runtime and issue authenticated runtime commands, but
   it should not become a schema migration or development-build surface.
4. Generated catalogs are inspectable projections. They are authoritative only
   when executable consumers use them and the generator is verified. A generated
   assertion list is not proof by itself.
5. Tests may create isolated fixtures, but test helper DDL must not become a
   parallel schema authority.
6. External adapters must declare whether Sinex owns, mirrors, exports, or only
   stages the external domain.

## Consolidation Targets

- Keep removing active event schemas that have no producer or mark them as
  advisory/future so declaration-to-consumer drift stays visible.
- Route source parser material creation through acquisition/material
  helpers rather than parser-local database writes.
- Keep `xtask` runtime commands limited to local development/status views; live
  operation should be exposed through `sinexctl` and `sinexd::api` contracts.
- Convert hand-maintained source lists into projections of source contract
  registrations where practical.
- When strict schema drift finds a live/source mismatch, fix the desired schema
  or explicitly document the non-goal instead of adding another migration path.

## Review Checklist

For any PR that adds a command, table, service, generated file, or integration:

- What concern does it touch?
- Which authority surface owns that concern?
- Is this change adding a wrapper/projection, or a second writer?
- If it is a second writer, what removes or demotes the previous one?
- What command verifies the projection still matches the authority?
