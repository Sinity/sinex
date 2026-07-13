# Sinex documentation

Sinex documentation follows the code that owns each contract. This page is the
reader-facing map; crate-local indexes remain authoritative for their detailed
surfaces.

## Understand the system

- [Architecture deep dive](architecture.md) explains provenance, time,
  identity, replay, topology, and persistence.
- [Glossary](glossary.md) defines the project vocabulary.
- [Automaton chaining](architecture/automaton-chaining.md) describes derived
  pipelines, circuit breaking, and failure isolation.
- [OpenTelemetry projection boundary](architecture/otel-projection-boundary.md)
  separates interoperable telemetry from richer Sinex evidence views.

## Capture and derive evidence

- [Source documentation](../crate/sinexd/docs/sources/README.md) is the main
  entry point for source contracts, staged materials, parser packages,
  readiness, replay, and lifecycle.
- [Automata documentation](../crate/sinexd/docs/automata/README.md) covers
  derived-event processing and lineage.
- [Event taxonomy](../crate/sinex-db/docs/schema/event-taxonomy.md) documents
  persisted event naming and schema conventions.
- [Knowledge boundaries](../crate/sinex-primitives/docs/knowledge_boundaries.md)
  distinguishes evidence, typed records, graphs, and artifacts.

## Operate Sinex

- [sinexctl documentation](../crate/sinexctl/docs/README.md) covers the CLI,
  TUI, MCP reader, private mode, snapshots, and operator data lifecycle.
- [Operator surfaces](../crate/sinexctl/docs/operator_surfaces.md) records the
  authority boundary shared by the CLI, TUI, MCP, shell, and launchers.
- [NixOS deployment](../nixos/README.md) covers module composition, service
  deployment, secrets, and runtime hardening.
- [Runtime-target boundaries](../xtask/docs/runtime-target-boundaries.md)
  separates checkout tooling, live runtime probes, VM tests, and host proof.

## Develop and verify

- [Contributing](../CONTRIBUTING.md) defines the Beads, branch, review, and
  merge workflow.
- [Testing](../TESTING.md) maps test tiers and protected contracts.
- [xtask](../xtask/docs/README.md) is the repository automation and
  verification entry point.
- [Architecture authority map](../.github/authority-surfaces.md) identifies
  which surface owns each runtime and data concern.

Active work belongs to the committed Beads graph. Browse the
[web board](https://sinity.github.io/sinex/beads/) or use `bd ready` locally.
