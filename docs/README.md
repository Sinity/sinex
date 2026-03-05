# Sinex Documentation Index

**Purpose:** Map of canonical docs; update when ownership or authoritative references change.

## Ecosystem Orientation

Sinex is the **event-sourced kernel** within a three-layer personal infrastructure stack:

```
SINNIX (static)     →  NixOS/home-manager configuration, declarative deployment
     ↓
LYNCHPIN (pull)     →  Read-layer views, dashboards until Sinex captures the data
     ↓
SINEX (push/events) →  Nodes emit events → NATS → ingestd → Postgres → automata
```

For full ecosystem context, see `exploration/ecosystem-context.md`.

## Documentation Structure

- `current/` — single source of truth for what exists and works today (architecture, security, testing).
- `planning/` — development roadmap, priorities, feature proposals, and near-term SDK plans.
- `vision/` — long-term direction, strategic designs, and aspirational architecture.
- `exploration/` — investigation notes, analysis artifacts, and in-progress research.
- `documentation-guidelines.md` — authoring conventions.
- Crate-local docs under `crate/**/docs/` remain authoritative for implementation details.

## Current (what is live now)

- `current/architecture/` — Core architecture, security architecture, operations, user interaction, event taxonomy.
- `current/configuration/` — Shared environment variables (per-service config in crate docs).
- `current/security.md` — Current security posture and guardrails.
- `current/testing/` — Testing overview and pipeline guides (detailed patterns in `xtask/docs/sandbox/`).

## Planning (what's next)

Development priorities, roadmap, and feature proposals:

- `planning/ROADMAP.md` — Long-term roadmap with vision document links.
- `planning/development-priorities.md` — Current development focus areas.
- `planning/features/` — Individual feature proposals (browser extension, embeddings, multi-device sync, etc.).
- `planning/testing-priorities-and-roadmap.md` — Test infrastructure evolution.
- `planning/type-safety-enhancements-roadmap.md` — Type system evolution.

SDK development vision is in `sinex-node-sdk/docs/vision.md`.

## Vision (long-term)

Strategic direction and aspirational architecture:

- `vision/manifesto.md` — Philosophical north star and design principles.
- `vision/architectural-evolution.md` — Strategic system evolution roadmap.
- `vision/multi-device-sync-architecture.md` — Cross-device synchronization.
- `vision/semantic-desktop-stream.md` — AI-powered context understanding.
- `vision/project-target-state.md` — High-level project goals.
- `vision/emergent-insights-and-extensions.md` — Speculative ideas and thought experiments.

Pipeline design is in `sinex-ingestd/docs/pipeline-design.md`.

## Exploration (research & analysis)

Investigation notes and analysis artifacts (not canonical, may be rough):

- `exploration/ecosystem-context.md` — Sinnix ↔ Lynchpin ↔ Sinex relationship.
- `exploration/competitive-landscape.md` — Market positioning and commercial alternatives.
- `exploration/productivity-research.md` — Developer velocity research.
- `exploration/db-repository-migration.md` — SQL migration tracking.
- `exploration/opportunities_tooling.md` — Tooling improvement ideas.
- `exploration/pure-anal/` — Raw analysis transcripts.

## Crate-Level Documentation

Implementation details are documented close to the code:

| Crate | Key Documentation |
|-------|-------------------|
| `sinex-primitives` | Type system, newtypes, validation, error handling |
| `sinex-node-sdk` | Node patterns, provenance, stage-as-you-go, SDK vision |
| `sinex-db` | Database pools, repositories, query helpers |
| `sinex-gateway` | RPC server, transport security, environment |
| `sinex-ingestd` | Event validation, pipeline design, NATS security |
| `sinex-schema` | Database schema, migrations, UUIDv7 identifiers |
| `xtask` | Test patterns (sandbox), build automation, CI pipelines |

Each crate's `docs/README.md` serves as the entry point.

## Host / Deployment Notes

Host-specific documentation (NixOS layout, secrets, deployment topology) lives in `/realm/sinnix/docs/{structure,target,breakthrough}.md`. Keep those files authoritative for the `sinnix` host and avoid duplicating them here.

## Contributing to Documentation

- Keep canonical explanations beside the implementation when possible (e.g., crate README for crate-specific behaviour).
- Update this index when a new top-level doc is introduced or relocated.
- If you mine historical archives, port only the verifiable, evergreen portions into the curated docs above.
