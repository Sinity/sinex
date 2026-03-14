# Sinex Documentation Index

**Purpose:** Canonical map of project documentation. Keep this file synchronized with `.claude/includes/reference/docs-map.md`.

## Ecosystem Orientation

Sinex is the **event-sourced kernel** within a three-layer personal infrastructure stack:

```
SINNIX (static)     →  NixOS/home-manager configuration, declarative deployment
     ↓
LYNCHPIN (pull)     →  Read-layer views, dashboards until Sinex captures the data
     ↓
SINEX (push/events) →  Nodes emit events → NATS → ingestd → Postgres → automata
```

## Documentation Structure

- `current/` — authoritative present-state docs (architecture, configuration, security).
- `planning/` — near-term proposals and roadmaps under active consideration.
- `vision/` — long-range direction and aspirational architecture.
- `analysis/` — synthesized investigation output; not policy authority.
- `exploration/` — in-progress investigation notes; promote stabilized content into `current/` or `planning/`.
- `documentation-guidelines.md` — authoring and placement policy.
- Crate-local docs under `crate/**/docs/` remain authoritative for implementation details.

## Current (what is live now)

- `current/architecture/` — core architecture, type system, distributed behavior, observability, and security architecture.
- `current/configuration/` — shared environment variables and configuration policy.
- `current/security.md` — current security posture and guardrails.
- `xtask/docs/verification.md` — perf verification and contracts.
- `xtask/docs/sandbox/README.md` — testing policy and sandbox usage.
- `crate/core/sinex-ingestd/docs/schema_gitops.md` — schema GitOps operational flow.

## Planning (what is next)

- `planning/ROADMAP.md` — staged roadmap.
- `planning/event-sources-coverage.md` — ingestion source coverage plan.
- `planning/explore-ux-roadmap.md` — Explore UX planning.
- `planning/features/` — feature proposals.

## Vision (long-term)

- `vision/manifesto.md` — design principles.
- `vision/architectural-evolution.md` — long-term architecture progression.
- `vision/streaming-architecture.md` — streaming-first architectural direction.
- `vision/project-target-state.md` — target-state narrative.
- `vision/semantic-desktop-stream.md` — semantic desktop direction.
- `vision/multi-device-sync-architecture.md` — multi-device architecture.
- `vision/feature-status.md` — high-level initiative status.

## Analysis and Exploration

- `analysis/synthesis/` — synthesized analysis artifacts used for decision support.
- `exploration/README.md` — entry point for exploratory notes.

## Crate-Level Documentation

Implementation details are documented close to the code:

| Crate | Entry Point |
|-------|-------------|
| `sinex-primitives` | `crate/lib/sinex-primitives/docs/overview.md` |
| `sinex-db` | `crate/lib/sinex-db/docs/README.md` |
| `sinex-node-sdk` | `crate/lib/sinex-node-sdk/docs/README.md` |
| `sinex-schema` | `crate/lib/sinex-schema/docs/README.md` |
| `sinex-services` | `crate/lib/sinex-services/docs/README.md` |
| `sinex-ingestd` | `crate/core/sinex-ingestd/docs/README.md` |
| `sinex-gateway` | `crate/core/sinex-gateway/docs/README.md` |
| `xtask` | `xtask/docs/README.md` |

## Contributing to Documentation

- Keep canonical explanations beside the implementation whenever possible.
- Update this index and `.claude/includes/reference/docs-map.md` in the same change when docs move.
- Keep present-state docs factual; avoid historical narration unless it is required for an active decision.
- Do not prescribe compatibility shims or deprecation wrappers in present-state docs; document the canonical path only.
