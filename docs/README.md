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

- `current/` — authoritative present-state docs (architecture and security).
- `documentation-guidelines.md` — authoring and placement policy.
- Crate-local docs under `crate/**/docs/` remain authoritative for implementation details.
- General vision, target-state, gap, and cross-cutting planning synthesis live in the sibling report repo at `/realm/project/sinex-target-vision/`.

## Current (what is live now)

- `current/architecture/` — cross-cutting architecture and security/integrity invariants.
- `nixos/modules/README.md` — canonical deployment configuration surface.
- `current/security.md` — current security posture and guardrails.
- `xtask/docs/verification.md` — perf verification and contracts.
- `xtask/docs/sandbox/README.md` — testing policy and sandbox usage.
- `crate/core/sinex-ingestd/docs/schema_gitops.md` — schema GitOps operational flow.

## Cross-Cutting Vision And Planning

- Maintained roadmap, target-state architecture, and gap framing live in `/realm/project/sinex-target-vision/`.
- Keep future cross-cutting horizon docs there unless the material is directly implementation-facing for a specific crate or live subsystem.

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
| `sinexctl` | `crate/cli/docs/README.md` |
| `xtask` | `xtask/docs/README.md` |

Key moved architecture topics:

- query/read path and gateway coordination: `crate/core/sinex-gateway/docs/`
- current-state tracking: `crate/lib/sinex-services/docs/current_state_tracking.md`
- data lifecycle: `crate/lib/sinex-db/docs/data_lifecycle.md`
- type system and NATS subjects: `crate/lib/sinex-primitives/docs/`
- distributed runtime, observability, extensibility: `crate/lib/sinex-node-sdk/docs/`

## Contributing to Documentation

- Keep canonical explanations beside the implementation whenever possible.
- Update this index and `.claude/includes/reference/docs-map.md` in the same change when docs move.
- Keep present-state docs factual; avoid historical narration unless it is required for an active decision.
- Do not prescribe compatibility shims or deprecation wrappers in present-state docs; document the canonical path only.
