# Sinex Documentation Index

**Purpose:** Map of canonical docs; update when ownership or authoritative references change.

- `current/` — single source of truth for what exists and works today (architecture, security, testing).
- `planning/` — active playbooks (`active/`), roadmap/priorities (`roadmap/`), proposals under consideration (`proposals/`), and backlog (`backlog/TODO.md`).
- `vision/` — long-term direction and explorations.
- `archived/` — superseded/historical material (see `archived/README.md` for pointers).
- `documentation-guidelines.md` — authoring conventions.
- Crate-local docs under `crate/**/docs/` remain authoritative for implementation details.

## Current (what is live now)
- `current/architecture/` — Core architecture, provenance, security architecture, operations, user interaction, event taxonomy.
- `current/security.md` — Current security posture and guardrails.
- `current/testing/` — Testing patterns and guides in use today.

## Planning (what’s next)
- `planning/active/` — Execution playbooks (`way.md`, `implementation-plan.md`).
- `planning/roadmap/` — Roadmap, development/test priorities, feature directions.
- `planning/backlog/TODO.md` — Backlog of tracked tasks.
- `planning/proposals/` — Proposals under review (e.g., DB repository migration).
- `TODO.md` — Backlog of tracked tasks (cross-referenced from planning).

## Vision (long-term)
- `vision/manifesto.md` and `vision/*.md` — Strategic direction and exploratory designs. Check file headers for currency notes.

## Archived
- `archived/` — Superseded analyses/overviews; consult `archived/README.md` for where to find the current equivalents.

## Host / Deployment Notes
Host-specific documentation (NixOS layout, secrets, deployment topology) lives in `/realm/sinnix/docs/{structure,target,breakthrough}.md`. Keep those files authoritative for the `sinnix` host and avoid duplicating them here.

## Contributing to Documentation
- Keep canonical explanations beside the implementation when possible (e.g., crate README for crate-specific behaviour).
- Update this index when a new top-level doc is introduced or relocated.
- If you mine historical archives, port only the verifiable, evergreen portions into the curated docs above.
