Sinex Documentation Index
> **Purpose:** Map of canonical docs; update whenever ownership or authoritative references change.

Use this index to locate the current sources of truth. Historical essays and
exploratory brainstorming live in `docs/historical/` unless they are promoted
into the curated list below.

## Core Architecture

- `docs/way.md` – JetStream refactor playbook and ingestion canon. Treat this as
  the authoritative flow until it says otherwise.
- `docs/architecture/Core_Architecture.md` – end-to-end system shape and data
  substrate.
- `docs/architecture/provenance.md` – sensor/ingestor boundaries,
  Stage-as-you-go execution, and provenance expectations.
- `docs/security.md` – live security posture, open gaps, and contributor
  guardrails (pairs with the broader architecture note below).
- `docs/architecture/security-architecture.md` – threat model and defence
  layers; update alongside the security posture doc.

## Implementation References

- Each crate under `crate/**/doc/` documents its domain (core types, satellite
  SDK, schema, services, test utils). Prefer crate-local docs for implementation
  detail before expanding this index.
- `TESTING.md` and `crate/lib/sinex-test-utils/doc/testing_quality_overview.md`
  define testing contracts.
- `docs/documentation-guidelines.md` covers authoring conventions.

## Vision & Roadmap

- `docs/vision/manifesto.md` – consolidated principles and strategic
  trajectory (supersedes scattered “vision” documents).
- `docs/vision/*.md` – individual explorations; heed the operational notes at
  the top of each file for currency.

## Host / Deployment Notes

Host-specific documentation (NixOS layout, secrets, deployment topology) lives
in the system configuration repository: `/realm/sinnix/docs/{structure,target,breakthrough}.md`.
Keep those files authoritative for the `sinnix` host and avoid duplicating them
here.

## Contributing to Documentation

- Keep canonical explanations beside the implementation when possible (e.g.,
  crate README for crate-specific behaviour).
- Update this index whenever a new top-level doc is introduced or a pointer is
  retired.
- If you need context that lives in historical archives, port only the
  verifiable, evergreen portions into the curated docs above.
