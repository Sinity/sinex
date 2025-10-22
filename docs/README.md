Sinex Documentation Index

Use this index to locate the current sources of truth. “Historical” and “vision” artifacts are background reading only; organisation, invariants, and operational behaviour must come from the documents below.

Architecture & Design
- `docs/architecture/Core_Architecture.md` – end-to-end flow, invariants, and data substrate.
- `docs/architecture/SystemOperations_And_Integrity_Architecture.md` – operating model, observability, recovery.
- `docs/architecture/security-architecture.md` – current security posture and open work.
- `docs/architecture/event-taxonomy.md` – canonical event families and payload minima.
- `docs/way.md` – JetStream ingestion plan (authoritative).

Crate-Level References
- `crate/lib/sinex-core/doc/overview.md` plus the adjacent deep dives – repositories, error handling, and shared types.
- `crate/lib/sinex-satellite-sdk/doc/overview.md` – satellite/automaton lifecycle, processor runner, Stage-as-You-Go.
- `crate/lib/sinex-schema/doc/overview.md` & `crate/lib/sinex-schema/doc/ulid.md` – schema source of truth and ULID integration.
- `crate/lib/sinex-services/doc/README.md` – service layer APIs (analytics, content, PKM, search).
- Each crate under `crate/*/*/doc/` owns its specific deep dives; consult those before adding material to `docs/`.

Operations & Tooling
- `nixos/README.md` – NixOS deployment guide.
- `cli/README.md` & `cli/DESIGN.md` – gateway RPC integration and CLI philosophy.
- `docs/documentation-guidelines.md` – how to add or relocate documentation.
- `TESTING.md` and `crate/lib/sinex-test-utils/doc/testing_quality_overview.md` – testing expectations and utilities.

Roadmap & Vision
- Forward-looking plans: `docs/roadmap/`.
- Speculative/vision work: `docs/vision/`.
- Historical or exploratory analyses remain under `docs/misc-including-high-level-overviews-and-plans/`.

Need something else?
- Prefer crate-local docs when you need implementation detail.
- If a link is stale or a description diverges from the code, add an inline note and open an issue/PR—the goal is to keep canonical explanations beside the implementation.
