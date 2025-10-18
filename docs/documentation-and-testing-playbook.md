# Documentation & Testing Playbook

This playbook captures how Sinex organizes written design context. Treat it as
the canonical reference when adding new crates or wiring up rustdoc. For
testing and QA policy, see `crate/lib/sinex-test-utils/doc/testing_quality_overview.md`.

## 1. Documentation Principles

- **Single source of truth** – rich design, architecture, and workflow notes live
  next to the code they describe. Inline comments stay short and always point
  toward the canonical Markdown source.
- **Layered narrative** – crate-local documentation owns the immediate “what” and
  “how”, while workspace-level docs under `docs/` provide cross-cutting stories
  (architecture decisions, roadmaps, operations guides). Link between layers
  liberally so readers can climb or descend the stack.
- **Guaranteed discoverability** – every crate must surface its main reference
  material directly through rustdoc so `cargo doc` readers reach the same context
  as someone browsing the filesystem.

## 2. Crate Layout Requirements

1. Create a `doc/` directory in every crate root (`<crate>/doc/`).
2. Add `doc/README.md` that explains the crate’s responsibility, major public
   entry points, and how it interacts with the rest of the system.
3. Write one Markdown file per deep-dive topic or module:
   - Name files after the module or concern (`doc/replay_state_machine.md`,
     `doc/grpc_client.md`).
   - Co-locate diagrams or auxiliary assets under the same directory when needed.
4. Keep narrow, tactical notes (field explanations, algorithm choices) inside the
   Markdown. The corresponding `.rs` file should only retain a short pointer such
   as `//! See crate::doc::replay_state_machine`.

## 3. Rustdoc Inclusion Guide

- At the crate root, include the overview via `#![doc = include_str!("doc/README.md")]`.
- For modules that have companion Markdown, place `#![doc = include_str!("doc/<file>.md")]`
  (or the appropriate relative path) on the module.
- When a module benefits from a short inline summary, keep a 1–2 sentence `//!`
  block in the source above the include. This provides instant context without
  duplicating the long-form documentation.
- Cross-reference global documentation by chaining includes. Example:
  ```rust
  #![doc = include_str!("doc/README.md")]
  #![doc = include_str!("../../docs/architecture/runtime/topology.md")]
  ```
- Use crate-relative paths for intra-crate references and workspace-relative
  paths when pointing to `docs/`. This keeps links stable if the crate moves.

## 4. Workspace Documentation

- The top-level `docs/` directory continues to host architecture books,
  blueprints, historical analyses, and other cross-cutting materials.
- Crate-level Markdown should link upward when additional background already
  exists. For example, a gateway module deep dive can reference
  `../../docs/architecture/data-plane.md` for broader context.
- Update global docs when crate-level changes alter system-wide behaviour; the
  crate doc should highlight the relationship and trust the reader to follow the
  link for the full story.

## 5. Further Reading

- Testing policy, suite layout, Nextest profiles, and CI expectations are
  documented alongside the harness in
  `crate/lib/sinex-test-utils/doc/testing_quality_overview.md`.
- The workspace `tests/` directory contains per-suite READMEs where additional
  guidance is helpful (property tests, VM fixtures, etc.).

## 6. Migration Checklist

When touching a crate, run through this list:

1. [ ] `doc/` directory exists with `README.md` and module deep dives.
2. [ ] Crate root uses `#![doc = include_str!(...)]` for the overview and any
       relevant global documents.
3. [ ] Modules include their Markdown counterparts via `#![doc = include_str!(...)]`.
4. [ ] Inline documentation is limited to short summaries or essential notes.
5. [ ] Integration and property tests live under `<crate>/tests/` unless
       exercising private-only helpers.
6. [ ] Inline tests are justified (proc-macro parsing, minimal helper coverage).
7. [ ] Workspace docs are linked when they provide extended rationale.
8. [ ] `just check` and `just test` succeed locally after documentation or test
       moves.

Following this playbook keeps Sinex documentation discoverable and our tests
reliable, while avoiding the drift that comes from scattering long-form context
throughout the source tree.
