# Documentation Guidelines

Use this guide when adding or updating written material across the Sinex
workspace. The aim is to keep the authoritative explanation as close as
possible to the code it describes, while ensuring discoverability through
rustdoc.

## 1. Documentation Principles

- **Single source of truth** – rich design, architecture, and workflow notes
  live next to the code they describe. Inline comments stay short and always
  point toward the canonical Markdown source.
- **Layered narrative** – crate-local documentation owns the immediate “what”
  and “how”, while workspace-level docs under `docs/` provide cross-cutting
  stories (architecture decisions, roadmaps, operations guides). Link between
  layers so readers can move up or down the stack easily.
- **Guaranteed discoverability** – every crate surfaces its main reference
  material directly through rustdoc so `cargo doc` reaches the same context as a
  filesystem browse.

## 2. Crate Layout Requirements

1. Create a `doc/` directory in every crate root (`<crate>/doc/`).
2. Add `doc/README.md` describing the crate’s responsibility, major entry points,
   and its relationships to the rest of the system.
3. Write one Markdown file per deep dive (e.g. `doc/replay_state_machine.md`,
   `doc/jetstream_pipeline.md`). Co-locate diagrams or assets alongside.
4. Keep narrow, tactical notes (field explanations, algorithm details) inside
   the Markdown file. The corresponding `.rs` file should only retain a short
   pointer such as `//! See crate::doc::replay_state_machine`.
5. When documentation moves into a crate, delete the old global file rather than
   leaving a stub. Update links across the repo to reference the new canonical
   location.

## 3. Rustdoc Inclusion Guide

- At the crate root, include the overview via `#![doc = include_str!("doc/README.md")]`.
- For modules with companion Markdown, add `#![doc = include_str!("doc/<file>.md")]`
  (or the appropriate relative path) alongside the module definition.
- If a module benefits from an inline summary, keep a 1–2 sentence `//!` block
  above the include.
- Use crate-relative paths for intra-crate references and workspace-relative
  paths when pointing to material under `docs/`. This keeps links stable if the
  crate moves.

Example:

```rust
#![doc = include_str!("doc/README.md")]
#![doc = include_str!("../../docs/architecture/runtime/topology.md")]
```

## 4. Workspace Documentation

- The top-level `docs/` directory hosts architecture books, blueprints,
  historical analyses, and other cross-cutting material.
- Crate-level Markdown should link upward when wider background already exists.
  For example, a gateway module deep dive can reference
  `../../docs/architecture/data-plane.md` for wider context.
- When crate-level changes alter system-wide behaviour, update the relevant
  global doc and leave a short link from the crate so readers can follow the
  chain.

## 5. Migration Checklist

When touching a crate, run through this list:

1. [ ] `doc/` directory exists with `README.md` and any deep dives.
2. [ ] Crate root uses `#![doc = include_str!(...)]` for the overview and any
       relevant global documents.
3. [ ] Modules include their Markdown counterparts via `#![doc = include_str!(...)]`.
4. [ ] Inline documentation is limited to short summaries or essential notes.
5. [ ] Integration and property tests live under `<crate>/tests/` unless
       exercising private-only helpers.
6. [ ] Inline tests are justified (proc-macro parsing, minimal helper coverage).
7. [ ] Workspace docs are linked when they provide extended rationale.
8. [ ] `devenv tasks run dev:check` and `devenv tasks run dev:test` succeed
       locally after documentation or test moves (Nextest-only; `cargo test`
       is unsupported).

Keeping these steps in sync ensures documentation stays discoverable and aligned
with the code that implements it.
