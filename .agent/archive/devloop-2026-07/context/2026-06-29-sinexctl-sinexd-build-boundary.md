---
created: "2026-06-29T13:19:00+02:00"
purpose: "Capture dev-loop velocity evidence from sinexctl context-pack work"
status: "active"
project: "sinex"
---

# sinexctl -> sinexd Build Boundary

## Context

While adding `sinexctl events context --artifact-dir`, focused checks passed
quickly enough once warm, but producing a runnable `sinexctl` binary for a live
runtime smoke exposed a velocity problem: `xtask build -p sinexctl` repeatedly
compiled the large `sinexd` library and was terminated by SIGTERM with zero Rust
diagnostics.

The build succeeded only with a one-shot low-fanout override:

```bash
CARGO_BUILD_JOBS=1 SINEX_CARGO_TIMEOUT=1800 xtask build -p sinexctl --bg
xtask jobs wait 2000201
```

That job completed successfully in about 107 s. Earlier unconstrained attempts
failed around 32-62 s with `errors: 0`, `warnings: 11`, and stderr showing
`rustc ... crate/sinexd/src/lib.rs ... (signal: 15, SIGTERM)`.

## Findings

The `sinexctl` dependency on `sinexd` is not only a trivial logging helper:

- `crate/sinexctl/src/main.rs` and `src/bin/sinex-mcp-server.rs` import
  `sinexd::runtime::service_runtime` just to load the tracing env filter.
- `crate/sinexctl/src/commands/blob.rs` imports
  `sinexd::runtime::content_store::{ContentStoreConfig, MaterialContentStore,
  cas_fsck, gc, ...}` for local blob/CAS maintenance commands.
- Therefore a clean decoupling is not just "copy one helper"; the content-store
  primitives currently live under the daemon runtime namespace even though some
  of them are operator/CLI maintenance primitives.

## Implication

This is a dev-loop velocity tax on CLI work. A small `sinexctl` UX/demo change
can require compiling the daemon library, and under live host pressure that can
turn a command-level smoke into a multi-minute build with SIGTERM retries.

## Next Shape

Likely durable fix: move daemon-independent content-store primitives out of
`sinexd::runtime` into a lighter shared crate boundary, probably `sinex-db` if
the operations are DB/content-store repository concerns, or a new focused
workspace crate only if `sinex-db` would become semantically muddled.

Avoid a rushed feature gate that merely hides `ops blob`; the goal is to keep
operator capability while making the common `sinexctl` build path cheaper.
