---
created: "2026-06-30T22:40:00Z"
purpose: "Root/dotfile/tree organization audit"
status: "active"
project: "sinex"
---

# Root And Tree Organization Audit

## Current Findings

- Tracked root files are mostly canonical project/tool entrypoints:
  `Cargo.toml`, `Cargo.lock`, `flake.nix`, `flake.lock`, Rust toolchain/fmt/analyzer
  config, `README.md`, `TESTING.md`, `CONTRIBUTING.md`, and repo service dotfiles.
- `.gitignore` already ignores checkout-local runtime/build state:
  `.sinex/*` except `.sinex/.gitignore`, `crate/**/.sinex/`, `xtask/.sinex/`,
  `.direnv/`, `.agent/scratch/`, `.agent/demos/`, `.agent/handoff/`, and
  `.claude/worktrees/`.
- Safe root cleanup performed: removed ignored `result` symlink.
- Important local sprawl that should not be blindly deleted:
  `.claude/worktrees/` contains six Git-registered worktrees. Several are dirty:
  `agent-a2c7e0005d2a7f3ae`, `agent-a4fb3e6b355fa0f30`, and
  `agent-ac74836b5d8274e95`.
- Heavy ignored state:
  `.sinex` is about 3.2 GiB; `crate/sinex-primitives/.sinex` is about 2.5 GiB
  from trybuild target state. These are ignored cache/runtime state, not tracked
  root clutter, but they matter for disk/tree noise.

## Conclusions

- Do not gitignore `.agent` wholesale. The tracked `.agent` surface is now an
  intentional devloop handoff/scaffold; ignored subtrees carry volatile demos,
  scratch, handoff copies, and artifacts.
- Do not delete `.claude/worktrees/` until dirty worktrees are either committed,
  merged, archived, or explicitly discarded. They are root-tree clutter, but
  currently contain real work.
- Future agent worktrees should live under `/realm/tmp/worktrees/` per the
  environment guidance. Repo-local `.claude/worktrees` should be treated as a
  legacy/stale location to drain, not the ongoing default.
- Root file relocation opportunities are limited. Moving Rust/Nix/GitHub config
  files would likely break common tooling or reduce discoverability. The better
  cleanup target is local state/worktree placement, followed by flat source/test
  directory splits where those reduce production-file sprawl.

## Candidate Actions

- Inventory each `.claude/worktrees/agent-*` branch and decide: merge/cherry-pick,
  archive as patch, or discard. Only after that remove the worktree registrations.
- Add or improve local devloop guidance so future subagents create worktrees under
  `/realm/tmp/worktrees/`, not inside this repo.
- Consider whether trybuild target state under `crate/sinex-primitives/.sinex` can
  be safely pruned by xtask cache hygiene rather than manual deletion.
- Continue mechanical inline-test splits using sibling `src/foo_test.rs` files.
