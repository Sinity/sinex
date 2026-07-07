# .agent — Sinex Repo Agent Surface

Orientation for agents. Always-loaded rules live in `CLAUDE.md` (= `AGENTS.md`);
repo conventions in [`CONVENTIONS.md`](CONVENTIONS.md).

- **Task substrate**: Beads. `bd prime` → `bd ready` → claim → work → PR →
  close with reasons. There is no separate devloop scaffold; the former
  conductor packet is archived at `archive/devloop-2026-07/` (its README maps
  each retired piece to what subsumed it).
- `CONVENTIONS.md` — bead content bar, graph lint invariants, execution
  tactics, PR cadence, scratch/demo/git-boundary rules.
- `scripts/` — small helpers: `check-tool-usage.sh` (hook-style tool-usage
  checker), `codex-dev` (run Codex CLI inside the repo devshell).
- `scratch/` — gitignored thinking space. Current code-grok entry point is the
  highest-numbered `NNN-*.md` note (see the `code-grok-*` bd memory).
- `dev/` — small tracked dev artifacts (recall proof packets, dev source
  bindings).
- `inbox/` — external analysis batches awaiting verification (`INDEX.md`).
- `demos/` — curated demo shelf (`demos/sinex/CURATED_CATALOG.md`).
- `archive/` — retired scaffolds kept as evidence, never resurrected.
