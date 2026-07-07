# Adversarial Scaffold Review

Created: 2026-06-30

## Threat Model

Assume a future agent is tired, under context pressure, and trying to move fast.
The scaffold fails if it becomes ceremony, drifts from the inbox packet, hides
active resource pressure, or lets TODO logs masquerade as handoff.

## Findings And Mitigations

### Active Mirrors Can Drift From The Conductor Root

Risk: an agent recreates `.agent/scratch/current`, a handoff mirror, copied
scripts, or root-level handoff files. The copies silently diverge from
`.agent/conductor-devloop/`.

Mitigation: `devloop-review` checks for scratch/current, retired handoff
mirrors, conductor script snapshots, root handoff files, and unexpected loose
root files. `devloop-sync` regenerates only derived sidecars and a manifest.

### Logs Can Rot Into TODO Templates

Risk: timestamped logging helps only if entries are filled. A series of template
TODOs would create false confidence.

Mitigation: `devloop-review` inspects the latest operating-log entry and warns
if it still contains `TODO`.

### Time Awareness Was Not Computed

Risk: asking agents to track elapsed time manually is fragile.

Mitigation: `devloop-log` now computes elapsed wall time since the previous
timestamped entry when GNU `date` can parse both timestamps.
`devloop-refresh-events` also writes `PHASES.jsonl`, a generated phase/focus
stream from `OPERATING-LOG.md`. `devloop-velocity` reads that sidecar and
prints recent transitions plus the largest recent phase gaps, so time loss is
visible without hand-scanning the operating log.

### Process Pressure Can Hide Behind "No Active Xtask Jobs"

Risk: no active `xtask` job does not mean the host is idle. Duplicate Codex
sessions, Polylogue catch-up, stale Postgres/NATS scopes, and MCP servers can
dominate RAM/IO.

Mitigation: `devloop-status` prints likely loop-affecting processes, and
`devloop-review` warns on Codex multiplicity, `polylogued`, and active
build/test-like process names.

### Role Language Could Become Identity Theater

Risk: "roles" can sound like separate personas instead of operational focus.

Mitigation: `PROCESS.md` was rewritten as focus modes with entry triggers,
questions, exits, and mode-switch log shorthand.

### Tactics Can Still Be Ignored

Risk: "do something while compiling" remains aspirational if not attached to a
checkpoint.

Mitigation: `devloop-checkpoint` asks what useful foreground work is next when
a heavy job is running, and `devloop-ahead` prints concrete options.

## Remaining Weaknesses

- The scripts warn but do not enforce; this is intentional for now because
  false positives should not block urgent work.
- `devloop-review` is process-name based and will miss unusual wrappers.
- Elapsed-time tracking is transition-oriented. It does not yet infer the full
  `t0..t6` loop timeline when entries omit explicit loop phases.
- Handoffs are snapshots under `context/handoffs/`; review checks for TODO
  placeholders there, but it cannot prove the summary is complete.
- Demo manifest regeneration exists through `devloop-refresh-demos` and
  `devloop-sync`, but choosing when to retire, merge, or caveat stale demo
  packets still requires judgment.

## Next Hardening Ideas

- Teach the phase extractor to infer full `t0..t6` loop segments instead of
  only reporting adjacent transition gaps.
- Add a demo manifest validator for `/realm/inbox/demos_sinex`.
- Add a pressure budget summary using machine telemetry PSS/cgroup rows instead
  of only live `ps`/PSI.
