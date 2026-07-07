# Ahead Work Queue

This active-state file holds non-blocking work that can advance the devloop
while a compile, test, runtime startup, or external proof is running.

Use it for backlog shaping, bounded audits, demo artifact prep, and subagent
assignments. Do not use it for operator sequencing directives; those belong in
`QUEUE.md`.

## Rules

- Ahead work must not conflict with the active proof lane.
- Default to read-only research, docs/scaffold edits, demo curation, or small
  non-overlapping code edits.
- Subagents need a strict contract: owned paths, avoided paths, forbidden heavy
  commands, output format, and whether edits are allowed.
- Reconcile ahead work after the waited-on proof completes: keep, discard,
  queue, or implement now.

## Entry Template

```markdown
## YYYY-MM-DDTHH:MM:SS+TZ - title

Status: proposed | active | reconciled | discarded
Impact: high | medium | low
Safe surfaces:
Forbidden work:
Expected output:
Stale after:
Reconciliation:
```

## Open Lanes

### 2026-07-02T18:06:51+02:00 - side research reconciliation

Status: complete 2026-07-03 — findings encoded as beads issues (`bd ready`)
Impact: high
Safe surfaces: beads (`bd`), `.agent/conductor-devloop/ACTIVE-LOOP.md`, `.agent/demos/sinex`
Forbidden work: no duplicate compile/test/runtime command while a runtime start
is active
Expected output: reconcile James/Parfit/Fermat/Singer findings into a ranked
queue, then select the next construction slice from the queue.
Stale after: once `recovered_partial` coverage and runtime restart proof are
completed or explicitly superseded.
Reconciliation: keep `recovered_partial` coverage as the next small product
slice; queue query DSL and xtask coordinator visibility as the next larger
substrate/meta slices.

### 2026-07-02T18:06:51+02:00 - runtime build earlyoom diagnosis

Status: active
Impact: high
Safe surfaces: `journalctl`, `xtask analytics pressure`, process/cgroup
inventory, `.agent/conductor-devloop/OPERATING-LOG.md`
Forbidden work: do not lower Sinex workload or permanent build policy to hide
the failure
Expected output: prove whether `xtask run core` failed from code, xtask, or
host pressure; clear stale agent-tool memory if implicated; retry once with
evidence.
Stale after: dev `sinexd` starts from current `master` or a reproducible
tooling bug is filed/implemented.
Reconciliation: early evidence says earlyoom killed `rustc` at 5% MemAvailable
while stale Codebase Memory/Serena processes were resident; stale agent scope
was stopped before retry.

### 2026-07-01T19:48:00+02:00 - contextual ahead suggestions

Status: proposed
Impact: medium
Safe surfaces: `.agent/scripts/devloop-ahead`, `.agent/conductor-devloop/AHEAD.md`
Forbidden work: no test/build/runtime commands
Expected output: `devloop-ahead --contextual` prints three ranked safe tasks
from active focus, active proof lane, host pressure, and this file.
Stale after: when wait-state reconciliation is automated
Reconciliation: implement after the current runtime-liveness proof commit if it
still looks useful.

### 2026-07-01T19:48:00+02:00 - wait completion lifecycle

Status: proposed
Impact: high
Safe surfaces: `.agent/scripts/devloop-wait`, `.agent/scripts/devloop-review`,
`.agent/conductor-devloop/OPERATING-LOG.md`
Forbidden work: no broad log parser rewrite during a proof wait
Expected output: a command or convention that records proof result, ahead work
done, and keep/discard/defer decision.
Stale after: once repeated wait entries already show explicit reconciliation.
Reconciliation: promote before the next substantial meta pass.

### 2026-07-01T19:48:00+02:00 - subagent prompt template

Status: proposed
Impact: medium
Safe surfaces: `.agent/conductor-devloop/TACTICS.md`, `.agent/conductor-devloop/RUNBOOK.md`
Forbidden work: no agent launch automation until the template is stable
Expected output: copyable research-only and edit-allowed prompt shapes with
owned paths, avoided paths, forbidden commands, and output schema.
Stale after: when subagent usage becomes rare or fully tool-supported.
Reconciliation: fold into `TACTICS.md` after this file proves useful.

### 2026-07-01T19:52:00+02:00 - source-status filtered latency

Status: proposed
Impact: high
Safe surfaces: `crate/sinexd/src/api/handlers/source_status.rs`,
`crate/sinex-db/src/repositories/state.rs`, `crate/sinexctl/src`
Forbidden work: do not mask with longer CLI timeouts; measure handler/query
shape first
Expected output: `sinexctl sources status browser.history --format json`
returns quickly, or the handler explains/progresses expensive full-view work
separately from filtered status.
Stale after: once source-status filtered calls are under one second on the
active dev store.
Reconciliation: make this a near-next runtime/ops velocity slice because it
blocked live proof collection after the liveness projection fix.
