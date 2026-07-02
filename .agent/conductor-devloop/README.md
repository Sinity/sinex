# Sinex Conductor Devloop

This is the active Sinex conductor packet. A fresh agent should be able to
resume the devloop from this directory plus `.agent/DEVLOOP.md`, without chat
history.

## Read First

1. `INDEX.md` — routing map and current goal.
2. `ACTIVE-LOOP.md` — current slice, accepted warnings, and next action.
3. `OPERATING-LOG.md` — newest entries first for actual recent work.
4. `RUNBOOK.md` — loop protocol and proof ladder.
5. `PROCESS.md` — focus modes and rotation triggers.
6. `TACTICS.md` — async/heavy-work tactics.
7. `VELOCITY.md` — speed, cadence, and friction rules.
8. `DEMO-RADAR.md` — demo candidates, artifact actions, and caveats.
9. `AHEAD.md` — wait-time lanes, subagent prompts, and reconciliation backlog.
10. `ADVERSARIAL-REVIEW.md` — scaffold and process failure modes.

## Root Boundary

Keep this root small. Live state and protocol files belong here. Supporting
historical notes live in `context/`; bulky generated proof payloads belong in a
named demo packet or another purpose-specific ignored shelf.

Do not reintroduce `.agent/scratch/current`, a handoff mirror, or copied script
snapshots under this directory. `.agent/scripts/` is the executable primitive
surface, and `devloop-sync` regenerates only derived files such as
`EVENTS.jsonl`, `PHASES.jsonl`, and `MANIFEST.md`.

Timestamped handoffs, when useful, live under `context/handoffs/`. They are
snapshots and pointers into the active packet, not a second source of truth.
