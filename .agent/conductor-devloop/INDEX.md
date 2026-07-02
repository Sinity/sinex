# Current Conductor Packet

This directory is the active Sinex conductor/devloop packet.

## Minimum Startup Read

1. `README.md`
2. `ACTIVE-LOOP.md`
3. `OPERATING-LOG.md`
4. `RUNBOOK.md`
5. `PROCESS.md`
6. `TACTICS.md`
7. `VELOCITY.md`
8. `DEMO-RADAR.md`
9. `AHEAD.md`
10. `ADVERSARIAL-REVIEW.md`
11. `context/2026-06-30-conductor-sinex-assimilation.md`
12. `context/2026-06-29-devloop-demo-query-priority.md`

## Supporting Current Context

- `README.md` — active packet entrypoint and root-boundary rule.
- `context/000-capability-log.md` — running capability log.
- `context/001-standing-goal.md` — earlier standing-goal note.
- `context/014-dogfood-session-2026-06-29.md` — previous dogfood session details.
- `context/016-demo-value-plan-assimilation.md` — demo-value framing.
- `context/017-algebra-audit.md` and `context/018-recall-algebra-silo.md` — silo-vs-algebra
  findings.
- `context/2026-06-29-demo-packet-curation.md` — `/realm/inbox/demos_sinex` curation.
- `context/2026-06-29-devloop-audit-swarm.md` — recent audit swarm summary.
- `context/2026-06-29-agent-performance-reflection.md` — process reflection.
- `context/2026-06-29-ram-io-pressure-investigation.md` — resource/host-pressure note.
- `context/2026-06-29-sinexctl-sinexd-build-boundary.md` — sinexctl/sinexd build
  boundary note.
- `OPERATING-LOG.md` — timestamped detailed loop log; append here during work.
- `EVENTS.jsonl` — generated structured event stream from the operating log.
- `PHASES.jsonl` — generated phase/focus stream for timing and velocity review.
- `VELOCITY.md` — time model and acceleration rubric for the conductor loop.
- `AHEAD.md` — non-blocking backlog/audit lanes for productive wait time.
- `INTEGRATION.md`, when present locally, is branch-specific integration state
  and should be treated as the current PR-boundary plan for the long-lived
  devloop branch. It is ignored live state, not durable scaffold.
- `PROCESS.md` — focus modes, mode-switch triggers, and trajectory-adjustment
  rules for the single conductor agent.
- `TACTICS.md` — async verification, compile/test overlap, proof ladder, and
  non-idle work rules.
- `ADVERSARIAL-REVIEW.md` — failure modes in the scaffold and current
  mitigations.
- `RUNBOOK.md` — exact start gate, focus state machine, heavy-job protocol, and
  end gate.
- `ACTIVE-LOOP.md` — current focus, trigger, accepted warnings, and next action.
- `context/` — supporting notes moved out of the active root to keep startup
  state small. Mine selectively; promote durable rules into tracked scaffold or
  includes instead of growing this packet root.
- `context/handoffs/` — optional timestamped handoff snapshots. These point to
  active state and proof files; they do not replace `ACTIVE-LOOP.md` or
  `OPERATING-LOG.md`.

## Current Goal

Conduct the Sinex dogfood/demo devloop indefinitely: continuously choose the
highest-value live-data capability slice, produce inspectable artifacts proving
that Sinex makes agents and the operator better at reconstructing real work and
machine/personal context, collapse silos into general acquisition/query/evidence
projection/rendering substrate, verify on the active store or live local
captures, update the operating log and handoff, and use each loop's evidence to
reprioritize the next slice while maximizing devloop velocity.
