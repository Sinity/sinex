# Archived: conductor-devloop packet (retired 2026-07-08)

This directory preserves the bespoke devloop/conductor scaffold that ran the
Sinex dogfood loop from roughly 2026-06-27 to 2026-07-07. It is **evidence,
not scaffold** — do not resume these files or the `devloop-*` script names.
(The packet's own entrypoint at retirement time is `PACKET-README.md`.)

## Why retired

Beads (`bd`) subsumed the coordination roles, and the remainder was either
distilled into `.agent/CONVENTIONS.md` / `CLAUDE.md` or deliberately dropped:

| Retired piece | Purpose | Subsumed by |
|---|---|---|
| `ACTIVE-LOOP.md`, `QUEUE.md`, `AHEAD.md` | current focus, deferred directives, backlog lanes | `bd` claims / directive beads / `bd ready` + wave labels |
| `OPERATING-LOG.md` | timestamped loop journal | bead notes per work item + polylogue session capture; this log stays as the historical corpus for sinex-pya / sinex-hlv / cem.10 (devloop-as-source) |
| `EVENTS.jsonl`, `PHASES.jsonl` | generated event/phase streams | deleted (regenerable from the log; superseded by polylogue + future sinex-native capture) |
| `DEVLOOP.md` cold start + `devloop-handoff` | recovery without chat history | `bd prime` + CLAUDE.md Session Orientation + `bd memories` |
| `RUNBOOK.md`, `PROCESS.md` focus modes/gates | conductor discipline | CLAUDE.md operator contract + verification cadence; focus-mode machinery dropped |
| `TACTICS.md` | async verify, heavy-job overlap, proof ladder, greedy batch | distilled into CONVENTIONS.md (Execution Tactics, Greedy Batch) |
| `VELOCITY.md` | speed accounting | dropped; cem.10 answers this from sinex data when it lands |
| `devloop-*` scripts + `scripts/lib/` | wrap the packet state | retired with the packet |
| `devloop-contract.json` | cross-repo packet contract | retired (it referenced the deleted `.agent/includes/**`; beads needs no contract file) |
| `ADVERSARIAL-REVIEW.md`, `SELF-PROMPTS.md`, `MANIFEST.md`, `INTEGRATION.md`, `context/` | supporting notes | archived as-is; mine `context/` selectively if needed |

## Corpus value

`OPERATING-LOG.md` (~1MB) and `context/` are a dense record of real agent
loops: decisions, proofs, focus transitions, failure modes. The beads
`sinex-pya` (dogfood conductor state into Sinex) and `sinex-hlv` (beads/devloop
as Sinex sources) plus demo `cem.10` treat this directory as import material.
