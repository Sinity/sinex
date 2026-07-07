# Sinex Devloop Self-Prompts

Use these prompts when context is thin, when a loop stalls, or when a heavy
command is running and the next useful action is not obvious.

## Resume Prompt

Continue the Sinex dogfood/demo devloop from `.agent/DEVLOOP.md`. Run
`.agent/scripts/devloop-status` and `.agent/scripts/devloop-review`, read
`.agent/conductor-devloop/INDEX.md`, `ACTIVE-LOOP.md`, and the newest
`OPERATING-LOG.md` entries, then choose the highest-value next slice from live
evidence. Keep the active loop rooted in `.agent/conductor-devloop/`, not
`.agent/scratch/current` or a handoff mirror.

## Slice Prompt

Pick one capability slice that improves Sinex quickly and can produce an
inspectable demo or proof. State the demo value, reusable substrate, proof
ladder, non-goals, and first action in `OPERATING-LOG.md`. Prefer general
acquisition/query/evidence/projection/rendering substrate over one-off demos or
CLI-only paths.

## Wait Prompt

If a build, test, import, daemon start, or runtime probe is running, record the
wait with `devloop-wait`, then rotate focus instead of idling. Useful rotations:
review adjacent call sites, update a demo packet skeleton, refresh the active
loop log, inspect current runtime/DLQ state, improve a helper script, or plan
the next verification batch.

## Meta Prompt

When the process itself slows work down, fix the scaffold or tool directly.
Look for duplicated state, stale generated mirrors, unclear source-of-truth
rules, script-name divergence from Polylogue, missing machine-readable sidecars,
or checks that agents repeatedly bypass. Keep the fix concrete and return to
object-level Sinex capability work.

## Demo Prompt

Before ending a loop that changed capability evidence, ask: what can be shown
to an external reader now? Refresh `.agent/demos/sinex` manifests when useful,
record caveats, and retire or consolidate weak artifacts. Do not preserve raw
dumps just because they exist; demos are curated product evidence.
