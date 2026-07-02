# Recall Artifacts

Tracked compact artifacts for recall/context demos. Full JSON proof packets live
under `.agent/demos/sinex/` and are regenerated as ignored demo shelf state; this
directory keeps small durable proof notes that should survive a clean checkout.

## Recall v2

- `RECALL-v2-audit-20260703.md` — compact cold-reader audit of the current
  Recall v2 baseline-arm packet. It proves the refreshed fs+git+shell/context
  shape and records why the terminal fs+git+shell+browser claim is not yet
  satisfied.

## Recall Silo Collapse

Tracked before/after artifacts for the 2026-06-28 recall silo-collapse demo.

These files are proof material for the shift from CLI-private recall output
toward shared family-aware context view primitives:

- `before-context-*.txt` — old default/desktop context output.
- `after-context-*.txt` and `after-context-default.json` — replacement output
  with activity/derived/self-observation separation.
- `DEMO-recall-silo-collapse.md` — human-readable demo explanation.
