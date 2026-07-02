# Recall v2 Audit - Baseline Arm

Captured: 2026-07-03T00:13+02:00

Full ignored demo packet:
`.agent/demos/sinex/sinex-recall-v2-audit-20260703T0013Z/`

## Window

- UTC: `2026-07-02T18:04:00Z .. 2026-07-02T18:42:59Z`
- Local: `2026-07-02 20:04:00+02:00 .. 20:42:59+02:00`
- Real work anchor: Sinex PR train #2242 through #2246.

## Verdict

Not terminal. The refreshed packet is a useful Recall v2 audit artifact, but it
does not satisfy the queued terminal target because browser activity is absent
from the selected work window.

## Evidence

Raw baselines:

- `git log` returns five PR-train commits: #2242, #2243, #2244, #2245, #2246.
- `atuin search` returns three commands in the matching UTC window:
  `z poly`, `tokei`, and `claude`.

Sinex context:

- `sinexctl events context --since 2026-07-02T18:04:00Z --until 2026-07-02T18:42:59Z --limit 200 -f json`
  returns 201 events across 9 sources.
- Sources:
  `journald`, `sinexd.event_engine`, `sinex`, `sinexd.automaton`,
  `sinexd.source`, `shell.atuin`, `derived.activity-window`,
  `knowledge-graph`, `fs-watcher`.
- The useful improvement over the earlier packet is source-priority top-up:
  `shell.atuin` and `fs-watcher` appear in the context output even though
  self-observation dominates the raw event stream.

Runtime caveats captured in the full packet:

- Catch-up verdict: `blocked`.
- Catch-up summary:
  `dlq=0 materials=3817 failed=579 partial=227 remediation_candidates=760 remediation_events=2229714 runtime_active=5 inactive=16`.
- DLQ pressure: nominal, 0 messages.
- Source remediation plan: 760 candidates covering 2,229,714 admitted events.

## Why This Is Not Terminal

The queued target asks for a multi-source fs+git+shell+browser reconstruction
through the shared context recall view. This packet has:

- git: yes, via raw baseline.
- shell: yes, via raw Atuin and Sinex `shell.atuin` context.
- fs: yes, via Sinex `fs-watcher` context.
- browser: no, absent from the selected work window.

Browser-history coverage exists in the store, but the current evidence says it
is not ready for this terminal demo:

- `sources coverage` shows `browser.history` material spanning
  `2026-07-01 15:25:06Z .. 2026-07-02 06:49:34Z`.
- `sources remediation-plan --source browser.history` reports 14 high-severity
  candidates accounting for 1,602,919 admitted events.
- Broad generic browser-history event queries were too slow to use as a
  crisp recall baseline during this audit.

## Next Slice

Do not claim Recall v2 terminal proof yet. Choose one:

1. Find a real work window with browser+fs+git+shell evidence and regenerate the
   packet.
2. Repair browser-history freshness/query ergonomics so browser can participate
   in the current Recall v2 context view truthfully.
3. Explicitly revise the terminal target if browser is no longer a requirement.

Current best next implementation slice: browser-history participation repair,
starting with a bounded source/query audit that explains why browser coverage
exists but does not appear in a suitable context-recall window.
