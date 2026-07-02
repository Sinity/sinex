# Demo: collapsing the "what was I doing?" recall silo into a general family-aware lens

**Date:** 2026-06-28
**Branch:** `feature/dev/automaton-confirmed-delivery`
**Surface:** `sinexctl events context` (crate/sinexctl/src/commands/context.rs)
**Data:** the live dev event store, dogfooding Sinex on my own work — `shell.atuin` (30K real shell commands) plus sinex self-telemetry, on the checkout-local dev DB.

## What the silo was

`sinexctl events context` is documented "Build a session-resumption context pack from recent activity" — the operator's "what was I doing around T" lens. Its implementation had two problems:

1. **Default path = a flat recency fetch.** It fetched the last N events (`limit`, default 200) globally, deduped to one card per source, and listed sources. With sinex's own self-observation telemetry firehosing (`sinexd.event_engine` batch stats, `sinexd.automaton` latency snapshots, `sinex` derived rollups), the most-recent N events are *almost entirely self-observation*. The operator's actual work never appears.
2. **The rich rendering was a desktop silo.** ~680 lines of `DesktopContextView` / `DesktopNotificationPressureView` / `DesktopFocusSessionListView` / `DesktopProjectContextListView`, dispatched by hardcoded `match card.source.raw` / `match card.event_type` arms. Only desktop-family sources got a real projection; terminal/shell/git/fs got nothing.

## BEFORE — the silo's actual output on real data

`sinexctl events context -s 24h --limit 500` (default path):

```
Context (last 24h): 3 sources
────────────────────────────────────────────────────────────
  sinex                4s ago  derived.events_processed.run
  sinexd.automaton     4s ago  latency_snapshot with 8 payload field(s)
  sinexd.event_engine  5s ago  batch.stats with 15 payload field(s)
────────────────────────────────────────────────────────────
  500 events across 3 sources in last 24h
```

Asked "what was I doing in the last 24h?", the lens answers with **sinex watching itself** — three self-telemetry sources. My 30,000 real shell commands are completely invisible, even though they are present and queryable in the very same store and window:

```
$ sinexctl events query -s 24h --source shell.atuin --limit 5
  ... "event_type": "command.executed", "command_string": "just chisel" ...   (present!)
```

`sinexctl events context -s 24h --desktop` (the specialized contract view):

```
Desktop context (last 24h)
input family        state      refs  caveats
────────────────────────────────────────────
desktop             missing       1        1
terminal            missing       1        1
browser             missing       1        1
notification        missing       1        1
```

Every family "missing" — the desktop view only models desktop-family inputs, and "terminal" here means desktop terminal *focus*, not my shell history. So both modes fail to reconstruct the session.

## The fix — a general, family-aware session-resumption pack

The default path is rebuilt as a thin lens over the existing query algebra, with **no per-source special cases**:

1. **Coverage pass** — a `CountBy(source)` aggregation over the *whole* window (not a recency fetch), so a high-frequency family can never crowd out the rest. Sources bucket into **families** (`source_family`: the namespace before the first `.`) and partition into **activity** vs **self-observation** (`is_self_observation`: sinex's own `sinex`/`sinexd` namespaces).
2. **Detail pass** — recent event cards fetched for the activity sources *only*, so representative work surfaces regardless of self-observation volume.
3. **Render** — activity families first (count, recency, sample events); self-observation reported on its own de-emphasized line; coverage stated honestly (`N total events in window (X activity, Y self-observation)`).

Desktop becomes one family among many (it shows up when desktop data exists); `--desktop` remains as a separate specialized contract view, no longer the only way to get a useful answer.

## AFTER — the replacement's output on the same data

`sinexctl events context -s 24h --limit 500` (rebuilt, same gateway, same store):

```
Session context (last 24h): 66 activity events across 1 family
────────────────────────────────────────────────────────────────
  shell            66 ev  latest 2h34m ago
      · command.executed with 11 payload field(s)
────────────────────────────────────────────────────────────────
  + 48235 self-observation events (sinex, sinexd) — excluded from activity above
  48301 total events in window (66 activity, 48235 self-observation)
```

Now the lens answers the actual question: **my shell activity (66 commands I actually ran in the last 24h) is surfaced**, and the **48,235 self-observation events — 99.86% of the window — are counted and set aside** instead of being the entire answer. The same `--format json` output is a schema-versioned `ViewEnvelope` (`sinexctl.context`, `query_echo.mode = "session_resumption"`) with `payload.activity[]` (per-family: count, sources, latest, deduped samples) and a separate `payload.self_observation[]`:

```json
{
  "source_surface": "sinexctl.context",
  "query_echo": { "mode": "session_resumption", "since": "24h" },
  "payload": {
    "activity": [
      { "family": "shell", "event_count": 66, "sources": ["shell.atuin"],
        "latest": "2026-06-28T18:50:46Z", "samples": ["command.executed with 11 payload field(s)"] }
    ],
    "self_observation": [ { "family": "sinex", ... }, { "family": "sinexd", ... } ]
  }
}
```

Note the `latest` timestamp (18:50, ~2.5h before the run) and the count of 66 (not 30,000): the window filters by **`ts_orig`** — real-world command time — so "what was I doing in the last 24h" means commands I actually ran in the last 24h, not the 30K historical commands ingested moments ago. The provenance model's clock separation makes the lens correct for free.

### Enrichment 1 — real payload, generically

Samples now render the event's `payload_preview` (a general `EventCardView` field), not a generic "N payload field(s)" — no per-source knowledge:

```
  shell            66 ev  latest 2h48m ago
      · command.executed: atuin_history_id=019f0f91e1b9… a…
      ...
```

### Enrichment 2 — "what was I doing around T" (`--at`)

The goal asks for recall *around a point in time*, not just "recent". `--at` anchors a ±`--since` window on an instant (RFC3339, or a relative "ago" duration), so a prior session can be reconstructed after a context reset:

```
$ sinexctl events context --at 3h -s 2h
Session context around 2026-06-28T18:39:28Z (±2h): 34 activity events across 1 family
────────────────────────────────────────────────────────────────
  shell            34 ev  latest 0s ago
      · command.executed: atuin_history_id=019f0f91e1b9… a…
────────────────────────────────────────────────────────────────
  + 2294 self-observation events (sinex, sinexd) — excluded from activity above
  2328 total events in window (34 activity, 2294 self-observation)
```

The JSON envelope carries `payload.anchor` and `query_echo.at`. This is the dogfood-recall primitive: a context-reset-resilient "what was I doing around T" lens, general over families, honest about coverage.

## Follow-ups (recorded, not blocking)

- Payload preview shows the first object keys alphabetically (`atuin_history_id` before `command`); a salience ranking (de-prioritize id-like keys) would surface the command text. Keep it general — no per-source field lists.
- `git-commit-history` source can't ingest (StaticFileAdapter rejects a directory) — would add a git family to recall.
- fs watcher family not yet wired into the dev manifest.

## Why this is better (equivalence + improvement)

- **Equivalent**: still a finite, schema-versioned `ViewEnvelope` ("sinexctl.context") for json/yaml; still rejects streaming formats (finite view); still a session-resumption pack.
- **Better**:
  - Actually reconstructs operator activity (shell/terminal/git/fs), which the silo structurally could not.
  - Self-observation can never drown the answer (aggregation coverage + activity-only detail fetch).
  - Family-agnostic: a new source family appears automatically, with zero new match arms.
  - Coverage honesty: self-observation volume and per-family counts are explicit, never silently mixed in.
  - Net code: removed `render_context_machine_output` + its dead-in-production tests; the recall pack is general machinery, not a desktop special case.
