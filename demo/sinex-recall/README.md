# sinex-recall — "what was I doing around then?"

A runnable demonstration that an agent or operator is **measurably better off with
Sinex's captured context than without it**, on real data.

Given a timestamp, `recall.sh` reconstructs what the operator was doing in that
window by querying Sinex's event store — the **terminal capture** (`shell.atuin`),
**deduplicated** and interleaved with Sinex's **derived activity structure**
(`derived.activity-window`, `derived.session-detector`), unified on real-world
occurrence time (`ts_orig`). Over the real archive this spans **14 months**
(2025-04 → 2026-06) of captured command history.

(Sinex also derives `canonical.terminal` command forms and `entity-extractor`
entities over the same events; the entity layer is currently shallow for shell —
it extracts the cwd, not file arguments — so it is omitted from this view rather
than padded in. That gap is logged in the operational report.)

## Run it

```bash
demo/sinex-recall/recall.sh '2025-06-03 16:05:00+00' 8
# or against another deployment:
SINEX_DB_URL='postgresql:///sinex_dev?host=/var/run/postgresql' demo/sinex-recall/recall.sh '<ts>' 10
```

Real committed output: [`sample-output.txt`](sample-output.txt).

## The before / after (why this proves Sinex's value)

**Question an agent or operator actually faces:** *"What was I doing around
2025-06-03 16:05?"* (debugging a regression, writing a postmortem, resuming a
half-finished task months later).

### WITHOUT Sinex — the baseline
The answer lives in **separate, non-interoperable stores**, and most of it is not
available to an agent at all:
- Raw shell history is in **atuin's local SQLite** on the operator's machine —
  an agent helping over chat/API has *no access* to it.
- Even the operator, with `atuin search`, gets **only raw commands + timestamps**:
  no canonical form, no grouping into work sessions, no extraction of the files
  and entities touched, no notion of "this was one activity window."
- ActivityWatch (window focus), journald (system), browser history each live in
  yet another tool with its own clock and format; aligning them is manual.
- There is **no single artifact** that says "here is what happened, in order."

### WITH Sinex — one command
`recall.sh '2025-06-03 16:05'` returns a single occurrence-time-ordered
reconstruction: the deduplicated commands, the directories they ran in, the
entities Sinex extracted, and the activity-window/session boundaries it derived —
all from one query against the captured archive. An agent can run exactly this to
answer a question it otherwise could not.

> Honest scope: this demonstration uses **one** real source (terminal) plus
> Sinex's derivations over it — per the goal's "take one real source already
> flowing." The same query unifies additional sources (window focus, system,
> browser) wherever they overlap in time; in this archive those are dense only in
> a later window, so the headline demo is terminal + derived, which is where the
> 14-month depth and the richest enrichment live.

## How it works
- Single indexed range scan on `core.events` via `ix_events_ts_orig`
  (`ts_orig` is indexed), so the reconstruction is fast even over 72 M events.
- `shell.atuin` events are stored duplicated 5–7× (an ingestion artifact, noted
  in the operational report); the query **deduplicates on `atuin_history_id`** so
  each real command appears once.
- Derived rows (`derived.activity-window`, `derived.session-detector`) are
  interleaved as context — these are produced by Sinex's automata and have **no
  equivalent in raw atuin**.

## Reproduce / verify the claim
1. Run `recall.sh` for the timestamp in `sample-output.txt`; confirm it matches.
2. Compare with raw atuin for the same window (`atuin search --after/--before`) —
  note the absence of canonical/entity/session enrichment and the cross-store gap.
3. The data is real and queryable directly:
   `sudo -u postgres psql -d sinex_prod -c "select count(distinct payload->>'atuin_history_id') from core.events where source='shell.atuin' and ts_orig between '2025-06-03 15:57' and '2025-06-03 16:13'"`.
