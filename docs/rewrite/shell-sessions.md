# Selected retained shell sessions and recall

Enable `automata = ["session"]` alongside indexed retrieval and the configured
Atuin source. The implemented immutable profile is
`shell-session-v1-warsaw04-gap300`: Europe/Warsaw logical days from 04:00, with
a new grouping when the next command starts more than 300 seconds after the
current group's maximum recorded end. Each canonical command supplies one
recorded span; an unknown end is a point. Overlapping and nested spans extend
that same group, so a command belongs to exactly one grouping. The underlying command's outcome is
not inferred from a wrapper, shell pipeline or launcher result.

The input population is canonical commands, counted once with every exact
canonical parent. Sessions cite those canonical event IDs; ordinary `show` and
`trace` retain the path to original source coordinates. Their bounds determine
the equivalence key. A changed bound or support set supersedes the old reading;
it does not append a second settled live answer. The raw session events remain
available through `query 'events where kind = activity.session.boundary'`.

Use the ordinary configured endpoint and token:

```
sinexctl --endpoint URL --token-file FILE recall last --project tide --json
sinexctl --endpoint URL --token-file FILE query 'recall last where project = tide' --json
```

`sinex_recall` over the existing rmcp stdio entrypoint accepts optional `project`
and `timeout_ms` fields. `sinex_query` accepts the same fixed query text. The
server selects the latest session containing that configured project's commands,
returns its complete header and parents, and assembles only that project's
commands. Without project scope it returns the latest complete grouping. A
neighbor root such as `/work/tide-neighbor` does not match `/work/tide`.
Time-window, mark, general session-query and other-source recall remain unavailable.

The writer dirties logical days for admitted or archived canonical evidence,
including known span ends and adjacent keys. The existing background scheduler
loads a pinned canonical frontier in 64-row turns into temporary PostgreSQL
tables, sorts recorded-span pages and reaches true grouping boundaries. It emits
only groupings starting in the key's day. A backward crossing is compared with
the stored owner's complete bounds and parents. A changed forward crossing or
removed old crossing dirties the affected neighbor/owner; equal groupings do
not dirty each other forever. The selected implementation scans the complete
retained canonical frontier per dirty day. Targeted day-input efficiency is
unqualified.

Publication commits at most eight complete outputs per turn, then archives old
outputs in 64-row turns. The dirty row stays until every replacement step has
committed against its basis. Newer dirty positions survive. Restart discards
only temporary work and recomputes the durable dirty key. Ordinary controls and
source admission run between turns; shutdown stops background work. Each
statement has a two-second deadline and each ordinary computation turn a
three-second deadline. A failure retains the dirty key and publishes its
unavailable status. The explicit qualification rendezvous keeps its existing
120-second controller deadline instead of the ordinary turn timeout.

The current exact event representation supports at most 8,192 canonical parents
per grouping. Larger groups fail incomplete; they are never truncated into a
settled answer. Recall supports one assembly of at most 100 command rows, without
continuation; a larger requested session refuses with `partial`, directing the
caller to retained query/show/trace. This is a finite supported subset, not a
corpus-capacity claim. The regular query deadline and dropped-reader lifecycle
apply to recall. Pending fusion, session or projection work refuses a settled
recall answer; `status` exposes session dirty count, basis, day, phase and failure.

Missing focus and AFK sources leave continuity, active seconds, switches and
attention unknown. The gap rule is a classification of recorded spans,
not evidence of a real task boundary, continuous attention or actual execution
count. Source disappearance preserves retained answers and returns incomplete
coverage with the source's unavailable state. Successful polling never certifies
continuous capture.

Schema entry 3 extends the existing guarded upgrade owner with the registered
session payload/event type, dirty-day table and progress singleton. It seeds
existing canonical spans, with reader/select and writer-only mutation grants.
A populated exact current published stage-2 database requires the same explicit
fsynced dump before apply. Original event IDs, source material and producer
contracts are preserved. There is no reset or parallel schema framework.

`session_acceptance -- --session-predecessor-pin FILE` builds native development
entrypoints and exercises the real published stage-2 package, guarded upgrade,
independent synthetic initial sessions, a late bridge across the logical-day
boundary, later extension, a fresh reordered baseline, ordinary CLI/MCP reads,
observed pending publication, compatible restart and owned provider loss. Its
private report retains exact inputs, command exits, citations, database/retained
sizes, timings and interventions. Package publication, activation, live input,
confirmed delivery/NATS, summaries, focus/AFK sources and full M1 remain separate.
