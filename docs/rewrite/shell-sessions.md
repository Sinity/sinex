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
it does not append a second settled live answer. With the session reconciler configured, current raw session events are
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
loads a pinned canonical frontier in 16-row turns into temporary PostgreSQL
tables, sorts recorded-span pages and reaches true grouping boundaries. It emits
only groupings starting in the key's day. A backward crossing is compared with
the stored owner's complete bounds and parents. A changed forward crossing or
removed old crossing dirties the affected neighbor/owner; equal groupings do
not dirty each other forever. The selected implementation scans the complete
retained canonical frontier per dirty day. Targeted day-input efficiency is
unqualified.

Publication commits at most two complete outputs per turn, then archives old
outputs in 16-row turns. The dirty row stays until every replacement step has
committed against its basis. Newer dirty positions survive. Restart discards
only temporary work and recomputes the durable dirty key. Ordinary controls and
source admission run between turns; shutdown stops background work. Each
statement has a two-second deadline and each ordinary computation turn a
three-second deadline. A failure retains the dirty key and publishes its
unavailable status. Transient statement/turn deadlines retry after 250ms through
the same writer scheduler, which wakes even without new source input; permanent
representation or validation failures remain unavailable. A successful empty-ledger
observation clears a recovered transient failure, including an uncertain final
commit, without requiring new source input. The explicit qualification rendezvous keeps its existing
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
coverage with the source's unavailable state. Recall and indexed search share
source polling evidence: every input needs a completed latest observation with
zero record errors. Known zsh append-chain errors remain incomplete after later
clean or empty polls; `source_status` exposes both `record_errors` and
`retained_record_errors`, with the input identity and latest observation time.
Raw Atuin event reads retain their admitted-event availability semantics and
report failed records separately. Successful polling never certifies continuous
capture.

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

An explicit raw `kind = activity.session.boundary` or `source = session` query
returns `partial` when the reconciler is disabled; an unavailable capability
never becomes a complete empty session answer. Broad raw `events` reads list
admitted interpretation rows and disclose session enablement/pending metadata.
Included configured derived families must be settled for `complete: true`;
retained `show`/`trace` remains available independently of enablement. Current
session eligibility checks both canonical parents and retained grandparents.
Ordinary recall briefly waits up to the indexed owner's existing 500ms settling
window when the indexed projection has not reached the current commit position,
within the request deadline. Pending session or fusion work refuses immediately.
Once the retained assembly is available, source polling completeness is evaluated
without waiting for an in-progress observation: recall returns the retained answer
with `complete: false` while any latest observation is `reading`. This can recur
on unchanged input because each poll registers its observation before checking
the provider. A later completed poll can restore polling completeness; it does
not establish continuous capture.

Dirty rows retain an inclusive `through_day` bound. Admission/archive invalidation,
upgrade seeding and changed old/new output spans extend this bound. Completing a
key enqueues at most its next day, so empty intermediate days cannot hide a
multi-day ownership change. Each advancement commits with replacement progress;
newer dirty positions and enlarged bounds survive completion. Status/raw-session
metadata exposes the in-flight and pending through-day bounds. There is no
materialized date-range queue or second work ledger. The native controls add a
late day1-to-day5 span over a day3 grouping, with empty day2/day4, and its inverse
removal; settled answers match the independent full evaluation in both cases.

For a stopped failed synthetic session run, pass `--session-resume REPORT` to
that same operation with its predecessor pin. The harness verifies the retained
runtime hashes, the exact original source tree and a clean harness-only source
delta before opening the preserved owned cluster. It saves database dumps before
resuming owed work, then qualifies the remaining empty-ledger deadline, compatible
restart and provider-loss reads through ordinary CLI/MCP. It performs no build,
source reset or schema apply; original failures and passed assertions retain their
original scope. The owned state stays available after this continuation.

`recall_coverage_acceptance -- --recall-coverage-before-pin FILE` exercises the
narrow polling/error boundary through packaged CLI and MCP. It reproduces both
pre-fix false completeness cases using the exact immutable reviewed predecessor,
then checks latest Atuin errors, clean recovery, malformed zsh append and a later
error-free poll/restart whose retained chain still contains an omitted record.
A held projection step also checks the bounded settling window and disabled
recall remains a partial refusal. Session timeout retry is classified by the
typed deadline or SQLSTATE; validation failures never acquire retry authority
from their diagnostic text. This reuses the separate session lifecycle evidence; it does not rerun that
journey or claim continuous coverage.
