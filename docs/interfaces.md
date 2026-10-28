# Interfaces

Every way to reach Sinex: the operation table and its transports, callers and their
powers, the `sinexctl` CLI, MCP, status, signals and stall alerts, `follow`, and the
latency budgets. The query catalog and its behaviour are in [query](query.md); recall,
the brief and agent arrival in [recall](recall.md); Today and the reader in
[web](web.md). The shared mechanics (the `Operation` trait, request and answer
envelopes, the failure-to-exit-and-HTTP table, latency classes and deadlines, SSE
framing, the status figure format) are the common
[`command-surface`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/command-surface/README.md)
crate's; the signal rule schema and evaluator are common
[signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/signals.md).

## Principles

- **One question, one answer, every surface.** One parsed question, one resolved read
  context and one server-side answer serve the CLI, MCP, HTTP, `follow` and the
  reader. Page size, presentation budget and deadline never change what was selected.
- **One entry point for questions.** The query is the entry point; a new capability
  arrives as a table, an alias, a stage or a verb on rows, never as a top-level
  command that restates a query. Ruling, 2026-05-25: "seriously, randomly introduce
  new top-level command for that?"
  (cc-queued:c2404522-7bba-4de3-936d-e08deb14262c@2026-05-25T00:40Z).
- **Agents are primary users** and get no fewer powers than the operator; what
  distinguishes the operator is authorship, which the daemon computes. Ruling,
  2026-06-01: "design stuff in sinex with mind towards agents as primary usres … Critical,
  then, to design the system to be fundamentall legible" (journal 2026-06-01 06:54).
- **Legible and honest.** Every answer says what was observed, what was derived, what is
  unknown and what was left out of the display. A quiet source is not a failed one; a
  running process is not evidence of capture; a command with no recorded end is "end
  not recorded", never "running"; proximity is not provenance.
- **Captured text is data.** Every result holding captured text carries the data
  notice, delimits payload text per row and escapes control sequences. What is masked,
  hashed or withheld is decided only by the operator's configured rules, applied
  identically on every output ([output parity](privacy.md#output-parity)).
- **Nothing unasked without a rule.** No streaks, scores, gamification or unsolicited
  behavioural advice; anything Sinex surfaces unasked comes from a declared signal rule.

## Operation table

- **One typed operation table** declares every command once. The CLI parser, the socket
  and HTTP routes, the MCP tools (with `outputSchema` generated from the answer types),
  help, completion and the shipped agent skill are generated from it. A command missing
  from the table fails closed.
- **Every feature reachable through MCP is in the CLI**, and the reverse, unless the
  table records a reason. Ruling, 2026-07-29: "probably should be in CLI, unless clear
  reason not to, but CLI itself is supposed to be disciplined"
  (cc-queued:d6f4b0c3-840b-47b6-bd57-5f1bd6c9c7d4@2026-07-29T01:12Z).
- **The write path.** Handlers hold only the read pool. Every write (stage, `record`,
  note, mark, judgment, saved query, signal rule, archive request) goes to the one
  [admission function](admission.md) as a request material plus events.
- The command list itself is generated into [reference/commands](reference/commands.md).

## Transports

- `sinexd` serves a Unix socket and an HTTP listener carrying one typed request
  protocol: `POST /v1` with a tagged request, `GET /v1/commands` for the table's
  metadata, SSE for `follow`. There are no dotted method aliases.
- The listen address and authentication mode are deployment configuration (default
  loopback with a peer-uid check; a token file for any other address) set through the
  NixOS module, the config file or environment variables
  ([configuration](operations.md#configuration)). Phone or remote access is that
  configuration, not a product feature.
- **Requests are strict.** Unknown fields are refused. A request that changes state
  (archive, restore, replay, tombstone, stage, recompute) has no default scope: an
  omitted scope is an error, never "everything".
- **Health** is its own route: up or down, without internal error text, separate from
  capture health.
- **Version.** The server reports its version; the client warns on a mismatch.
- **The daemon is required.** Ordinary commands go through `sinexd`. Only the declared
  offline diagnostics (`verify`, `status --offline`, `store verify --offline`) read the
  database directly through the reader role, and completion works from the published
  catalog file. Ruling, 2026-09-14: "Daemon is the standard, not some sort of optional
  addon" (cc:652ae14f-a036-496b-abfa-42e7574c9ec9:b8a68d64-772f-4d8c-8e95-435ce601d245).

## Failures

One `Failure { kind, message, details, position }`, mapped once to an exit code and an
HTTP status by the shared table in `command-surface`. Sinex uses the shared kinds and
adds one:

| Kind | Raised when | Exit |
|---|---|---|
| `stopped_early` | an archive, restore, replay, recompute, stage or tombstone operation did not finish | 5 (degraded); never reported as ok |

- MCP returns typed failures, never a success with empty rows, for a refusal, an
  unknown capability or a timeout.
- A refusal after the first committed batch of a lifecycle operation ends
  `stopped_early` naming what moved; `resume OPERATION` continues it
  ([lifecycle](lifecycle.md)).

## Callers

### One command set

The operator and agents hold the same command set; there are no ordered access tiers.
Ruling, 2026-07-03: "I want agents to have more power / capabilites rather than less,
more affordances" (cc-queued:30f5e650-7033-4423-9f91-2b66ebd1e6b2@2026-07-03T13:20Z).

- **Grants are per-credential configuration**, seeded with defaults by the NixOS
  module, never code constants. Which output a caller sees is decided by the output
  rules, which are the same for every caller ([privacy](privacy.md)); nothing is
  refused because the caller is an agent. Privacy rules may be written by any author;
  status lists those written by anyone but the operator as non-operator changes.
- The credential gates grants; it is not an identity.
- **An act reserved to the operator is an explicit configured rule with its reason**,
  listed in configuration and in `help`, never inferred from the token. The seeded
  reserved acts:

| Act | Rule | Reason |
|---|---|---|
| confirming a tombstone's cascade preview | any caller requests and previews; the operator confirms interactively (default; open operator question) | tombstoning is the one irreversible deletion ([tombstone](lifecycle.md#tombstone)) |
| `same`/`distinct` judgments that govern fusion | every caller records judgments; the operator's govern, and a per-rule setting admits others' | fusion judgments shape every fused record ([fusion](derivations.md#fusion)) |

### Computed authorship

- Every write records `written_by`: `author` (`operator`, `agent:<session>`,
  `agent:<origin>`, `script:<unit>`, `unattributed`) and `via` (`cli`, `mcp`, `web`,
  `hook`, `daemon`, `import`, `desktop`).
- **No request field names an author.** The client reports its harness identity (the
  variables supported harnesses already set: `CLAUDE_CODE_SESSION_ID`,
  `CODEX_THREAD_ID`, `GEMINI_CLI`, `HERMES_SESSION_ID`; for MCP, the harness
  connection), and the daemon decides:
  - the nearest harness wins;
  - `operator` only through the operator's own channels with no harness identity
    (an interactive terminal without harness variables is the operator);
  - `script:<unit>` from the calling process's cgroup unit;
  - otherwise `unattributed`.
- No job runner is recognised by name. A launcher that forwards its caller's harness
  variables keeps the caller's session.
- `events where written_by.session = S` lists everything one agent session wrote.

### Agent writes are visible and undoable

- Status lists the last week's agent writes by session and kind.
- **`undo --session S [--dry-run]`** reverts every write one session made, listing it
  first and recording each reversal with its own author. It archives the session's
  notes, marks, judgments, saved queries, signal rules and `record` outputs; archives
  what the session's stages admitted; and undoes the session's replays by the ordinary
  [replay undo](lifecycle.md#undo), possible at any later time. A tombstone confirmed in
  the session is listed as not revertible. A second undo restores what the first
  archived.

### Derived conclusions: `record`

`record KIND PAYLOAD --parent REF…` (MCP `sinex_record`) writes an agent's derived
conclusion: a registered kind, a payload matching its schema, at least one parent
citation, the computed author, and the producer `agent:<session>` with its model as
producer version. It passes ordinary admission and is withdrawn by its author's archive
act or by session undo. Programs that derive continuously use the ingress as
[outside handlers](producers.md#outside-consumers-and-handlers), not `record`.

### Agent guidance shipped with Sinex

- Sinex ships a skill text **generated from the operation table**: what Sinex holds, the
  core operations, example queries with their `read_as`, the data notice, and the rule
  "cite event IDs". Sinex's NixOS module installs it into each harness's skill
  directory; `sinexctl skill print` prints it for any other installation. Ruling,
  2026-10-18: "CLAUDE/AGENTS/skills -- yes"
  (cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:12f1a067-cc5b-41b7-9209-297099d6b0c1).
- The MCP `instructions` field carries its summary; each tool description is short and
  says when to use the tool.
- Context reaches agents without their asking, through Polylogue's session-start brief
  ([agent arrival](recall.md#agent-arrival)).

## CLI

`sinexctl` is product UI, not developer tooling. Ruling, 2026-02-12: "sinexctl isn't DX
tho...? I mean, it's part of the system, its UI"
(claude.ai:7abe2ed3-2651-4e23-a23a-48a61c82bb85:019c5366-a07e-7623-9477-9d8a857b0aa3).

### Grammar

- **One grammar rule, shared with Polylogue**
  ([Polylogue's grammar](https://github.com/Sinity/polylogue/blob/master/docs/interfaces.md#grammar)):
  the first argument is a command name, otherwise the start of a query, which needs a
  signal (a field, a table, a reference, `find`, or one argument holding whitespace or
  query punctuation). An unsignalled first word is refused within the local budget
  with the nearest command and the `find WORDS` form.
- **Verbs act on rows after a query:** `QUERY read`, `QUERY trace`, `QUERY follow`,
  `QUERY keep`, `QUERY save NAME`, `QUERY archive`, `QUERY replay`; `REF read`,
  `REF trace`, `REF related`.
- **There is no `query`, `search` or `timeline` command.** Bare words in a signalled
  query already search, and a timeline is the default rendering of an event query
  sorted by time. `recall`, `context` and `brief` are commands because each assembles
  several queries into one answer ([recall](recall.md)).
- **Groups, not hyphens:** `library add|item|tag|untag|set|collect|uncollect|collection|rate|compare|rank|open|viewed|preview|import`,
  `privacy rules|rule|class|verify`, `forecast issue|resolve|report`,
  `analysis catalog|run|panel|why`, `signal add|list|enable|disable|remove`,
  `sources [NAME]`, `kinds [KIND]`, `store verify`. MCP tool names join the words with
  `_` (`sinex_library_add`).
- **Export is `read --to`.** `QUERY read --to DIR` writes the bundle atomically with its
  manifest: the question, its resolved read context, the selected records, references to
  the material bytes they came from, what was left out, and the replay recipe.
  `--to FILE` writes one document. There is no `export` verb. Results produced outside
  from a bundle come back as attributed derived events through `record` or the ingress,
  never as edits.
- **No aliases.** Each name is fixed once; [vocabulary](vocabulary.md) holds every
  command word and flag.

### Alignment with Polylogue's CLI

Each product's concepts are its own; the CLI mechanics are shared through
`command-surface` and `query-language`:

- the grammar rule above, with Sinex's own catalog; `|` between stages and the rest of
  the query-language spelling;
- the exit table, the request envelope with deadline classes and lifting, the answer
  envelope and SSE framing;
- the grammar-position completer, a catalog file published by the daemon, and shell
  registration scripts;
- global flags with the same names and meanings: `--json`, `--jsonl`, `--ids`,
  `--page-size N`, `--cursor C`, `--fields`, `--why`, `--deadline MS`; Sinex's own
  target flag is `--endpoint`;
- space-separated command groups, and MCP names joined with `_`;
- the same words for the same mechanical acts: `read` (one thing rendered), `follow`
  (a stream of new rows), `trace`, `changes --since POS`, `explain`, `sql`, `status`,
  `recall`, `brief`, `keep`, `save`, `verify`, `mcp`, `completions`, `setting`, and
  `read --to` for writing out. A vocabulary check compares both products' tables.

Not shared, because they are Sinex's own concepts: the lifecycle verbs (archive,
restore, replay, tombstone, stage), library curation, privacy rules, forecasts and
briefs. Where both products have a brief or a recall, they are different things.

### Lifecycle and staging verbs

The semantics are [lifecycle](lifecycle.md)'s and [staging](materials.md#staging)'s;
the surfaces are:

- `stage PATH | DIR | DB [--parser NAME] [--export-date DATE] [--account NAME]`, ending
  with an account by event type (admitted, suppressed with example references, failed)
  and `stopped_early` when it did not finish; `stage list` shows progress and what paces
  it, and gives no estimate while the rate varies;
- `archive REF | QUERY | --source S --before T` and `restore REF | QUERY`, each printing
  the cascade before it runs and refusing when any part cannot move;
- `replay QUERY [--compare]` and its undo; `recompute DERIVATION [--compare]` and
  `recompute DERIVATION --switch` ([versions](derivations.md#versions));
- `tombstone REF | QUERY | --material ID`, only on archived chains, with the cascade
  preview and the operator's confirmation ([one command set](#one-command-set));
- `backup hold` and `backup release HOLD`, called by the deployment's backup tool
  around a copy ([backup](operations.md#backup-and-restore));
- each records an operation with its computed author, and the preview is shown on
  every surface before a state change runs.

Archive and restore take something out of view and bring it back; tombstone is the
one irreversible deletion; a refused material is read again by replay.

### Output

- Typed printers per operation, from its answer type. Modes: a table (default on a
  terminal), `--json` (one complete document), `--jsonl` (rows streamed after a
  documented metadata frame), `--ids`.
- Colour only on a terminal with `NO_COLOR` unset. Colour and weight encode status only
  (ok, stale, missing, provisional, error), never source identity. Provisional rows look
  provisional; unknown is printed "unknown", never blank. Widths count display columns.
  Local times carry their offset when ambiguous. Captured control characters are
  escaped.
- Long output on a terminal streams into `$PAGER`, fetching later pages only as the
  pager reads.
- **Errors** are one line of what failed and what to do, positioned in the query, with a
  runnable correction when one exists; with `--json`, an error object on stderr. An
  empty result says why: nothing matched, the source was not observed, or the rows are
  outside the caller's grants.
- `--deadline` has no upper bound; `--deadline none` lifts it.

### Completion, picker and references

Completion and fuzzy finding are part of the product. Ruling, 2026-10-27: "advacned
autocomplete, fuzzyfingind where appropiate"
(cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:dad489d4-a1b8-48bd-bbc0-26262f34b262).

- **Completion runs in the CLI process from a catalog file `sinexd` publishes**: tables,
  aliases, fields per event type from the kind registry, sources, hosts, recent event
  types, saved queries. The daemon rewrites it atomically after committed batches. It
  works with the daemon down and never waits on it. The grammar-position completer is
  the shared `query-language` function; MCP `completion/complete`, HTTP and the reader's
  query box call the same function.
- `completions SHELL` prints registration scripts for bash, zsh and fish.
- **One embedded picker** (`nucleo` matching) opens only when stdin and stdout are
  terminals and no machine format or harness is present. Otherwise an ambiguous
  reference refuses (exit 2) with the candidates and the exact non-interactive call.
  `pick` prints chosen references for shell widgets.
- **References:** full IDs; unique prefixes of at least 8 characters; `@N`, `@N.M`,
  `@N-M`, `@*` from the previous list in this terminal (a row file holding the list's
  query, read context and next cursor, so `next` continues it); `last`; the
  [citation forms](query.md#citation-forms). Over MCP, `@N` names the agent session's
  previous result. `@` rewriting applies only to reference and query arguments.
- There is no REPL and no full-screen terminal interface: the shell with completion,
  `@N`, the pager and the picker is the interactive loop, and the [reader](web.md) is
  the exploration surface.

### Help and orientation

- Help is generated from the table; no command is hidden; each entry says what it does
  and one usable next step, and its examples run as tests.
- `explain QUERY` prints the query read back as a sentence (`read_as`), its field
  phrases and the resolved read context.
- `kinds [KIND]` lists event types with counts, schemas, time quality and the aliases
  that expand to them; `kinds export DIR` writes JSON Schemas for outside producers.
- `setting` prints the effective configuration and where each value came from.
- **Bare `sinexctl` renders [Today](web.md#today)**; `status` stays explicit.

## MCP

- `sinexctl mcp` (stdio, on `rmcp`) advertises every operation the table marks for MCP,
  with strict argument schemas and `outputSchema`. The tool list is generated, so it has
  no fixed size.
- Query, recall, context, read, trace, related, brief, sources, kinds, status, `record`,
  notes, marks, judgments, saved queries, signal rules and the lifecycle requests are
  available; an agent needs no CLI fallback for anything the table marks for MCP.
- `follow` is available as a resumable subscription with the same positions as SSE.
- Output rules, the data notice and per-row delimiting apply exactly as on HTTP and the
  CLI. `@N` and `since last_seen` are per agent session; one client's state never
  affects another's.
- A query that would exceed MCP's deadline is refused with its estimate and the CLI
  form, never answered differently.
- MCP never returns raw material bytes.

## Status

### The status view

One status view covers:

- the daemon, writer, bus and ingress backlog, and database;
- per source, its run state and coverage;
- per derivation, its position, lag, pending version switch and compare result, and last
  failure ([versions](derivations.md#versions));
- fusion's ambiguity counts;
- replays, stages and recomputes in progress;
- **refusal figures**: the refusal rate per source and kind, and kinds whose every
  material is refused;
- failures; archive size and growth; disk free and days until full (warning below 14
  days);
- the last week's agent writes by session;
- outside consumers' positions and lag ([outside handlers](producers.md#outside-consumers-and-handlers)).

Every figure uses the shared figure state and verdict of `command-surface`. Sinex's
domain fields keep their own values beside it, each with a stated mapping to the verdict:

| Field | Values |
|---|---|
| `last_run` | `new`, `repeats`, `nothing`, `not_run` |
| gap state | `gap`, `unknown`, `estimated` |
| coverage cell | `observed`, `partial`, `recorded_unknown_coverage`, `missing` (with reason) |

A run that found only repeats is healthy; a staged export is never stale; a missing cell
carries its reason. Status reads self-observation events, so nothing is recounted on the
request path. There are no scheduled verify, scrub or check runs, because invariants
hold by construction: `store verify` is on demand, reports mismatches and repairs
nothing. `status --offline` reads positions, the last commit and the writer lock's
holder through the reader role while the daemon is down.

### The sources relation

- `sources` is the one source status relation and `sources NAME` the one detail view.
  Gaps, inputs and payload shapes are the query tables `gaps`, `inputs` and `shapes`
  ([tables](query.md#tables)), read and paged like any query, so there are no separate
  gap, continuity, drift or readiness commands.
- Fields: `source`, `binding`, `driver`, `version`, `event_types`, `purpose`,
  `exclusion` and its reason, `last_run`, `quiet` (against the declared `expect` cadence
  and the operator's active time), `stale_after`, `coverage`, `behind`, `rate`,
  `refused_rate`, `shape_changed`, `days_with_data_30d`, `gaps_30d`, `last_success`,
  `last_error`.
- **One source-evidence path** classifies sources for fill, panels, recall, status and
  alerts: durable poll outcomes (success, unchanged, failure), cursor positions, gap
  records with their missing count, record failures and shape observations, read under
  the answer's read context. Never source-name prefixes, process liveness, run lifetime
  alone, or earliest and latest event alone.
- A never-read binding is never healthy; an unchanged successful poll is never a stall.
- `sources NAME` gives per-input accounting: records read, events made, suppressed,
  matched to another input, record errors with the first unparsed form. An input whose
  records did not become events shows as such instead of reading as complete.

### The status bar

`status --bar` emits one JSON line on change, for a desktop bar or dashboard to
consume:

- the **privacy stripe**, coloured by the most invasive capture running now: green for
  files only, amber for screen, red for keyboard and screen; audio and accessibility
  count with screen;
- the count of quiet sources;
- passive signals not yet seen;
- per binding, `producing_now` (an event admitted in the last 5 s).

### Polylogue on Sinex

When Polylogue runs on Sinex, status shows both sides: what Polylogue has sent and Sinex
committed, the confirmed-stream position Polylogue has applied, and its lag. While Sinex
is unreachable, pending counts are `not_measured` and `up_to_date: false` is shown,
never success ([Polylogue primary mode](https://github.com/Sinity/polylogue/blob/master/docs/primary-mode.md)).

## Signals

A **signal** is a standing query or standing analysis with a declared delivery rule. It
is the only way Sinex surfaces anything unasked. The rule schema, the evaluator and its
laws, the interrupt budget and desktop notifications are common
[signals](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/signals.md);
its conformance cases run in Sinex. What Sinex supplies:

- a rule's `query` may be a standing query or `analysis:<recipe>`;
- `value` targets are a file or a loopback URL, and `action` names a command already in
  configuration;
- occurrences are keyed by citation (record coordinates, source record key or derived
  key, never the reading), so replay and re-import never fire a rule again.

### Delivery

- **Declared, never inferred.** A rule is written explicitly by the operator or an agent
  and runs as written. Status lists agent rules with their authors; `signal disable`
  withdraws one.
- **Passive by default:** a delivery appears in the next brief, Today, the status bar
  and the reader's Signals view.
- **Interrupting** only for rules marked so, under the one interrupt budget. Sinex is
  the one deliverer on a host where Polylogue runs on it: Polylogue's interrupting
  notices arrive with its other deliveries and are shown under the same budget and
  quiet hours, which are Sinex settings there.
- **Three deliveries, one mechanism:** a notice, a value, an action. An action names
  only a command already in configuration, runs outside `sinexd` with the triggering
  citation as its last argument, and its result returns as ordinary events.
- **Desktop delivery** is the common freedesktop notification design, outside the
  writer, with the optional configured command as a second route; a slow notification
  server never holds the writer.
- **Every delivery is a self-observation event** naming the rule revision and its
  triggering rows, so "why did this fire" is a query.

### Shipped rules

| Condition | Ships |
|---|---|
| the daemon stalled | enabled, interrupting |
| Sinex blocked (admission cannot accept producer input) | enabled, interrupting |
| a source unreadable | enabled, interrupting |
| capture stalled: `source-quiet` ([stall alerts](#stall-alerts)) | enabled, interrupting |
| a kind whose every material is refused | enabled, interrupting |
| the resume digest: operator activity returns after a gap, and the [resume card](recall.md#recall-and-context) is delivered as a notice | disabled; passive when enabled |

Everything else is passive. Tripwires, prospective memory and the hyperfocus guard are
rules the operator or an agent declares; none is enabled by shipping an example.
Elicitation prompts are deliveries of declared rules ([elicitation](higher-layer.md)).

No rule starts or resumes an agent session (default; open operator question): there is
no agent-starting delivery, and the deployment's action commands include none that
start or resume an agent, because a run would spend quota with no one acting.

### Stall alerts

- `source-quiet` fires from the binding's declared `expect` and `stale_after` and its
  durable poll outcomes. It tells apart an unchanged successful poll, an event-driven
  source with no recent events, an unavailable input, a parser rejection, an excluded
  source, source lag and an actual missed expected read. It never fires from low counts
  alone.
- An alert carries its evidence, the last success, the affected interval and a concrete
  next action, and stays visible in status and the brief until recovery clears it.

## Follow

- `QUERY follow` (SSE over HTTP, a subscription over MCP) is a signal without a delivery
  rule: the same standing evaluator delivered to the open connection, `on_new` by
  default, `--for` and `--what` as in rules. Nothing is recorded.
- Each message carries its commit or stream position as the SSE `id`. A reconnect with
  `Last-Event-ID` receives everything after it, from the confirmed stream or
  PostgreSQL, including lifecycle changes (archive, restore, replay, tombstone) that
  affect matching rows. A position whose history is unavailable refuses with a named
  gap; it never silently starts now.
- Messages report withheld payloads as withheld; keepalives are not capture evidence.
  Grouped, aggregate and ranked subscriptions use `on_change` semantics.
- Programs that act on every committed fact subscribe to the confirmed stream as
  [outside handlers](producers.md#outside-consumers-and-handlers).

## Latency budgets

Budgets are p95, end to end through the real binary with a warm daemon. Each operation
belongs to one class of the shared `command-surface` classes; Sinex's budgets per class:

| Class | Sinex operations | Budget |
|---|---|---|
| local | help, version, grammar completion, refusals of malformed input | ≤ 15 ms |
| status | `status`, the first screen of bare `sinexctl`, `status --bar` | ≤ 100 ms |
| completion | each keystroke, daemon up or down | ≤ 50 ms |
| read | `REF read`, indexed lists | ≤ 100 ms to the first screen |
| search | bare-word queries, `recall` | ≤ 250 ms to the first page |
| aggregate | grouped and joined queries | ≤ 400 ms by default; a timeout names a narrower query; `--deadline none` lifts it |

Journey budgets on top:

- the morning view (bare `sinexctl`, Today) renders in under 1 s;
- resuming work takes at most two commands or interactions to reach evidence;
- `recall` answers in under 2 s on a year of data;
- `mark` commits in under 1 s;
- the machine section answers within Polylogue's 100 ms deadline;
- confirmed delivery to `follow` and outside handlers arrives under 5 s after a row is
  queryable;
- a stall alert arrives within the binding's `stale_after` plus measured evaluation and
  delivery delay.

A miss is recorded with its measurement, never met by relaxing the number. Measurement
is paired benchmark runs with hardware-independent figures (rows scanned, bytes encoded,
memory per page); no test asserts elapsed time ([benchmarks](testing.md#benchmarks)).

## Laws

- **surface-parity**: the same synthetic questions through the CLI, HTTP, MCP and the
  reader give identical rows, totals, units, `read_as`, output-rule effects and failures.
- **generated-table**: CLI help, the MCP tool list with `outputSchema`, and
  `/v1/commands` equal what the operation table declares; a command missing from the
  table fails closed.
- **mcp-cli-reach**: every operation marked for MCP has a CLI form, and every CLI
  operation without a recorded reason is marked for MCP.
- **typed-failures**: each failure kind maps to its exit code and HTTP status; a
  stopped-early archive, restore, replay, stage, recompute or tombstone exits 5; MCP
  returns typed failures, never an empty success.
- **state-change-needs-scope**: a state-changing request with no scope is refused, and an
  unknown request field is refused.
- **one-command-set**: an agent credential can call every command; a reserved act
  refuses with its configured reason; no refusal depends on the credential kind except
  through configured grants.
- **computed-authorship**: no request field sets an author; harness variables and the
  channel decide it; two agent sessions writing produce two authors.
- **session-undo**: `undo --session S` archives every write of S, undoes its replays by
  archive and restore, records each reversal, and a second undo restores them.
- **no-subset-commands**: every operation-table row records what a query plus an
  existing verb cannot express; a row that restates a query or does a subset of another
  command fails the vocabulary check.
- **signal-before-query**: an unsignalled first word refuses within the local budget
  with the nearest command and the `find` form.
- **shared-mechanics-names**: every mechanical act shared with Polylogue has the same
  command word and global flag names in both products' tables; the vocabulary check
  fails on a difference or on a word missing from the vocabulary.
- **completion-offline**: completion answers from the catalog file with the daemon
  stopped, within its budget, and never offers an unavailable operation.
- **picker-off-terminal**: off a terminal or with a machine format, an ambiguous
  reference exits 2 with its candidates and the exact non-interactive call.
- **help-examples-run**: every help example runs on the synthetic world and matches its
  golden.
- **source-evidence-path**: a never-read binding is never healthy, an unchanged
  successful poll is never a stall, and a failed read inside a live run is not covered;
  status, fill, recall and alerts agree on each.
- **signals-conformance**: the common signals conformance cases pass against Sinex's
  catalog and occurrence keys; a threshold fires once per crossing; `for` expires
  without new events; the shared agent interrupt budget demotes the fifth agent
  interruption in an hour and never the operator's.
- **replay-never-refires**: replaying or re-importing the material behind a delivered
  occurrence produces no second delivery.
- **stall-alerts**: a reader that fails while its process lives raises one interrupting
  alert with evidence; recovery clears it; unchanged polls never alert; a restart does
  not deliver it again.
- **follow-resumes**: disconnect, admit, archive and restore rows, reconnect with
  `Last-Event-ID`: delivery is complete and ordered, including lifecycle messages; an
  unavailable position refuses with a named gap.
