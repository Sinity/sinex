# Retained Atuin source subset

This isolated foundation connects `terminal.atuin-history` to normal CLI raw-event answers. It is preparation for the first milestone, not formal M1 acceptance or a deployed service. Only synthetic inputs are qualified by its acceptance command.

Enable one source with explicit absolute paths in the normal daemon configuration:

```toml
sources = ["terminal.atuin-history"]
automata = []

[atuin]
path = "/absolute/path/synthetic-history.db"
poll_interval_ms = 30000

[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
```

Privacy patterns are Rust regular expressions with literal replacement text. They apply before storing event payloads and again on read. Source identity and row fingerprints use original unmasked fields. Retained canonical rows are explicitly plain evidence. Guarded/sealed storage and privacy administration are unavailable.

Before RPC admission at startup, the writer commits the configured input and an empty `reading` observation through the normal Atuin admission path. The first poll reuses that observation. Each subsequent poll likewise commits its reading observation before SQLite open/read. Queries therefore report incomplete coverage while the initial open/read is blocked, including after a previously completed zero-row poll and restart. With no configured or retained inputs, empty coverage remains complete. Only reaching EOF establishes completed zero; open/read errors establish unavailable.

Each poll opens SQLite read-only without the create flag and reads one consistent transaction. The initial implementation performs a full row pass every poll. It yields at most 64 rows and 1 MiB of canonical row bytes per writer step. This keeps late open-row updates visible without guessing a trailing window, but does not implement the final new/open/daily-full scheduler, transaction-time adaptation or resource acceptance. A single row over the byte budget leaves the poll unavailable, with no completed-zero claim.

The input registry identifies the source database separately from its acquired row materials. Canonical sorted-key JSON lines retain SQLite values and an `_from` observation/table/rowid reference. Rowid is acquisition evidence only; equivalence uses the registered input and Atuin native ID. `_end_not_recorded` records whether a later command in the same session proves that an unfinished row cannot close. Fingerprints exclude the acquisition observation and rowid. Later changed readings supersede their prior event; soft deletes become removed readings, preserved in history and absent from ordinary event queries. Unknown exit values stay null. Integer source timestamps and duration values retain nanoseconds; PostgreSQL's timestamp carries the microsecond portion and `ts_orig_tiebreak` carries the remaining nanoseconds.

Rows append to an owned open part and are fsynced before the writer transaction commits its events, failures, durable material length, observation accounting and cursor. Parts close into the content store at poll completion or 100 MiB. Startup truncates an interrupted part to its committed length and closes it as recovered partial. A restart retries the source snapshot from the beginning; equivalence suppression prevents duplicate live facts. An incomplete, unavailable or interrupted observation never reports completed zero. Physical SQLite page/snapshot revisions remain unavailable until slice 1c; row evidence is not a database snapshot.

Use explicit endpoints as in the main README:

```sh
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token sources --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token query 'events where source = terminal.atuin-history and kind = command.executed' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token show EVENT_UUID --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token trace EVENT_UUID --bytes --json
```

The query subset supports `events`, conjunctions, equality on `source`, `kind`, `id`, `when`, `payload.command`, `payload.exit_code`, and literal substring `~` on command text. Other syntax is refused. `search TEXT` uses that same command substring query. The later `commands` alias, full-text indexing, arbitrary payload paths and the general query language are unavailable. Queries return exact totals and 100-row live pages; pass the self-contained keyset cursor using `query QUERY --cursor CURSOR`. The continuation contract below defines current-state changes, expiry and compatibility. Source incompleteness is explicit and the CLI prints the partial answer with exit 5. `show` includes archived and source-removed readings. `trace --bytes` resolves a masked canonical row and its exact retained range.

Run declared `atuin_acceptance` or `atuin_package_acceptance` with the explicit checkout path. The owned harness retains commands, exits, source observations, binary hashes and synthetic fixture variants even on failure. Its late-update case spans several writer steps, and its empty/failed cases use separate product state. The packaged Kestrel driver uses its independently pinned oracle and explicit
bad-answer/launch reporting controls. `kestrel_boundary_acceptance` additionally
qualifies the observed retention and admission boundaries described below.
Time-budget/channel fairness, a general read-model lifecycle/index, physical
SQLite snapshots and formal task-routing/M0/M1 gates remain open.


## Observed durability boundaries

The explicit qualification socket control holds the real writer at three named
data-bearing instructions (empty reading registration, EOF and unavailable accounting do not rendezvous): `retained-fsynced` after the canonical open-part write and
`sync_all` have returned, before the admission transaction; `transaction-staged`
after events, failures, durable length, observation accounting and cursor have
been staged in that transaction, before COMMIT; and `transaction-committed`
after COMMIT returns, before part closure and the source's admission reply.
The socket frame names the process, input, observation, material/range and step
size; transaction points also include the actual staged/committed summary.
The controller observes the frame before SIGKILL, so elapsed sleeps are not
substituted for reaching the boundary. A one-shot release or kill is required;
control failure fails the admission. Normal operation enables no rendezvous.

The first two states have acknowledged no source success and expose no staged
events or data progress to readers. The separately committed reading registration
remains visible with zero acquired rows; it is not completed empty coverage.
Their fsynced but uncommitted tail is deliberately
truncated to the committed durable length on recovery; a fresh read of the
unchanged SQLite input then admits its rows. The third state has committed facts
and progress even though its source reply has not been delivered. Recovery closes
that material, preserves its event identities and exact retained coordinates,
and suppresses duplicate live facts when the source retries. Repetition may
create new acquisition observations and repeated malformed-record diagnostics;
it does not create new command facts. Tests compare actual queries, source
accounting and retained native rows/clocks to an uninterrupted baseline. This
is process-kill durability evidence, not a power-loss or physical SQLite snapshot
claim. Read-only custody audits with the actual restricted reader role additionally
verify committed lengths, recovered blob lengths and interrupted versus completed
observations; admission itself always uses the product writer. No read-model index is implemented or qualified by these controls.

## Per-input record diagnostics

The normal `sources --json` response includes `cursor` (the committed admission
summary or null) and `first_record_error` for each input's `last_run`. With no
record errors, `first_record_error` is null. Otherwise it names `observation_id`,
`material_id`, `anchor_byte`, `end`, `reason`, `representation` and the first
unparsed canonical `record`. The record is read from the retained material and
masked with its attributed presentation rules, just as `trace --bytes` is.
`evidence_status` is `available` when that retained row can be read. If its bytes
are inaccessible, the diagnostic keeps its attribution/range start/reason with
`evidence_status = unavailable`, `record = null` and `end = null`; unavailable
evidence is not fabricated or represented as zero errors. The boundary suite
exercises this failure while the actual writer is held, restoring its owned
synthetic part before the kill/restart comparison. The canonical row's `_from`
retains observation, history-table and rowid evidence; this is a logical row form,
not a SQLite page snapshot.


## Retained reads without the provider

`kestrel_provider_loss_acceptance` takes the same required specification Git
repository argument. It runs a fresh packaged candidate against synthetic state,
reusing the separate normal-path, three-boundary and reporting-control
qualification. The driver observes `transaction-committed`, kills the recorded
held child before its source reply, then moves the owned SQLite provider away
before restarting the same package with unchanged configuration and product
state. Normal `query`, `show`, `trace --bytes` and `sources` establish that
committed event identities, retained ranges and masked canonical row values
survive. The independent pinned oracle checks all malformed-fixture query facts
without changing its expectations. The query envelope remains incomplete with
CLI exit 5. Source status settles to unavailable with its read-only-open
failure reason; a query overlapping a fresh failed poll may briefly see reading
with no reason yet. Both envelopes remain incomplete. The unavailable observation
has read no rows; this does not erase earlier committed custody or claim a completed
empty source. Repeated unavailable polls must not create a replacement database.

A `transaction-staged` contrast moves the provider away after the observed
pre-COMMIT kill. Its fsynced uncommitted tail has no promised custody: recovery
truncates to zero and answers have zero retained events with explicit
incompleteness. Reader-role custody audits inspect the original observation and
recovered material in both cases. Source digests, held frames/PIDs, commands and
relative artifacts remain in the existing portable report. This qualifies reads
of retained logical rows after process death and provider disappearance. The separate [retained replay qualification](retained-replay.md) covers parser replay. Original SQLite byte reconstruction, power loss, general search/indexing
and formal M1 acceptance remain outside this test.

## Raw-event continuation

Each request reads one current repeatable-read PostgreSQL state and returns its exact matching `total` and `resolved.commit_seq`. Pages contain at most 100 rows ordered by original time at nanosecond precision ascending, unknown time last, then interpretation ID ascending. The page size is fixed in this implemented subset. A continuation resumes strictly after the last returned key. Removals apply to later requests; additions before the key may be absent and additions after it may appear. A row whose key moves may be absent or repeated. There is no transaction or fixed snapshot across requests.

The `page` object includes `size`, `cursor` (null at the end), `cursor_version` (currently 1), `first_commit_seq`, `expires_at`, `changed_since_first_page` and `change_notice`. `first_commit_seq` remains the first page's commit position. A different current commit position sets `changed_since_first_page` to true and supplies an explanation in `change_notice`; otherwise the notice is null. This is a conservative commit-position comparison, including source observations. It does not establish that matching rows changed. `total` describes all matches in the current page's state, not the number still reachable after its cursor key.

A cursor expires 24 hours after the first request's resolved clock, disclosed as the RFC3339 `page.expires_at`. Continuing never extends that deadline. It binds the raw-event meaning/version and ordering descriptor, structural query, last key, first commit position, and the first page's `resolved.now`, `zone` (UTC) and `logical_start` (04:00). These temporal values stay unchanged on later pages; the page's `resolved.commit_seq` remains current. Equal conjunctions are accepted with reordered or repeated terms, whitespace or quoting differences, `when`/`ts_orig` aliases and equivalent RFC3339 times or UUID spellings. Value or operator changes conflict.

Malformed, expired, incompatible or conflicting cursors fail as `bad_query` (CLI exit 2), with no automatic restart. The predecessor cursor encoding is refused. A compatible daemon restart alone preserves continuation: all required cursor state travels in the cursor and no server cache is needed. Callers treat its encoding as opaque. Unsupported language, ordering, historical reads, semantic limits and variable page sizes remain unavailable.

`complete` and CLI exit 5 report source coverage independently of pagination. A successfully encoded page, a non-null cursor or an exact current-state count does not prove that configured sources have completed their reads. An unavailable source can leave retained rows queryable with incomplete coverage.

The declared `live_cursor_acceptance` operation exercises the Nix-packaged daemon and CLI on an independent 241-row synthetic Atuin fixture, applies removals and append rows on both sides of the continuation key, then checks exact totals, stable nanosecond/identity ordering, structural restatement, unchanged and changed notices, refusals, compatible restart and retained paging with unavailable source coverage. It uses owned disposable PostgreSQL state and preserves raw command outputs and fixture variants through the existing foundation report. Pure store tests cover structural identity and cursor context validation, including unknown-time continuation keys. This qualifies the supported raw-page refinement; full M1 acceptance and the general language remain open.

## Raw-event read deadlines and cancellation

An explicit `--query-timeout-ms` on another command is refused before reading credentials or contacting the daemon; it is never silently ignored.

`query.run` defaults to a 10,000 ms execution deadline. Its optional `timeout_ms` parameter accepts an integer from 1 to 3,600,000; other values fail as `bad_query` before SQL execution. `sinexctl query` and `search` send 60,000 ms by default, configurable with `--query-timeout-ms`. This is an execution budget and changes no filters, count, page, cursor or source-coverage meaning. The deadline covers reader-pool acquisition and the entire repeatable-read page, including its count and source observations. A transaction-local PostgreSQL `statement_timeout` additionally bounds each SQL statement. The client allows five further seconds to receive the complete deadline response; transport timeout also exits as `budget`, with a distinct message.

An execution deadline returns JSON-RPC error kind `budget`, code -32006 and CLI exit 6. Its data names `execution_deadline` and `timeout_ms`, reports `estimate: null` because this subset has no cost estimator, and suggests narrowing supported filters or retrying after contention ends. It carries no rows, total, cursor or completeness claim. This does not implement the specification's cost estimates or general query planner.

The response connection owns the read and subsequent masking/encoding future. Disconnecting or abandoning that response drops its pending work and closes that query's exact checked-out PostgreSQL transport instead of recycling it. Each query transaction sets PostgreSQL's `client_connection_check_interval` to 100 ms so an observed socket closure aborts the blocked statement and releases its snapshot. Cleanup sends no PID-targeted cancellation signal, so independent backend death and PID reuse cannot redirect cancellation to another session. Successful reads commit and return their connections normally. Cancellation never reaches the writer or undoes admission. The statement deadline remains a fallback if the kernel cannot observe socket loss, including network partitions. This fast disconnect behavior is qualified on Linux with PostgreSQL 18 over an owned Unix-domain socket; other transports and platforms remain unqualified. See PostgreSQL's [connection-check documentation](https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL).

The response encodes one complete JSON-RPC object before emitting its first body bytes. A disconnected or failed transport cannot become a successful partial page. Synchronous masking and JSON encoding run within the response future; this subset does not provide preemption within synchronous CPU work. These deadlines and backend cleanup currently qualify `query.run`, including CLI search. Other read methods and MCP have not received this qualification.

`read_lifecycle_acceptance` runs the packaged daemon and CLI against owned PostgreSQL state. It acquires an actual exclusive lock on the event relation, observes blocked statements and their snapshot ownership in `pg_stat_activity`, then verifies disconnect cleanup, the API's 10-second default, the CLI's 60-second default and a configured deadline. Two simultaneous blocked clients also exercise a deadline during reader-pool acquisition and cancellation while both reader slots are occupied. Successful reads after each case establish usable reader slots and released snapshots. The retained report includes commands, exact package/binary identity, raw responses and resource observations. Existing continuation and source-coverage qualification remains separate.
