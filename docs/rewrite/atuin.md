# Retained Atuin source subset

This isolated foundation connects `terminal.atuin-history` to normal CLI raw-event answers. It is preparation for the first milestone, not formal M1 acceptance or a deployed service. Only synthetic inputs are qualified by its acceptance command.

Enable one source with explicit absolute paths in the normal daemon configuration:

```toml
sources = ["terminal.atuin-history"]
automata = []

[atuin]
path = "/absolute/path/synthetic-history.db"
poll_interval_ms = 30000
full_interval_ms = 86400000 # successful full value comparison, default one day

[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
```

Privacy patterns are Rust regular expressions with literal replacement text. They apply once before storing each event interpretation, including replay from retained original rows. Ordinary reads preserve those stored values. Retained raw-row reads are a separate protected projection of original plain evidence: they apply the complete attributed rule sequence, then current configured entries whose exact `(pattern, replacement, created_by)` tuple is absent from the attributed sequence. Authored duplicates within the attributed sequence remain; additional current duplicates absent there also retain their multiplicity and order. This bounded raw policy preserves both old and current protection, without claiming policy equivalence or reinterpreting stored payloads. Changes to opaque raw selectors or hashes refuse the whole read. The fixed credential output floor still applies to every response. Source identity and row fingerprints use original unmasked fields. Retained canonical rows are explicitly plain evidence. Guarded/sealed storage and privacy administration are unavailable.

Before RPC admission at startup, the writer commits the configured input and an empty `reading` observation through the normal Atuin admission path. The first poll reuses that observation. Each subsequent poll likewise commits its reading observation before SQLite open/read. Queries therefore report incomplete coverage while the initial open/read is blocked, including after a previously completed zero-row poll and restart. With no configured or retained inputs, empty coverage remains complete. Only reaching EOF establishes completed zero; open/read errors establish unavailable.

The collector opens SQLite read-only without the create flag and retains its connection between polls. One read transaction binds each poll's cookies, coordinates and selected values. A new connection always performs a full value comparison. Between full comparisons, the collector selects coordinates above its acknowledged rowid checkpoint and previously admitted unfinished native keys, including very old commands. Eligibility comes from canonical heads as of the poll's admission basis, not the provider's current unfinished predicate: a newly completed row must still be read. End-known/unknown-exit rows are not unfinished. Each actual read, including an unchanged confirmation, retains the complete original row and acquisition envelope. Unread keys and empty reduced polls do not advance any value reading basis.

The connection's epoch is checkpoint scope, never provider identity or a new logical input. Before and after reading, SQLite's handle-bound `SQLITE_FCNTL_HAS_MOVED` check and the configured path's resolution against SQLite's opened filename must both succeed. Rename/replacement, symlink retargeting, unsupported or unverified correspondence discard that reader; reopening requires a real full read. Schema/version cookies, including a normal VACUUM change, invalidate reduced progress. This supports SQLite-protocol changes through the tested Unix VFS, not arbitrary byte overwrites or a durable physical incarnation proof. Registered input plus native ID remains the occurrence key; rowid is only a read coordinate, and rowid reuse or explicit insertion below the checkpoint is found by a full comparison.

`full_interval_ms` defaults to one day and accepts 100 through 604800000 ms. A completed full read selects its next full-due target with checked clock arithmetic; the final successful admission atomically retains that target and checkpoint in the existing cursor carry. Reduced successes do not postpone it. The same live connection also retains the paired monotonic deadline, so a backward realtime adjustment cannot postpone a full comparison indefinitely; restart still forces full. Failed/partial reads preserve the last checkpoint and discard the connection; restart or reopen always performs a real full read before reduced reuse. An actually completed initial full may reset the target. Known parser omissions force another full comparison instead of disappearing on an empty reduced poll. `_sinex_read_plan_v1` labels completed observation scope and checkpoint metadata; legacy observations without it have unknown read-plan evidence. Existing `observed_at` remains poll start and controls reading order. Full completion time is observed after source EOF, before the final writer reply; it is not an exact provider commit time.

The source yields at most 64 rows and 1 MiB per writer step, with bounded prior-unfinished pages and a two-second canonical eligibility-query timeout. A single oversized row or failed read leaves the poll unavailable. Rowid range selection uses the provider's existing rowid access and native-key lookups use its existing key index when present; no provider indexes are added. Counters measure selected/decoded records and retained work, not internal scans, physical IO or CPU. Canonical unfinished eligibility may scan history within its read budget. Completed reduced scope does not mean every current provider row was revalidated. Completed source coverage exposes that scope and the last full value-comparison target. A later reading or unavailable observation has unknown current read scope; earlier completed evidence remains inspectable through `sources`.

A scheduled full value comparison is not a retained physical SQLite snapshot, completed key census or physical-deletion proof. Missing keys do not admit removal. Those capabilities, transaction-time adaptation, general service fairness and sustained resource acceptance remain open.

The input registry identifies the source database separately from its acquired row materials. Canonical sorted-key JSON lines retain SQLite values and an `_from` observation/table/rowid reference. Rowid is acquisition evidence only; equivalence uses the registered input and Atuin native ID. Unfinished rows (`duration = -1`, `exit = -1`) report `completion = end_not_recorded`: no end is recorded as of this reading; neither running nor terminal state is proved. A later same-session command does not change this classification or prevent future rereads. Nonnegative duration, including zero with exit -1, records an end; the exit stays unknown. New captures omit the retired derived `_end_not_recorded` hint. The current parser ignores that historical hint when reparsing retained v1 logical rows; their bytes remain unchanged. Historical payload schemas still admit old `running` records for truthful history, but the current parser never emits that unsupported running assertion. Fingerprints exclude the acquisition observation and rowid. Parser recipe `atuin-v3` is bound through the existing producer-version field independently of the source fingerprint and equivalence key. Ordinary suppression requires both unchanged source hash and matching recipe; a newer reading under a changed or previously unrecorded recipe readmits, archives the prior interpretation and dirties its canonical group. An older observation still cannot replace a newer one. Replay binds the current recipe even when the evidence material was captured under v1. Later changed readings supersede their prior event; soft deletes become removed readings, preserved in history and absent from ordinary event queries. Only provider sentinel `-1` maps to null. Other signed 64-bit exit statuses, including `-2`, remain exact integers; zero alone is reported success, with no signal decoding or claim about why a negative value occurred. A non-sentinel status also records an end under Atuin’s history-end assignment contract, even when duration is unknown. [Pinned Atuin history-end](https://github.com/atuinsh/atuin/blob/fb33978d9439ba7e97123c1adc932c6a74e3c8ae/crates/atuin/src/command/client/history.rs) accepts signed `i64` values; [upstream PR 821](https://github.com/atuinsh/atuin/pull/821) preserves negative shell statuses. Guarded schema change 6 relaxes the registered Atuin payload domain without changing old schema fingerprints or retained bytes. The persistent `atuin-v1` payload-format label remains distinct from the parser recipe. Retained v1/v2 rows can be reprocessed under v3; previous omission observations remain historical failures until an actual subsequent successful source read establishes its current outcome. Integer source timestamps and duration values retain nanoseconds; PostgreSQL's timestamp carries the microsecond portion and `ts_orig_tiebreak` carries the remaining nanoseconds.

Rows append to an owned open part and are fsynced before the writer transaction commits its events, failures, durable material length, observation accounting and cursor. Parts close into the content store at poll completion or 100 MiB. Startup truncates an interrupted part to its committed length and closes it as recovered partial. A restart retries the source snapshot from the beginning; equivalence suppression prevents duplicate live facts. An incomplete, unavailable or interrupted observation never reports completed zero. Physical SQLite page/snapshot revisions remain unavailable until slice 1c; row evidence is not a database snapshot.

Use explicit endpoints as in the main README:

```sh
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token sources --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token query 'events where source = terminal.atuin-history and kind = command.executed' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token show EVENT_UUID --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token trace EVENT_UUID --bytes --json
```

The query subset supports `events`, conjunctions, equality on `source`, `kind`, `id`, `when`, `payload.command`, `payload.exit_code`, and literal substring `~` on command text. The connected [Chromium subset](chromium.md#public-queries) adds its fixed content/profile/GUID/redirect fields and default redirect filtering; raw cursor version 2 refuses prior descriptors rather than reinterpret changed defaults. Other raw syntax is refused. `search` now uses the separate [indexed shell retrieval subset](indexed-retrieval.md); it requires the explicit projection profile and does not fall back to substring scans. That contract owns the `commands` alias and supported full-text/project/time clauses. Arbitrary payload paths and the full general query language remain unavailable. Queries return exact totals and 100-row live pages; pass the self-contained keyset cursor using `query QUERY --cursor CURSOR`. The continuation contract below defines current-state changes, expiry and compatibility. Source incompleteness is explicit and the CLI prints the partial answer with exit 5. `show` includes archived and source-removed readings. `trace --bytes` resolves a masked canonical row and its exact retained range.

Run `foundation_exec -- python3 tests/foundation.py --atuin-exits --atuin-exits-predecessor-pin FILE` for signed status, guarded schema upgrade and retained v2 failure reprocessing controls. The pin must identify the exact published stage-five package. These synthetic controls do not claim complete real-provider or continuous coverage.

Run declared `atuin_read_plan_acceptance` for the reduced plan, or `atuin_read_plan_package_acceptance` with the exact immutable package pin for its public CLI/MCP recovery controls. The older `atuin_acceptance` and `atuin_package_acceptance` fixtures explicitly request a short full interval to preserve their full-comparison assertions. All commands use the explicit checkout path. The owned harness retains commands, exits, source observations, binary hashes and synthetic fixture variants even on failure. Its late-update case spans several writer steps, and its empty/failed cases use separate product state. The packaged Kestrel driver uses its independently pinned oracle and explicit
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
observations; admission itself always uses the product writer. These source durability controls do not qualify the separate indexed read model.

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

The response encodes one complete JSON-RPC object before emitting its first body bytes. A disconnected or failed transport cannot become a successful partial page. Synchronous masking and JSON encoding run within the response future; this subset does not provide preemption within synchronous CPU work. These earlier controls qualify the raw `query.run` route. The [indexed retrieval qualification](indexed-retrieval.md) separately exercises indexed search, MCP cancellation and the newly connected object/source/status read leases and deadlines.

`read_lifecycle_acceptance` runs the packaged daemon and CLI against owned PostgreSQL state. It acquires an actual exclusive lock on the event relation, observes blocked statements and their snapshot ownership in `pg_stat_activity`, then verifies disconnect cleanup, the API's 10-second default, the CLI's 60-second default and a configured deadline. Two simultaneous blocked clients also exercise a deadline during reader-pool acquisition and cancellation while both reader slots are occupied. Successful reads after each case establish usable reader slots and released snapshots. The retained report includes commands, exact package/binary identity, raw responses and resource observations. Existing continuation and source-coverage qualification remains separate.

The v2 interpretation correction is independent of the current reduced read plan; the former later-row label did not demonstrate native data loss. The focused typed prior-recipe test uses byte-identical synthetic source fields and explicitly does not claim an actual predecessor binary. It preserves prior retained coordinates/row/hash, checks v1→v2 replacement and canonical maintenance, then checks v2 duplicate suppression and stale-reading precedence.

Explicit raw `payload.*`/`ts_orig` selectors retain raw binding and its type errors; a refused explicit raw selector does not fall through to indexed availability. Indexed questions bind before projection availability is checked, so malformed or unsupported shapes return `bad_query` even with an empty source and no configured index. Valid raw reads remain available independently.

[Reversible exclusion](reversible-exclusion.md) is separate from source removal. A listed Atuin stable key suppresses every normal/replayed admission while retaining newly read row evidence. Source accounting discloses these rows as `excluded`; `rows_read = admitted + repeated + excluded + record_errors`. Undo only removes the explicit exclusion, permitting later acquisition or retained replay without unarchiving old readings.

The [acquisition investigation subset](source-investigation.md) extends this same sources route with labelled settlement/next-start metadata, inherited outcome references and bounded history/acquisition receipts. Poll-start `observed_at` and reading order remain unchanged. New scheduling metadata advances the canonical read position; legacy absent timing and receipt evidence stays unknown.
