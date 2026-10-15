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

The query subset supports `events`, conjunctions, equality on `source`, `kind`, `id`, `when`, `payload.command`, `payload.exit_code`, and literal substring `~` on command text. Other syntax is refused. `search TEXT` uses that same command substring query. The later `commands` alias, full-text indexing, arbitrary payload paths and the general query language are unavailable. Queries return exact totals and 100-row live pages; pass the query-bound keyset cursor using `query QUERY --cursor CURSOR`. Source incompleteness is explicit and the CLI prints the partial answer with exit 5. `show` includes archived and source-removed readings. `trace --bytes` resolves a masked canonical row and its exact retained range.

Run declared `atuin_acceptance` or `atuin_package_acceptance` with the explicit checkout path. The owned harness retains commands, exits, source observations, binary hashes and synthetic fixture variants even on failure. Its late-update case spans several writer steps, and its empty/failed cases use separate product state. The packaged Kestrel driver uses its independently pinned oracle and explicit
bad-answer/launch reporting controls. `kestrel_boundary_acceptance` additionally
qualifies the observed retention and admission boundaries described below.
Time-budget/channel fairness, a general read-model lifecycle/index, physical
SQLite snapshots and formal task-routing/M0/M1 gates remain open.


## Observed durability boundaries

The explicit qualification socket control holds the real writer at three named
instructions: `retained-fsynced` after the canonical open-part write and
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
events or progress to readers. Their fsynced but uncommitted tail is deliberately
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
