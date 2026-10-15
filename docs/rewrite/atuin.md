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

Run declared `atuin_acceptance` or `atuin_package_acceptance` with the explicit checkout path. The owned harness retains commands, exits, source observations, binary hashes and synthetic fixture variants even on failure. Its late-update case spans several writer steps, and its empty/failed cases use separate product state. Process kill/restart is a durability smoke check. The independent Kestrel oracle, deliberate bad-answer/launch reporting, an observed kill between fsync and commit, time-budget/channel fairness, full read-model lifecycle and formal task-routing/M0 gates remain separate acceptance obligations.
