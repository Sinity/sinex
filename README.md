# Sinex

Sinex is a durable record of one person's digital life. It keeps the exact
bytes of everything its sources observe, turns those bytes into timestamped
events, builds derived knowledge from the events in layers, and answers
questions about all of it. Stored bytes are the record of truth: every event
points at the byte range it came from, or at the events it was derived from,
and can be read again when a parser or rule changes.

[ARCHITECTURE.md](ARCHITECTURE.md) describes how the crates divide the work,
and [docs/vocabulary.md](docs/vocabulary.md) names the concepts.

## What it does

- **Storage.** Bytes go to a content store named by their BLAKE3 hash.
  Admission is the one way events are stored: one live reading per record
  location or equivalence key, a newer reading replacing an older one, and
  events and source progress committed together by one writer task.
- **Sources.** Shared drivers (append-only files, SQLite databases, export
  archives, directory snapshots, live streams, API cursors, media segments,
  outside producers over NATS) and about forty sources on them: shell history,
  browsers, desktop and session capture, mail, chat, calendars, exports from
  Google, Samsung, Spotify, Reddit and others, notes vaults, git, the journal,
  file changes, the Sinnix capture lake, and AI sessions from Polylogue. An export
  archive is stored once and read in one pass for every source that uses it.
- **Derivations.** Fused shell commands, shell sessions, state intervals,
  presence, hourly and daily summaries, and text read from screen stills,
  recomputed per changed key as their inputs arrive.
- **Removal and replay.** Hide and unhide; excision, which deletes a record's
  events and bytes for good; replay of stored bytes with the current parsers.
  Hidden and excised records stay out when the source is read again.
- **Privacy.** Rules applied at admission, the classes `open`, `guarded` and
  `sealed`, and a private-mode switch that stops capture. Sealed sources are
  encrypted at rest; capture needs only the class key's public half, and
  reading or replaying sealed data needs an operator unlock.
- **Reading.** One query language, search, recall, provenance and read-only
  SQL, through `sinexctl`, the HTTP API and MCP, all generated from one command
  table. Committed events are published in commit order on a NATS stream.
- **Analysis.** Metrics per logical day with their coverage, registered
  methods (correlations, trends, change points, deviations), stored results, a
  daily brief, and probability forecasts with a Brier-score report.
- **Library.** A media library on the same storage: items, renditions, tags,
  properties, collections, ratings and pairwise ranking.
- **NixOS module.** `nix/` holds a NixOS module and its VM tests.

`sinexctl --help`, `/v1/commands` and the MCP tool list show the commands that
exist.

## Start

Create the PostgreSQL database, then the schema, then read some sources:

```sh
sinex-schema apply 'postgresql:///sinex'
sinexd backfill --database 'postgresql:///sinex' --store /path/to/store \
  --host HOST --source atuin=$HOME/.local/share/atuin/history.db --source zsh="$HISTFILE"
```

`sinexd admin schema` does the same as `sinex-schema apply`. No other command
creates the schema, and every command refuses a database at another schema
version. `backfill` reads each source to its end. The daemon, `sinexd run`,
reads the bindings in `--sources` continuously and also needs NATS and a tokens
directory; the NixOS module sets it up. A sealed source that was left part-way needs
`--identity-file KEYFILE` to continue.

## Ask

```sh
sinexctl query 'shell-sessions where cwds ^ /src/tide sort start desc'
sinexctl recall last --cwd '/src/tide (notes)'
sinexctl show sinex:ev/EVENT_ID
```

`recall` takes a time, a day, nothing (the current window), or `last` for the
latest recorded shell session; `--cwd` picks the latest session that ran a
command at or below that directory and shows everything recorded during it.
Shell sessions span their recorded commands, so their end is the last command
seen, not a known end of activity.

## Forecasts

```sh
sinexctl forecast-issue 'The synthetic job finishes' 0.8 'A completion record exists' \
  '2099-01-01T00:00:00Z' '2026-10-24T00:00:00Z'
sinexctl forecast-resolve sinex:ev/ISSUED_EVENT true --evidence sinex:ev/EVIDENCE_EVENT
sinexctl forecast-report
```

A forecast names a probability, how it resolves, a future horizon and an
information cutoff; `--information` citations must be recorded before the
cutoff. A resolution is `true`, `false` or `unknown` and cites live evidence;
`--at` records when the outcome happened, and without it that time stays
unknown. Both are ordinary stored events, at the `guarded` class, and need the
operator token or an agent token granted `guarded`. The report is computed on
each call: the Brier score over known outcomes and ten calibration bins. Hiding
or excising a forecast removes it from the report; two conflicting known
resolutions of one forecast stop the report until one is hidden.

## Build and test

```sh
nix develop --command cargo run -p xtask -- check      # fmt, boundaries, clippy against the schema database
nix develop --command cargo run -p xtask -- test       # every test under nextest
nix develop --command cargo run -p xtask -- db ensure  # the schema database's URL, for an editor
```

Through the shared job queue:

```sh
agentctl job start sinex focused -- -p sinex-sources -E 'test(session_socket)'
agentctl job start sinex focused -- -p sinexd --bin sinexd -E 'test(derivation_runner::tests)'
agentctl job start sinex compile -- -p sinex-store --tests
```

`xtask test` passes its arguments to `cargo nextest run`. `-p` limits what is
built; a name or `-E` filter alone still builds the whole workspace, and within
a package `--lib`, `--bin sinexd` or `--test NAME` limits it to one target.
Each database test gets its own copy of the schema template. AgentCTL takes
`CARGO_TARGET_DIR` from the caller; give each target directory one build at a
time.

## Benchmarks

`agentctl job start sinex bench -- run` backfills a fixed sample of real
inputs and reports source-file bytes, stored bytes per source, database size
and blob-store size, with timings and sampled memory and load. Estimates for
the full inputs assume time grows linearly with input bytes.
