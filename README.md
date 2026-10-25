# Sinex

Sinex is a durable record of one person's digital life. It keeps the exact
bytes of everything its sources observe, turns those bytes into timestamped
events, builds derived knowledge from the events in layers, and answers
questions about all of it. Stored bytes are the record of truth: every event
points at the byte range it came from, or at the events it was derived from,
and can be read again when a parser or rule changes.

This is the second rewrite, built to the target architecture. The old Sinex
is in Git history for reference.

[Architecture](ARCHITECTURE.md) describes current module ownership and the
boundaries between capture, interpretation, admission and public interfaces.

## What works

- **Storage.** The schema is declared in Rust and applied by
  `sinex-schema apply`. Admission is the one way events are stored: one
  set-based path with one live reading per record location or equivalence
  key, newer readings replacing older ones, and events and source progress
  committed together, through one writer task with live, derived and bulk
  priorities. Bytes go to a content store named by their BLAKE3 hash.
- **Sources.** Eight shared drivers (append-only files, SQLite databases,
  export archives, directory snapshots, live streams, API cursors, media
  segments, producers over NATS) and about forty sources on them: shell
  history, browsers, desktop and session capture, mail, chat, calendars,
  exports from Google, Samsung, Spotify, Reddit and others, git, the journal,
  file changes, the Sinnix capture lake, and Polylogue's archive.
  Raw captured material stays available for replay after a parser changes.
  `sinexd backfill --source KIND=PATH` reads sources to the end;
  `sinexd run --sources sources.json` reads configured bindings continuously;
  `sinex-session` and `sinex-capture` capture from the desktop session and
  from other hosts and send over the NATS ingress. An export archive is
  stored once and read in one pass for all the sources that use it. A sealed
  source needs only the class key's public half to capture; replaying or
  continuing a sealed archive needs an operator unlock
  (`sinexd backfill --identity-file KEYFILE`, or `replay --sealed`).
- **Derivations.** Fused shell commands, shell sessions, state intervals,
  presence, hourly and daily summaries, and text read from screen stills,
  recomputed per changed key as inputs arrive.
- **Removal and replay.** Hide, unhide and excision, remembered across
  re-imports and re-reads of the source; replay of stored materials with the
  current parsers. Excision refuses sealed material, a producer's records and
  records inside a stored original archive.
- **Privacy.** Versioned rules applied at admission, privacy classes, the
  sealed class with unlock sessions, and a private-mode switch.
- **Reading.** One query language, search, recall, provenance and read-only
  SQL, through `sinexctl`, the HTTP API and MCP, all generated from one
  command table. Committed events are published in commit order on a NATS
  stream.
- **Analysis.** Metrics per logical day with coverage, registered methods
  (correlations, trends, change points, deviations), stored results and a
  daily brief. Binary probability forecasts can be issued and resolved as stored
  authored events; `forecast-report` computes Brier score and ten fixed calibration
  bins over visible resolved pairs.
- **Library.** A media library on the same storage: items, renditions, tags,
  properties, collections, ratings and pairwise ranking.
- **NixOS module.** `nix/` holds a NixOS module and its VM tests.

CLI help, `/v1/commands` and MCP list implemented operations. Planned command
work includes `stage`, duplicate decisions (`same`/`distinct`) and moment marks
(`mark`).
Recall supports a time, day, the default current-time window, or `last` for the
latest recorded shell command group. `mark` remains planned. For example:

```sh
sinexctl query 'shell-sessions where cwds ^ /src/tide sort start desc'
sinexctl recall last --cwd '/src/tide (notes)'
sinexctl query 'recall last where cwds ^ /src/tide'
```

`--cwd` selects a group with a recorded member directory at or below that path;
it keeps context from other directories and sources in the selected window.
It applies only to `last`. Shell groups use recorded command extents, including
both endpoints; `observed_until` does not prove that activity ended. Unknown
durations count as points. The broader activity `sessions` alias remains
unsupported. If no matching timed group is known, recall says so without
claiming that no observations exist.

## Probability forecasts

With an operator token or an agent token granted `guarded`, record a forecast
with an explicit future horizon and information cutoff. Optional `--information`
citations must already have been recorded by that cutoff. The issue response
contains an event citation for resolution and ordinary `show`/`query` commands.

```sh
sinexctl forecast-issue 'The synthetic job finishes' 0.8 'A completion record exists' \
  '2099-01-01T00:00:00Z' '2026-10-24T00:00:00Z'
sinexctl forecast-resolve sinex:ev/ISSUED_EVENT true --evidence sinex:ev/EVIDENCE_EVENT
sinexctl forecast-report
```

`forecast-resolve` accepts `true`, `false` or `unknown` and requires live visible
evidence citations. Its optional `--at` records when the outcome occurred;
omission leaves that time unknown. The authenticated transport role supplies
`by` (`operator` or `agent`), and the daemon records when each act was submitted.
The original request bytes and manifest are stored through ordinary admission.
The information position records the visible archive head used at issuance;
the supplied information citations describe its declared information set.

The report is computed on each call at one archive position. It uses
`mean((probability - outcome)^2)` over known outcomes. Unknown or withdrawn
resolutions remain unresolved; no resolved pairs yields `not_estimable` and a
null Brier score. Empty calibration bins have null means and frequencies.
Hiding or excising an issue removes its pair; withdrawing its resolution or
supporting evidence makes the outcome unavailable. Concurrent known resolutions
for one forecast refuse scoring until an incorrect resolution is hidden.
The report includes its input event IDs and counts withheld events and unavailable
evidence separately. Forecasts use the guarded class floor. Sealed evidence and
sealed authored request storage remain unavailable to these commands.

## Initialize

Create the PostgreSQL database, then create the schema explicitly before
starting the daemon or running backfill, replay, excision or derivations:

```sh
sinex-schema apply 'postgresql:///sinex'
# Equivalent: sinexd admin schema --database 'postgresql:///sinex'
sinexd backfill --database 'postgresql:///sinex' --store /path/to/store --source notes=/path/to/vault
```

Ordinary commands require the schema at this build's version and hold the
single-writer claim; they never create the schema themselves.

## Build and test

```bash
nix develop --command cargo run -p xtask -- check   # fmt, boundaries, clippy against the schema database
nix develop --command cargo run -p xtask -- test    # every test under nextest; database tests clone the schema template
nix develop --command cargo run -p xtask -- db ensure   # the schema database's URL, for an editor
```

For a focused run through the normal job queue:

```bash
agentctl job start sinex focused -- -p sinex-sources -E 'test(session_socket)'
agentctl job start sinex focused -- -p sinexd --bin sinexd -E 'test(derivation_runner::tests)'
```

`xtask test` passes arguments to `cargo nextest run`. `-p` / `--package`
selects what Cargo builds; a name or `-E` filter alone selects tests after
building the workspace. Existing `xtask test NAME` calls keep their behavior.
Within a package, select the needed Cargo target too: `--lib` for library unit
tests, `--bin sinexd` for internal daemon tests, or `--test shutdown` for that
integration target. A name filter alone still builds the package's other test
binaries, even when none of their tests will run. Use the broader package or
workspace checks when the change needs those integration paths.
Without arguments it still runs the full workspace. Package selection keeps
the same schema database, per-test clones, shared NATS and tmpfs test storage.
The declared test operations give that storage to AgentCTL, which removes it
when the native job exits, including cancellation or SIGKILL. Devkit uses a
short held-directory path for long Linux socket roots while keeping files
inside owned scratch. Standalone Drop removes only its child; abrupt death has
no external cleanup guarantee. PostgreSQL and NATS keep their existing separate
lifecycle owners.

AgentCTL forwards an explicit `CARGO_TARGET_DIR` from the caller at enqueue time. Select an absolute target with exclusive writer ownership; omitting it keeps the devshell's checkout-local target.

```sh
CARGO_TARGET_DIR=/realm/worktree/leased-build/target \
  agentctl job start sinex focused --workspace /realm/worktree/sinex-edit -- -p sinex-sources -E 'test(session_socket)'
```

A first import of the operator's shell history:

```bash
sinexd backfill --database URL --store DIR --host HOST \
  --source atuin=$HOME/.local/share/atuin/history.db --source zsh="$HISTFILE"
```

Zsh imports accept plain and extended history, including mixed files. Plain
commands keep unknown start times and separate record locations; extended
records keep their source times and existing occurrence keys.

## Benchmark reports

`agentctl job start sinex bench -- run` uses the fixed sample. Reports keep
source-file bytes, per-source material bytes, database size and final blob-store
size separate. The runner does not measure total decoded, hashed or physically
written bytes. Its load observations cover setup, backfills, derivation and
shutdown, with sampled peaks, unknown readings and the largest sampling gap.

Full-input estimates explicitly assume linear time per source-file byte and are
uncalibrated; setup and teardown are excluded. A failed input or missing component
time leaves the total estimate unknown. A passing load check permits descriptive
comparisons on the same sample and storage, without establishing timing reliability.
