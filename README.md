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
  `sinexd backfill --source KIND=PATH` reads sources to the end;
  `sinexd run --sources sources.json` reads configured bindings continuously;
  `sinex-session` and `sinex-capture` capture from the desktop session and
  from other hosts and send over the NATS ingress.
  New unsealed archive bindings capture one physical revision per host and effective
  privacy class, then traverse it once while admitting bounded record parts. Logical
  bindings keep separate materials and committed cursors. Restart reuses retained
  bytes and may replay the archive prefix once; replay reads the exact stored record
  ranges. ZIP requires a private temporary file for seeking; overlapping selectors
  with different framing may stage one decoded member. One valid record may exceed
  the usual part size. Sealed sources retain their keyed reader. Existing unsealed byte cursors require
  an explicit migration and are refused without changing their saved progress.
  Excision refuses records backed by a retained original archive: cross-binding
  archive rewriting is not implemented, and hiding does not delete those bytes.
- **Derivations.** Fused shell commands, shell sessions, state intervals,
  presence, hourly and daily summaries, and text read from screen stills,
  recomputed per changed key as inputs arrive.
- **Removal and replay.** Hide, unhide and excision, remembered across
  re-imports; replay of stored materials with the current parsers.
- **Privacy.** Versioned rules applied at admission, privacy classes, the
  sealed class with unlock sessions, and a private-mode switch.
- **Reading.** One query language, search, recall, provenance and read-only
  SQL, through `sinexctl`, the HTTP API and MCP, all generated from one
  command table. Committed events are published in commit order on a NATS
  stream.
- **Analysis.** Metrics per logical day with coverage, registered methods
  (correlations, trends, change points, deviations), stored results and a
  daily brief.
- **Library.** A media library on the same storage: items, renditions, tags,
  properties, collections, ratings and pairwise ranking.
- **NixOS module.** `nix/` holds a NixOS module and its VM tests.

## Build and test

```bash
nix develop --command cargo run -p xtask -- check   # fmt, boundaries, clippy against the schema database
nix develop --command cargo run -p xtask -- test    # every test under nextest; database tests clone the schema template
nix develop --command cargo run -p xtask -- db ensure   # the schema database's URL, for an editor
```

For a focused run through the normal job queue:

```bash
agentctl job start sinex focused -- -p sinex-sources -E 'test(session_socket)'
```

`xtask test` passes arguments to `cargo nextest run`. `-p` / `--package`
selects what Cargo builds; a name or `-E` filter alone selects tests after
building the workspace. Existing `xtask test NAME` calls keep their behavior.
Without arguments it still runs the full workspace. Package selection keeps
the same schema database, per-test clones, shared NATS and tmpfs test storage.

A first import of the operator's shell history:

```bash
sinexd backfill --database URL --store DIR --host HOST \
  --source atuin=$HOME/.local/share/atuin/history.db --source zsh="$HISTFILE"
```

## Benchmark reports

`agentctl job start sinex bench -- run` uses the frozen sample. Reports keep
source-file bytes, per-source material bytes, database size and final blob-store
size separate. The runner does not measure total decoded, hashed or physically
written bytes. Its load observations cover setup, backfills, derivation and
shutdown, with sampled peaks, unknown readings and the largest sampling gap.

Full-input estimates explicitly assume linear time per source-file byte and are
uncalibrated; setup and teardown are excluded. A failed input or missing component
time leaves the total estimate unknown. A passing load check permits descriptive
comparisons on the same sample and storage, without establishing timing reliability.
