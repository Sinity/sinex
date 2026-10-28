# Sinex

Sinex is a durable record of one person's digital life. It keeps the exact
bytes of everything its sources observe, turns those bytes into timestamped
events, builds derived knowledge from the events in layers, and answers
questions about all of it. Stored bytes are the record of truth: every event
points at the byte range it came from, or at the events it was derived from,
and can be read again when a parser or rule changes.

[docs/architecture.md](docs/architecture.md) describes how the crates divide the work,
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
- **Derivations.** Fused records (shell commands, window focus, page visits,
  plays, clipboard changes, notifications and mail, each once however many
  sources saw it), shell sessions, state intervals, presence, hourly and daily metric
  figures, hourly and daily health figures, and text read from screen stills,
  recomputed per changed key as their inputs arrive.
- **Removal and replay.** Hide and unhide; excision, which deletes a record's
  events and its material's bytes for good, when the record is alone in its
  material and its source never reads a record twice (live streams, media
  capture, forecasts), and an outside producer's excision of whole materials
  it registered; replay of stored bytes with the
  current parsers. Hidden records stay out when the source is read again.
- **Privacy.** Rules applied at admission, the classes `open`, `guarded` and
  `sealed`, and a private-mode switch that stops capture. Sealed sources are
  encrypted at rest; capture needs only the class key's public half, and
  reading or replaying sealed data needs an operator unlock.
- **Reading.** One query language, search, recall, provenance and read-only
  SQL, through `sinexctl`, the HTTP API and MCP, all generated from one command
  table. Committed events are published in commit order on a NATS stream.
- **Analysis.** Panels of the daily metric figures with the coverage that
  tells a zero from an unobserved day, registered methods (correlations,
  trends, change points, deviations) run on request over those figures, a
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
  --host HOST --read atuin=$HOME/.local/share/atuin/history.db --read zsh="$HISTFILE"
```

`sinexd admin schema` does the same as `sinex-schema apply`, which also
upgrades a database at an older schema version while sinexd is stopped. No
other command creates or upgrades the schema, and every command refuses a
database at another schema version. `backfill` reads each input to its end. The daemon, `sinexd run`,
reads the bindings in `--sources` continuously and also needs NATS and a tokens
directory; the NixOS module sets it up. A sealed source that was left part-way needs
`--identity-file KEYFILE` to continue.

## Run on a host

On NixOS, import `nixosModules.default` from this flake and enable it:

```nix
services.sinex = {
  enable = true;
  nats.daemonPasswordFile = "/run/agenix/sinex-nats-daemon";
  readAnyFile = true;                 # sources under a private home directory
  sources."terminal.atuin-history" = {
    enable = true;
    paths = [ "/home/me/.local/share/atuin/history.db" ];
  };
  session = {                         # live desktop sources, as the operator
    enable = true;
    user = "me";
    passwordFile = "/run/agenix/sinex-nats-session";
    hyprland = true;
    clipboard = true;
  };
  backfills.takeout-2025 = [ "takeout=/data/takeout-2025.tgz" ];
};
```

That gives these units:

- `postgresql.service`, with its data in `database.dataDir`, and
  `sinex-db-setup.service`, which adds the extensions and the roles;
- `sinex-nats.service`, NATS with JetStream under `stateDir/nats`, with one
  user per producer; its config holds only bcrypt hashes of the passwords,
  and each process reads its own password from its file, never from its
  arguments;
- `sinexd.service`, which applies the schema, makes the API tokens under
  `stateDir/tokens` once, and reads every enabled source from
  `/etc/sinex/sources.json`; it plans the sources' paths again every minute,
  so a new day's file or recording is read without a restart;
- `sinex-capture-journald.service`, when the journald source is enabled;
- the `sinex-session` user service in the operator's graphical session;
- `sinex-dump.timer`, the daily database dump into `backup.dumpDir`, and a
  Borg job when `backup.repo` is set;
- one `sinex-backfill-NAME.service` per declared backfill, never started at
  activation.

`nix/catalog.json` lists every source with its formats and purpose. A source's
`paths` must be readable by the `sinex` user; `readAnyFile` gives sinexd read
access to every file instead (it never writes them). A path with nothing to
read, and a binding that fails, is logged when it happens, once, and again
only when that changes.

PostgreSQL keeps its stock memory and checkpoint defaults. Deployments tune
these through `services.postgresql.settings`. The module sets
`log_temp_files = 16MB` to record temporary files of at least 16 MB in the
PostgreSQL log.

Activation does not restart sinexd, NATS or the database setup. After a
deploy, restart them by hand (`systemctl restart sinex-db-setup sinexd`) and
check the running binary
(`readlink /proc/$(systemctl show -p MainPID --value sinexd)/exe`) and
`sinexctl status`. Login shells get `SINEX_ENDPOINT` (the loopback HTTP API;
the Unix socket is the daemon user's own), `SINEX_TOKEN_FILE` (the agent token)
and `SINEX_PRIVATE_MODE`, so `sinexctl` talks to the daemon and
`sinexctl private on` stops every capture on the host. The operator reads the
token and sets the switch through the `sinex` group, which takes effect at
their next login. A switch that cannot be checked also stops capture: each
process logs the reason once, and `sinexctl private` and `sinexctl status`
show it.

The database has one writer. Starting a backfill
(`systemctl start sinex-backfill-NAME`) stops sinexd, reads the inputs
read-only to their end, and starts sinexd again; the session service and the
journald capture keep spooling meanwhile.

The content store (`storeDir`) and the dumps are what a backup needs; the
database itself is rebuilt from a dump (`nix/tests/restore.nix` is the drill).

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

`agentctl job start sinex verify` runs the full Rust test suite and full check in
one job, using the test suite's tmpfs scratch and each command's time limit.

`xtask test` passes its arguments to `cargo nextest run`. `-p` limits what is
built; a name or `-E` filter alone still builds the whole workspace, and within
a package `--lib`, `--bin sinexd` or `--test NAME` limits it to one target.
Each database test gets its own copy of the schema template. AgentCTL takes
`CARGO_TARGET_DIR` from the caller; give each target directory one build at a
time.

The [query guide](docs/query.md) covers typed fields, completion, filled time
buckets, marks, time citations, saved queries and context views.

See [the development guide](docs/development.md) for isolated watched runs, VM scenarios, history comparisons, coverage and fuzz regression tests.

## Benchmarks

`agentctl job start sinex bench -- run` backfills a fixed sample of real
inputs and reports source-file bytes, stored bytes per source, database size
and blob-store size, with timings and sampled memory and load. Estimates for
the full inputs assume time grows linearly with input bytes.
