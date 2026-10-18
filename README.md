# Sinex

Sinex is a capture and provenance store. Collectors read local histories (Atuin shell history, extended zsh history, Chromium visits and downloads), and a single writer admits what they read into PostgreSQL with TimescaleDB. Sinex keeps the source material it read, so every event can be traced back to the original record. Clients query, show and trace events through `sinexctl` and an MCP server.

## What works

Sources:

- Atuin shell history, source `terminal.atuin-history`: [docs/atuin.md](docs/atuin.md)
- Extended zsh history files written append-only, source `terminal.zsh-history`: [docs/shell-grouping.md](docs/shell-grouping.md#zsh-source)
- Chromium `History` visits and persisted downloads, source `browser.history.chromium`: [docs/chromium.md](docs/chromium.md)

Features:

- Raw event queries with paging: [docs/queries.md](docs/queries.md)
- Indexed search over shell commands with project and time filters, plus seven MCP read tools: [docs/search.md](docs/search.md)
- Grouping of matching Atuin and zsh records into one canonical command: [docs/shell-grouping.md](docs/shell-grouping.md)
- Shell sessions and `recall last`: [docs/sessions.md](docs/sessions.md)
- Source status, acquisition history and record errors (`sources`): [docs/sources.md](docs/sources.md)
- Replay of one stored Atuin material through the current parser: [docs/replay.md](docs/replay.md)
- Reversible exclusion of one Atuin occurrence: [docs/exclusion.md](docs/exclusion.md)
- Credential masking on every response: [docs/credential-output.md](docs/credential-output.md)
- Schema bootstrap, checks and guarded upgrades: [docs/schema.md](docs/schema.md)

Not implemented yet: other sources, automata other than `session`, the general query language, and zsh or Chromium replay and exclusion.

## Build

The Nix flake pins the toolchain and PostgreSQL 18 with TimescaleDB. Build the admin-only daemon first; it needs no database and has no `serve` command:

```sh
nix develop --command cargo build -p sinexd --no-default-features
```

The full daemon compiles its SQL queries against a live database schema (there is no offline SQLx cache). Point `DATABASE_URL` at a development database that already has the schema applied, then build normally. `nix build .#default` bootstraps its own temporary database inside the build.

## Database

Sinex needs PostgreSQL 18 with TimescaleDB 2.27.2 preloaded, data checksums enabled, `timescaledb.enable_chunk_skipping=on`, and unprivileged `sinex_writer` and `sinex_reader` login roles. Put the administrative connection URI in a mode-0600 file, then create or check the schema:

```sh
target/debug/sinexd admin schema --connection-file /abs/admin-connection
target/debug/sinexd admin schema --connection-file /abs/admin-connection --check
```

`--check` only inspects. Upgrading a populated database needs `--apply --dump-file`; see [docs/schema.md](docs/schema.md).

## Configure and run

```toml
listen = "127.0.0.1:41037"
state_dir = "/abs/state"
store_dir = "/abs/blobs"
writer_connection_file = "/abs/writer-connection"
reader_connection_file = "/abs/reader-connection"
token_file = "/abs/token"
sources = []
automata = []
```

Connection files hold PostgreSQL URIs and the token file holds the bearer token for read methods. Paths must be absolute, `listen` must be a loopback address with an explicit port, and unknown keys are rejected. Optional keys:

- `operator_token_file`: a second, different token that allows replay and exclusion.
- `read_model_connection_file` and `[[projects]]`: indexed search ([docs/search.md](docs/search.md)).
- `[atuin]`, `[zsh]`, `[[chromium]]`: source settings, each matching an entry in `sources`.
- `automata = ["session"]`: shell sessions ([docs/sessions.md](docs/sessions.md)).
- `[[privacy_rules]]`: regex replacements applied once at admission (Atuin only, up to 32 rules).

```sh
sinexd config schema                       # print the configuration JSON schema
sinexd config check --config /abs/config.toml
sinexd serve --config /abs/config.toml
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token status --json
```

`sinexctl` always needs `--endpoint` and `--token-file` before the subcommand. Other commands: `version`, `config-show`, `catalog`, `sources`, `query`, `search`, `show`, `trace`, `recall`, `replay`, `replay-status`, `replay-cancel`, `delete` and `mcp`. SIGINT or SIGTERM stops accepting requests and drains the writer; stored state is kept.

## Query quick start

Pass the whole query as one shell argument:

```sh
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token query 'events' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token query 'events where source = terminal.atuin-history and kind = command.executed' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token query 'events where payload.command ~ "cargo test"' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token query 'commands where cargo and exit_code != 0 sort when desc limit 20' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token search cargo test --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token show EVENT_UUID --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /abs/token trace EVENT_UUID --bytes --json
```

`events` queries read stored events directly in pages of 100; continue with `--cursor`. `commands` queries and `search` need the indexed projection. `query --help` lists the supported forms. Exit codes: 0 success, 2 bad query, 3 not found, 4 refused, 5 partial or incomplete coverage, 6 deadline exceeded, 1 internal error.

## Development

```
nix develop --command cargo run -p xtask -- check     # fmt, dependency boundaries, cargo check; no database
nix develop --command cargo run -p xtask -- test      # all Rust tests in an owned PostgreSQL
nix develop --command cargo run -p xtask -- scenario NAME|all [--package DIR]
                                                      # end-to-end scenarios, tests/scenarios/NAME.py
nix develop --command cargo run -p xtask -- db ensure # build the schema database; print DATABASE_URL
nix develop --command cargo run -p xtask -- db exec -- CMD # run CMD with DATABASE_URL, PGHOST and PGPORT set
nix develop --command cargo run -p xtask -- status    # failing tests and errors from this checkout's last run
nix build .#default                                   # the package; runs the Rust tests in the builder
```

Tests run under cargo nextest. Every xtask run is recorded in the machine-wide
history store (`$XDG_STATE_HOME/devkit/history.db`); `xtask history failures`
and `xtask history diagnostics` show a run's failing tests and compiler
messages.

The database comes from devkit: the per-user PostgreSQL service when it runs,
otherwise a throwaway cluster on tmpfs removed afterwards. Each schema version
is built once into a template database; every database test gets its own copy
of it, kept only if the test fails. Scenarios run the
real daemon, CLI and MCP server against synthetic sources; a failed scenario
leaves its logs in `target/scenario-failures/`. The same commands are AgentCTL
operations (`check`, `test`, `scenario`, `package`). The pre-push hook runs
`cargo fmt --check` and `xtask boundaries` ([dependency rules](docs/dependency-boundaries.md)).
See `AGENTS.md` for the development loop.

The shared blob store comes from Polylogue's [`hashed-blobs` v0.1.1 release](https://github.com/Sinity/polylogue/tree/hashed-blobs-v0.1.1/crates/hashed-blobs); `Cargo.lock` pins its commit and the Nix build checks its source hash.
