# Sinex foundation

This is an isolated rewrite foundation, not a deployed release or completed M0. It provides canonical schema control, role-bound PostgreSQL ownership and an Atuin-only source path through retained row evidence, the single writer and normal daemon/CLI queries. MCP, other sources, automata, replay and confirmed delivery remain unavailable. See the [implemented Atuin subset](docs/rewrite/atuin.md) for its limits.

The [foundation contract](docs/rewrite/README.md) records source disposition and unimplemented criteria. [Shared dependency provenance](vendor/hashed-blobs/UPSTREAM.json) identifies the provisional reviewed library snapshot and canonical adaptation; release pinning remains open.

## Build and inspect

The Nix lock pins the compiler and package environment. The checked SQLx queries compile against an actual isolated PostgreSQL schema. Atuin admission and constructed queries execute against that same schema in acceptance. Build the query-free admin first:

```
nix develop --command cargo build -p sinexd --no-default-features
```

That command does not need a database. `sinexd config schema` and `sinexd catalog` likewise need no daemon. The admin-only binary explicitly refuses `serve`.

Provision an isolated PostgreSQL 18.4 database with TimescaleDB 2.27.2 preloaded, data checksums enabled, `timescaledb.enable_chunk_skipping=on`, and unprivileged `sinex_writer` and `sinex_reader` login roles. Put its administrative connection URI in a mode-0600 file, then apply/check the canonical schema with:

```
target/debug/sinexd admin schema --connection-file /absolute/path/admin-connection
target/debug/sinexd admin schema --connection-file /absolute/path/admin-connection --check
```

Schema checks reject incompatible definitions or unsafe grants without resetting data. The event stores use the canonical row shape, UUIDv7 partitions, occurrence side tables and logical archive view. Their declared indexes, guards, compression policy and sparse indexes are checked by definition. The Atuin command payload and source are registered; general event admission and upgrades from earlier preparation schemas remain unimplemented. Set `DATABASE_URL` only to this development schema when building the runtime through `xtask` or Cargo. No offline SQLx cache is used. `nix build .#default` performs its own query-free bootstrap within an isolated builder before compiling checked product queries.

## Configure and run

```
listen = "127.0.0.1:41037"
state_dir = "/absolute/path/foundation-state"
store_dir = "/absolute/path/foundation-blobs"
writer_connection_file = "/absolute/path/writer-connection"
reader_connection_file = "/absolute/path/reader-connection"
token_file = "/absolute/path/token"
sources = []
automata = []
```

Connection files hold PostgreSQL URIs; the token file holds the bearer token. Diagnostics and public config never emit their contents. Configuration requires absolute paths and loopback binding, rejects unknown keys, and does not inherit connection overrides from environment variables. The token authorizes read RPC methods. Configured sources submit their acquired rows directly to the same private writer owner.

```
sinexd config check --config /absolute/path/config.toml
sinexd serve --config /absolute/path/config.toml
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token status --json
```

The client requires its explicit endpoint and has no installed-service fallback. SIGINT/SIGTERM stops request admission, drains the owned writer and releases locks. An occupied endpoint fails before acquiring database/state ownership. Persistent state is never deleted by daemon shutdown.

## Verification

Use declared AgentCTL `foundation_build`, `foundation_acceptance`, `foundation_package`, and `foundation_package_acceptance` operations from this checkout. `foundation_format` formats owned workspace code before qualification. The acceptance runner owns a fresh PostgreSQL cluster and launches real built entrypoints; `foundation_package_acceptance` explicitly selects the Nix package binaries; it records command exits, raw output and scenario results in private scratch. Failures still emit `result.json` and return nonzero. It never targets production services.

`atuin_acceptance` and `atuin_package_acceptance` exercise retained synthetic Atuin through actual daemon and CLI processes. The original real-role integration test separately exercises event constraints, compression and definition-drift refusals; its synthetic proof API is not a source route. Exact package/build/client and operational criteria remain open until their reported checks actually pass. No source replacement PR, host activation, consumer update or external task migration is performed by these commands.
