# Sinex foundation

This is an isolated rewrite foundation, not a deployed release or completed M0. It provides canonical schema control, role-bound PostgreSQL ownership and an Atuin-only source path through retained row evidence, the single writer and normal daemon/CLI queries. The [retained Atuin replay subset](docs/rewrite/retained-replay.md) reparses one closed logical-row material with durable action recovery. MCP, other sources, automata and confirmed delivery remain unavailable. See the [implemented Atuin subset](docs/rewrite/atuin.md) for its limits.

The [foundation contract](docs/rewrite/README.md) records source disposition and unimplemented criteria. The shared file layer comes from Polylogue’s [hashed-blobs-v0.1.0 release](https://github.com/Sinity/polylogue/tree/hashed-blobs-v0.1.0/crates/hashed-blobs). Cargo.lock pins the owner commit and the Nix build checks its source hash; no vendor fallback is present.

## Build and inspect

The Nix lock pins the compiler and package environment. A separate Rust input supplies the shared release’s tested Rust 1.98.1 minimum while preserving the existing PostgreSQL/Timescale package pin. The checked SQLx queries compile against an actual isolated PostgreSQL schema. Atuin admission and constructed queries execute against that same schema in acceptance. Build the query-free admin first:

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

Connection files hold PostgreSQL URIs; the token file holds the bearer token. Diagnostics and public config never emit their contents. Configuration requires absolute paths and loopback binding, rejects unknown keys, and does not inherit connection overrides from environment variables. The token authorizes read RPC methods; optional `replay_token_file` enables the separate fixed operator profile described in the replay contract. Configured sources submit their acquired rows directly to the same private writer owner.

```
sinexd config check --config /absolute/path/config.toml
sinexd serve --config /absolute/path/config.toml
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token status --json
```

The client requires its explicit endpoint and has no installed-service fallback. SIGINT/SIGTERM stops request admission, drains the owned writer and releases locks. An occupied endpoint fails before acquiring database/state ownership. Persistent state is never deleted by daemon shutdown.

## Verification

Use declared AgentCTL `foundation_build`, `foundation_acceptance`, `foundation_package`, and `foundation_package_acceptance` operations from this checkout. `foundation_format` formats owned workspace code before qualification. `foundation_lock` takes explicit Cargo update selectors (for example, `-- --workspace`) to preserve unrelated locked dependencies. `foundation_publish` takes explicit Git push arguments and runs the normal hooks; source publication and deployment remain separate decisions. The acceptance runner owns a fresh PostgreSQL cluster and launches real built entrypoints; `foundation_package_acceptance` explicitly selects the Nix package binaries; it records command exits, raw output and scenario results in private scratch. Failures still emit `result.json` and return nonzero. It never targets production services.

`atuin_acceptance` and `atuin_package_acceptance` exercise retained synthetic Atuin through actual daemon and CLI processes. The original real-role integration test separately exercises event constraints, compression and definition-drift refusals; its synthetic proof API is not a source route. Exact package/build/client and operational criteria remain open until their reported checks actually pass. No source replacement PR, host activation, consumer update or external task migration is performed by these commands.


The `kestrel_demo` operation takes the read-only specification Git repository as its
required argument (after `--`). It runs the unchanged preparer and nanosecond-exact
oracle pinned to `ba29767740bc70fbb8eaffcfd99127ceb1e7e4ea`, selecting packaged
binaries with no installed-service fallback. It first requires real candidate
launch failure and deliberately corrupted captured-answer runs to fail with
retained diagnostics, then exercises base, repeated, same-path append, empty and
malformed Kestrel inputs through actual source admission and CLI queries. It
reuses the foundation harness's isolated PostgreSQL owner and result store;
unchanged foundational regression qualification is separate. `xtask demo
<workspace> <spec-repository> [--controls] [--boundaries]` is the corresponding frontend and
requires explicit source paths, including when xtask itself is packaged.

Each run retains exact prepared/exercised synthetic inputs, oracle source and
digests, expected and actual answers, command exits, source observations and
owned process logs. `comparisons.json` and `diagnostic.txt` identify expected and
actual values; `result.json` marks dependent cases unrun on failure. State is
removed only after successful evidence capture; failed owned state is retained
with PostgreSQL stopped. The reporting controls qualify failed-run diagnostics. The connected boundary
operation below qualifies observed retention/admission process kills. Physical
SQLite snapshots, a general read model/index, formal M1 acceptance and task-routing
gates remain open.


`kestrel_boundary_acceptance` takes the same required specification-repository
argument and adds actual writer-boundary recovery to the packaged demonstration.
The driver first records an uninterrupted malformed-fixture baseline, then uses
an explicit one-shot Unix-socket rendezvous at `retained-fsynced`,
`transaction-staged` and `transaction-committed`. It receives the actual writer's
PID, material/range, observation and progress evidence before killing only that
recorded child with SIGKILL. There are no elapsed-time kill triggers. The same
packaged candidate restarts with its unchanged source, database and state;
normal queries, sources and retained traces establish recovered facts, accounting,
provenance and repeat suppression against the baseline. `boundaries.json` holds
the observed frames and kill outcomes in the existing run report.

The daemon opt-in is `serve --config CONFIG --qualification-boundary POINT
--qualification-socket ABSOLUTE_SOCKET`. Both arguments are required together.
Only one matching instruction rendezvouses; without these arguments the normal
writer runs directly. The controller sends `c` to continue or kills its recorded
child. A missing controller, disconnect, refusal or timeout fails that admission;
it never silently crosses the requested boundary. This control can hold a real
transaction and is only intended for owned isolated qualification. See
[Atuin durability and source diagnostics](docs/rewrite/atuin.md).


Report-owned artifact references are relative to their owning report directory,
including nested query captures and expected/actual diagnostics. The driver checks
a copied report tree and refuses a deliberately restored absolute scratch link
before succeeding, so archival remains inspectable after
scratch cleanup. Exact executed argv, configuration/source coordinates and raw
stdout/stderr keep their original values as execution evidence.


`kestrel_provider_loss_acceptance` takes the same required specification Git
repository argument and qualifies retained logical-row reads after observed
precommit/postcommit kills and provider disappearance. See the owning
[Atuin provider-loss contract](docs/rewrite/atuin.md#retained-reads-without-the-provider)
for the assertions and limits. Run it with:

```sh
agentctl job start /absolute/checkout kestrel_provider_loss_acceptance --workspace /absolute/checkout -- /absolute/spec-repository
```

The corresponding frontend is `xtask demo <workspace> <spec-repository>
--provider-loss`. This mode reuses the separately recorded normal, three-boundary
and reporting-control qualification; it cannot combine with the other demo modes.


`live_cursor_acceptance` qualifies packaged raw-event current-state continuation,
expiry/context refusals and compatible restart against an independent synthetic
fixture. See the [owning continuation contract](docs/rewrite/atuin.md#raw-event-continuation)
for the concrete page fields, fixed 100-row size and source-coverage distinction.

`read_lifecycle_acceptance` observes packaged client disconnects and query deadlines under a real owned PostgreSQL lock. The [read lifecycle contract](docs/rewrite/atuin.md#raw-event-read-deadlines-and-cancellation) describes API/CLI defaults, configuration, typed errors and the currently qualified scope.


`retained_replay_acceptance` qualifies the packaged retained-material replay subset, including a changed historical interpretation, restored omission, provider removal, bounded replacement kills, automatic owner continuation, current authorization, cancellation and refusal controls. The [owning replay contract](docs/rewrite/retained-replay.md) defines direct run, optional preview basis, request identity, progress/effect accounting and unsupported shapes.
