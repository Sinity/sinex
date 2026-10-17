# Sinex foundation

This is the published rewrite foundation, with no deployment implied by source publication. It provides canonical schema control, role-bound PostgreSQL ownership, retained Atuin and explicitly declared append-only extended zsh acquisition through one writer, and ordinary query/show/trace clients. The [retained Atuin replay subset](docs/rewrite/retained-replay.md) reparses one closed logical-row material with durable action recovery. The [indexed shell retrieval subset](docs/rewrite/indexed-retrieval.md) adds ranked search, project/time scope and seven read tools over `rmcp`. The [selected shell grouping subset](docs/rewrite/shell-fusion.md) connects exact Atuin/zsh parents to actual canonical evidence. The [selected shell session subset](docs/rewrite/shell-sessions.md) adds writer-owned logical-day replacement and project-scoped `recall last`. Other sources, general fusion/automata, confirmed delivery and full M1 acceptance remain unavailable. See the [Atuin contract](docs/rewrite/atuin.md) for its source limits.

The [foundation contract](docs/rewrite/README.md) records source disposition and unimplemented criteria. The shared file layer comes from Polylogue’s [hashed-blobs-v0.1.1 release](https://github.com/Sinity/polylogue/tree/hashed-blobs-v0.1.1/crates/hashed-blobs). Cargo.lock pins the owner commit and the Nix build checks its source hash; no vendor fallback is present.

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

Schema checks reject incompatible definitions or unsafe grants without resetting data. The event stores use the canonical row shape, UUIDv7 partitions, occurrence side tables and logical archive view. Their declared indexes, guards, compression policy and sparse indexes are checked by definition. The Atuin command payload and source are registered; general event admission remains unimplemented. The [guarded published-schema upgrade](docs/rewrite/schema-upgrade.md) supports the exact published Atuin predecessor, preserves retained event evidence and requires an explicit dump before applying ordered changes. Earlier preparation schemas remain unsupported. Set `DATABASE_URL` only to this development schema when building the runtime through `xtask` or Cargo. No offline SQLx cache is used. `nix build .#default` performs its own query-free bootstrap within an isolated builder before compiling checked product queries.

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

Connection files hold PostgreSQL URIs; the token file holds the bearer token. Diagnostics and public config never emit their contents. Configuration requires absolute paths and loopback binding, rejects unknown keys, and does not inherit connection overrides from environment variables. The token authorizes read RPC methods; optional `operator_token_file` enables the fixed operator profile for replay and reversible exclusion. Configured sources submit their acquired rows directly to the same private writer owner.

```
sinexd config check --config /absolute/path/config.toml
sinexd serve --config /absolute/path/config.toml
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token status --json
```

The client requires its explicit endpoint and has no installed-service fallback. SIGINT/SIGTERM stops request admission, drains the owned writer and releases locks. An occupied endpoint fails before acquiring database/state ownership. Persistent state is never deleted by daemon shutdown.

## Reversible exclusion

`delete RAW_REF [--action-id UUID]` excludes one retained Atuin occurrence; `delete --undo RAW_REF [--action-id UUID]` removes only that exclusion. Both require `operator_token_file` through the explicit client token option. The original bytes and archived interpretations remain inspectable. Undo itself does not restore an answer; retained replay or a later normal acquisition can admit it again. See the [supported exclusion contract](docs/rewrite/reversible-exclusion.md) for stable keys, exact retry outcomes, refusals and qualification limits.

## Query quick start

`query --help` lists supported examples. Pass the complete query as one shell argument after the endpoint and token options:

```sh
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token query 'events' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token query 'events where source = terminal.atuin-history and kind = command.executed' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/path/token query 'events where payload.command ~ "cargo test"' --json
```

These [raw event reads](docs/rewrite/atuin.md) work without an indexed projection. Pages contain up to 100 rows; reuse the same query with `--cursor CURSOR` from the response to continue. Raw queries support the documented conjunction subset, without `limit` or `sort` clauses.

For [indexed retrieval](docs/rewrite/indexed-retrieval.md), `query 'commands where cargo and exit_code != 0 sort when desc limit 20'` selects canonical commands and requires a configured, caught-up projection. An unavailable projection returns `partial` (CLI exit 5). Indexed `limit` is a query clause; pipeline forms such as `events | limit 20`, `commands | limit 20`, and `events where source = "terminal.atuin-history" | limit 20` are unsupported and return `bad_query`. The full general query language remains unavailable.

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
SQLite snapshots, the full projection pruning/purge lifecycle, formal M1 acceptance and task-routing
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

Registered readers enforce the fixed credential presentation floor independently of optional admission rules. [Credential output scope and refusal semantics](docs/credential-output.md) describe logical strings, opaque identities, row flags and qualification.

`indexed_retrieval_package_acceptance` qualifies indexed shell retrieval through actual packaged daemon/API/CLI and rmcp stdio clients. The [owning indexed retrieval contract](docs/rewrite/indexed-retrieval.md) defines configuration, registered query shapes, ranking/snippets, canonical evidence, projection progress, interruption/reconstruction controls, and remaining full-product criteria. `indexed_retrieval_acceptance` is its focused development route. Existing raw-event, source, replay and output-floor evidence remains separate.

For a separately built clean exclusion candidate, `exclusion_package_acceptance -- --package-pin /absolute/package-pin.json` reuses the explicitly recorded immutable package and exact binary hashes. It never builds or selects a development fallback. Its connected client, provider-loss and real transaction-boundary controls are described in the [exclusion contract](docs/rewrite/reversible-exclusion.md); passing the internal `exclusion_contracts` route alone does not qualify them.


`fusion_acceptance -- --fusion-spec /absolute/spec-repository` and `fusion_package_acceptance` qualify the [selected Atuin/zsh lifecycle](docs/rewrite/shell-fusion.md) against pinned independent expectations, original retained record bindings, real ordinary clients and bounded writer interruptions. They do not access private or live history.

`configured_continuation -- --package-pin /absolute/qualification-pin.json` measures the [finite ongoing Atuin continuation](docs/rewrite/configured-continuation.md) through an unchanged pinned package, one configured collector and ordinary project-scoped CLI/MCP. Later producer batches, autonomous repeats, same-state restart catch-up and unavailable-versus-empty evidence retain exact facts, citations, timings, storage growth and interventions. It adds no product feature or daily-scalability claim and reuses earlier qualifications under their original bindings.

`xtask boundaries [workspace]` checks the actual locked Cargo metadata graph without a database. Build, test and check frontends, the Nix package build and the pre-push hook run it. `dependency_boundaries` is the declared focused qualification operation; it tests the checker and alters copied native manifests to prove forbidden normal, renamed, optional, development, build and foreign-target edges fail, along with PostgreSQL ownership, pure-core, unknown-package and malformed-manifest controls. Cargo resolution and execution errors fail the command.

The [dependency boundary contract](docs/rewrite/dependency-boundaries.md) defines the implemented package subset and provider SQLite seam.

`session_acceptance -- --session-predecessor-pin FILE` qualifies native retained shell sessions and project-scoped CLI/MCP recall, including a guarded upgrade from the exact current published predecessor. See [the selected session contract](docs/rewrite/shell-sessions.md) for its fixed profile, bounds and unknown evidence.

`privacy_rule_acceptance -- --packaged` builds and qualifies optional admission-once rules through actual query, indexed query, search, show, retained trace and replay clients in an isolated store. Non-idempotent literal replacements, idempotent controls, a credential-shaped replacement, provider removal, changed current rules, old plain evidence, zsh retained-original protection, opaque raw-selector refusal, archived interpretations and stable action/provenance bindings distinguish admission from the fixed output floor. `-- --package-pin FILE` reuses an explicitly bound immutable package when its runtime source is unchanged. Full privacy administration and installed behavior remain outside this qualification.

The [configured Atuin investigation subset](docs/rewrite/source-investigation.md) adds bounded ordinary source history, explicitly observed settlement/scheduling metadata and exact acquisition interpretation receipts. `source_investigation_acceptance` exercises development clients; `source_investigation_package_acceptance -- --package-pin FILE` consumes only an explicitly bound candidate package. Historical coverage, event-quiet notification and installed behavior remain unavailable.

`recovery_context_acceptance` extends the [selected session recall](docs/rewrite/shell-sessions.md)
with all eligible co-occurring context, own-cwd attribution, direct-member summaries
and newer unresolved session candidates under one bounded generation.
`recovery_context_package_acceptance -- --package-pin FILE` uses the explicitly
pinned package without rebuilding or choosing a fallback.

The recovery-context route also accounts for three completed fixed-provider
Atuin acquisitions at the existing `collector-scheduled` boundary. That
rendezvous holds the first baseline; two later autonomous polls use the same
continuous collector and a three-second interval. Each completed sample binds
source counters, event/session identities, material/blob custody and a full
store-file manifest. Equal observation/cursor/material basis before and after
reads refuses a raced sample; directory reads do not claim filesystem atomicity. The private report retains the exact plain
blobs, stock `zstd` decoding, source JSONL, `_from` serialization overhead and
encoded file lengths. The owner decoder does not separately expose verified
seek-metadata lengths; that finer split stays unknown. Open and temporary copies
must be absent at these closed boundaries. `tests/retention_accounting_checks.py` checks exact byte attribution
and forbidden runtime/build-delta controls without a database.

For a clean harness-only recovery candidate, the package pin may additionally
record `package_candidate` and `package_tree` for the original qualified runtime.
The ordinary clean `candidate`/`tree` and immutable binary hash checks still
apply to the current qualification candidate. Reuse refuses changes outside
`tests/`, `docs/`, `README.md` and `.agentctl/project.toml`; the report preserves
both source identities and their exact diff. This route requires an existing
`zstd` command and never builds a replacement runtime. It measures finite file
lengths across completed polls, not physical IO,
allocated storage, RSS, sustained capacity or everyday benefit. It cannot
retrospectively decompose older aggregate byte samples without their manifests.

For one finite repeated recovery journey, pass
`recovery_context_package_acceptance -- --package-pin FILE --recovery-prior-accounting /absolute/prior/result.json`.
The prior successful report must bind the same immutable runtime binaries and
retain its three-poll accounting. The journey reuses that evidence and does not
repeat the accounting, overflow or current-parent boundary controls. It asks
the same CLI/MCP project-recovery question after baseline, completed unchanged
polling, a late bridge, a later arrival, a compatible restart and provider-path
loss. While provider access exists, independently assembled read-only SQLite
facts check the returned membership, own-context qualifications and citations.
A controlled synthetic SQLite lock additionally brackets a question with an
actual `reading` observation; a scheduled future poll is never called active
reading. During that control SQLite access is also explicitly blocked. Held-out newer
invalid-path and prefix-neighbor sessions distinguish positive project selection
from unresolved or unconfigured context.

Private reports separate public question traffic from polling/validation,
record producer-to-validated-answer waits and explicit interventions, and retain
DB sizes, file lengths and named process CPU/RSS observations. `workspace_total`
includes PostgreSQL, retained blobs and provider/runtime fixtures; it overlaps
component figures and must not be added to them. Scratch-only growth is unknown
because the fixture has no separate scratch directory. The final
`whole-loop-rusage.json` covers the harness and children Linux accounts through
wait/reaping, through child/PostgreSQL shutdown. Managed-environment setup, final
report serialization and successful workspace deletion are excluded. It does not give inclusive descendant CPU,
physical IO or simultaneous aggregate peak RSS. Per-process `VmHWM` and Linux
`ru_maxrss` are process-lifetime individual high-water statistics and may predate
the measurement interval; named samples are not a
continuous workload peak. `tests/recovery_whole_loop_checks.py` checks these
measurement boundaries, prior-receipt refusals and front-door controls. This
finite scripted journey establishes no everyday productivity or resource
advantage, sustained capacity, complete history or full milestone.
