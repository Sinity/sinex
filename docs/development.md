# Development commands

Run builds, tests and development commands through AgentCTL from this checkout. Use the checkout path as the project argument to load its operation declarations, including operations that have not reached the main checkout yet. `agentctl project operations sinex` lists the operations. Each xtask run records its resolved arguments, stages and outcome; `xtask history explain RUN` shows stage timings, slow tests and overlapping recorded runs. Queue waits and device resources remain in AgentCTL, linked by job ID when recorded. An overlap alone does not establish resource contention.

## Full tests

The `test` operation gives the full suite tmpfs scratch; `exec` uses NVMe scratch. `NEXTEST_TEST_THREADS=1 agentctl job start /path/to/checkout test --workspace /path/to/checkout` selects one nextest thread while keeping the full test set and per-test limits. The resolved thread option is recorded with the run, so history comparisons keep different concurrency settings separate. Explicit nextest thread options take precedence over the environment. See [nextest's concurrency settings](https://nexte.st/docs/running/).

## VM checks

`agentctl job start /path/to/checkout vm --workspace /path/to/checkout -- --output /path/to/new/results` runs `module`, `vm-stack`, `vm-topology` and `vm-restore`. Repeat `--scenario vm-stack` to choose exported checks. Discovery must find every requested scenario. Commit all source files first: VM commands refuse uncommitted changes, including new files, so Nix and the run record refer to the same input. Each scenario writes its original build log and an entry in `results.json`; all selected scenarios run even when one fails. AgentCTL owns cancellation of the job's process tree.

## Development stack

`agentctl job start /path/to/checkout development --workspace /path/to/checkout -- --watch --output /path/to/new/private-output` starts its own PostgreSQL cluster, clones the schema into a development database, starts its own NATS server and builds and runs the daemon. It never selects the host's PostgreSQL or NATS services. The development operation holds no cargo capacity slot while idle. Only one development stack can own a checkout at a time. Its development lock is released when the process exits, including cancellation. The output directory must be new. Content, tokens and optional session spools live there; it is private and remains available after the run. The database and NATS data are temporary and are removed on orderly shutdown. Native cancellation can leave their temporary directories; the shared service helpers remove directories whose owner has exited on the next start. The API and optional session integration sockets live in the owned cluster's short socket directory, separate from the operator's sockets. `runtime.json` records their paths and owned process IDs.

Readiness requires an HTTP 200 response from the development API. `--watch` hashes source contents, including new and removed files, waits briefly for successive edits, rebuilds, stops the previous process groups and starts the new build. A failed rebuild prints its error and leaves the previous build running; the run records that failed build. Edits arriving during a build trigger another build. It prints change-detected-to-ready time, including the edit debounce, separately from build and service setup stages. Source detection polls every 100 ms. `--seconds SECONDS` bounds a smoke observation; otherwise cancel by the recorded AgentCTL job ID.

There are no default source bindings or session captures. `--sources /path/to/bindings.json` selects daemon bindings. Repeat `--session-source clipboard` (or `hyprland`, `notifications`, `players`, `integrations`) to start those session captures explicitly. The development runner writes a session sources file that preserves configured exclusions and excludes every unselected session capture declared by the catalog, including media and file watches. It passes that file through `--sources`. Treat resulting captures and tokens as private data. The watcher excludes its output directory.

## History comparisons

`xtask history compare BEFORE AFTER` requires passed runs with the same command, exact arguments, host and Rust toolchain. It reports duration changes, stages, slow tests and overlapping runs. A focused test is a different scope from a workspace suite. `xtask history compare-days YYYY-MM-DD YYYY-MM-DD` groups UTC days by those same fields and reports changes only for matching groups. Missing measurements stay null. Parallel test durations are listed separately, never summed into wall time. No default regression threshold is imposed.

## Coverage

`agentctl job start /path/to/checkout coverage --workspace /path/to/checkout -- --output /path/to/new/coverage --format html -- -p sinex-model` measures the selected package with LLVM instrumentation in a separate build directory. Formats are `html`, `lcov` and `json`. Each run writes `coverage.json` with the full LLVM data and `summary.json` with the exact arguments, line percentage and function regions mapped through each function's file table. `--threshold PERCENT` optionally fails below a caller-selected line percentage. Missing reports, malformed totals, no matching tests and failed children are errors. Output directories cannot be reused; no broad cleanup or automatic browser opening occurs.

## Fuzz regressions

The full `xtask test` gate replays synthetic inputs for manifest and producer-frame round trips, query parsing and the admission path's COPY encoder, including the saved overflowing-integer manifest input. These tests run without sanitizers. COPY cases compare values stored by PostgreSQL against the input, including PostgreSQL's rejection of NUL text. `xtask check` formats and checks the same harness in `fuzz/`. Test concurrency options preserve this regression gate.

Run `agentctl job start sinex check --workspace /path/to/checkout` and `test` on the exact PR head before integration. Shared runners live in devkit; this checkout supplies only Sinex schema setup, scenario names and daemon/session arguments.

## Select the build

`compile -- -p sinexctl`, `compile -- -p sinex-session` and
`compile -- -p sinex-capture` build without preparing a database. The CLI's
normal dependency graph has no SQLx or store crate. The database-free
`sinexctl::diagnostics` module runs `sinexd admin diagnostics` for offline
commands, which keeps database diagnostics available while the daemon is
stopped. Install `sinexd` beside the CLI to use those commands.

`sinex-source-core` owns the parser interface, drain, framing and shared
drivers. SQLite and archive readers are selected by the `sqlite` and
`archives` features. `sinex-sources` holds the source families and the live
capture loop. Its default `full` feature includes repository, SQLite, export
and network readers. Capture nodes use it with default features off; live
formats and their spool readers remain available. NATS transport has its own
TLS dependency independently of network source readers.

The daemon's `material_access` module stores and opens material bytes. Its
`sealing` submodule handles data keys and record encryption; `sinex-sealing`
owns age and payload cryptography. The store owns the checked wrapped-key
queries.

## Schema database identity

Xtask builds the query-free schema executable and asks `sinex-schema hash`
for the hash of its generated DDL. That includes table definitions, enum
CHECK lists and event-type views, including the payload fields those views
expose. Xtask combines that hash with the PostgreSQL version to select the
schema database and test template. Parser, model-method and payload-value
changes that do not change DDL keep the same `DATABASE_URL`. SQLx queries
continue to compile against a real database.

External dependencies have debug information disabled. Workspace code
keeps line tables for backtraces. Hash packages use the optimization settings
in `Cargo.toml`.
