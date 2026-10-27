# Development

Run builds and tests through the declared AgentCTL operations. See
`agentctl project operations sinex` for the current list and `AGENTS.md` for
publication requirements.

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
