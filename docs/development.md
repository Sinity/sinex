# Development

Run builds and tests through the declared AgentCTL operations. See
`agentctl project operations sinex` for the current list and `AGENTS.md` for
publication requirements.

## Select the build

`compile -- -p sinexctl`, `compile -- -p sinex-session` and
`compile -- -p sinex-capture` build without preparing a database. The CLI's
normal dependency graph has no SQLx or store crate. The database-free
`sinex-diagnostics` client runs `sinexd admin diagnostics` for offline
commands, which keeps database diagnostics available while the daemon is
stopped. Install `sinexd` beside the CLI to use those commands.

`sinex-source-core` owns the parser interface, drain, framing and shared
drivers. SQLite and archive readers are selected by the `sqlite` and
`archives` features. `sinex-sources` holds the source families and the live
capture loop. Its default `full` feature includes repository, SQLite, export
and network readers. Capture nodes use it with default features off; live
formats and their spool readers remain available. NATS transport has its own
TLS dependency independently of network source readers.

Sealed material handling is behind the daemon's `sealing` module. Acquisition,
archive passes and replay hand outcomes to that interface; the
`sinex-sealing` crate owns age and payload cryptography. The store owns the
checked wrapped-key queries. Existing sealing formats and access rules stay
unchanged.

## Schema database identity

Xtask builds the query-free schema executable and asks `sinex-schema hash`
for the hash of its generated DDL. That includes table definitions, enum
CHECK lists and event-type views, including the payload fields those views
expose. Xtask combines that hash with the PostgreSQL version to select the
schema database and test template. Parser, model-method and payload-value
changes that do not change DDL keep the same `DATABASE_URL`. SQLx queries
continue to compile against a real database.

External dependencies have debug information disabled. Workspace code
keeps line tables for backtraces, and the existing optimized hash packages
keep their optimization settings.

## Compare development costs

Use the same `test` and selected `compile` jobs before and after a change.
Keep queue time separate from job execution. Xtask reports database setup,
test compilation and test execution separately; AgentCTL reports the whole
job, including environment setup and resource waits. Concurrent jobs in one
worktree can wait for Cargo's target-directory lock.

Record whether the target was seeded or already warm. A dependency-profile
change invalidates compiled dependencies, so the first rebuild includes that
cost. Compare that run separately from subsequent warm runs. Preserve the
job IDs and stage timings in the PR's verification report.
