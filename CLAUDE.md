# CLAUDE.md

## CORE PRINCIPLES

STARTUP:

- nix develop (ALWAYS first, sets up environment)

WORKFLOW:

- Research before implementing
- ALWAYS use `just` commands (not raw cargo commands)
- TodoWrite for multi-step work
- Atomic commits with clear messages

MAGIC WORKFLOW (TRUE BACKGROUND COMPILATION):

THE ULTIMATE SETUP:
1. `cd project/sinex` - Daemon starts automatically via .envrc
2. Edit files - Daemon detects and compiles in background
3. `just tf` - Tests run instantly (already compiled!)

BACKGROUND DAEMON:
- Starts automatically when entering directory
- Watches all Rust files every 2 seconds
- Compiles changes immediately in background
- No manual compilation ever needed!

DAEMON COMMANDS:
- `just ds` - Show daemon status
- `just de` - Show current errors  
- `just dl` - Watch daemon logs
- `just daemon-start` - Start manually if needed
- `just daemon-stop` - Stop daemon

HOW IT WORKS:
- Daemon runs: `cargo check --workspace --all-targets`
- Keeps compilation fresh at all times
- Tests/runs start instantly - no compile delay
- Errors available immediately via `just de`

WORKFLOW EFFICIENCY:
1. Edit code
2. Run tests/commands - they start instantly!
3. Check errors if needed with `just de`
4. That's it - compilation is never a concern

STATUS AT A GLANCE:
- On directory entry: Shows if compilation has issues
- `just ds`: Current compilation status
- `/tmp/sinex-daemon.log`: Full compilation history

## ARCHITECTURE

FLOW: Satellites→ingestd(gRPC)→PostgreSQL+Redis→Automata→Synthesis
SATELLITES: fs-watcher, terminal, desktop, system (ingestors) + command-canonicalizer, health-aggregator (automata)
KEY_PATHS: /run/sinex/ingest.sock, postgresql:///sinex_dev?host=/run/postgresql, redis://localhost:6379

MODERN PATTERNS:

- sinex-error: canonical error handling crate
- QueryBuilder: database abstraction (never raw SQL)
- TestContext + #[sinex_test]: required for all database tests
- Environment-only config: NixOS→env vars (no file-based config)
- exo CLI: primary user interface (satellite CLIs are used only internally)

## NAVIGATION

event_types: crate/sinex-events/src/lib.rs
new_satellite: copy existing, implement StatefulStreamProcessor, add to flake.nix
config: crate/sinex-satellite-sdk/src/config.rs (environment-only)
checkpoints: crate/sinex-satellite-sdk/src/checkpoint.rs
validation: crate/sinex-ingestd/src/validation.rs
database_queries: crate/sinex-db/src/queries/ (OperationQueries pattern)
error_handling: crate/sinex-error/src/lib.rs (canonical error crate)

## INVARIANTS

- JSON schema validation on all payloads
- SQLX offline mode (commit .sqlx/)

## COMMANDS

dev: just dev (fmt, check, test-fast)
db: just migrate, just psql, just db-reset
test: just test-fast, just test-unit, just test-integration
run: just {ingestd,gateway,fs-watcher}
debug: RUST_LOG=debug command

## PATTERNS

RUST:

- ErrorContext over format! for errors
- #[sinex_test] for DB tests (auto rollback)
- EventFactory for event creation
- ValidationChain for input validation
- ULID→UUID for PostgreSQL (ulid_to_uuid(), uuid_to_ulid)

SEARCH:

- rg "impl.*Type" -t rust
- rg "function\(" -t rust -A 2
- cargo check 2>&1 | tee /tmp/err

## CORE ABSTRACTIONS

StatefulStreamProcessor: Unified ingestor/automaton interface

- scan(from, until, args) - Process events in time range
- TimeHorizon: Historical|Continuous|Snapshot

CheckpointManager: State persistence across restarts
EventRegistry: Type-safe event definitions

## DATABASE

SCHEMA:

- core.events: Main event table (ULID PK, immutable)
- core.automaton_checkpoints: Processing state
- raw.source_material_registry: External data tracking

## COMMON ERRORS

- "unsupported type ulid" → use ulid_to_uuid()
- "SQLX offline" → just sqlx-prepare && git add .sqlx/
- "socket connection refused" → check ingestd + /run/sinex/ingest.sock
- "no such file" in nix → uncommitted files (git status)
- "ConnectionRefused" → redis-server not running

## KEY DOCS

- spec/understand/ - top-down description of sinex
- spec/SADI.md - Architecture overview
- nixos/README.md - Deployment guide

## OUTPUT GUIDELINES

- Unless asked otherwise, just output reports directly, not into a file. Do not litter with random markdowns.
