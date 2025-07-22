# CLAUDE.md

## CORE PRINCIPLES

STARTUP:

- nix develop (ALWAYS first, sets up environment)
- git status && git pull (check state)
- just migrate (if DB work planned)

WORKFLOW:

- Research before implementing
- Save compilation output for analysis (don't recompile)
- TodoWrite for multi-step work
- Atomic commits with clear messages

EFFICIENCY:

- compile: cargo check --workspace >/tmp/out 2>&1
- errors: grep -E "^error\[E[0-9]+\]:" /tmp/out | sort | uniq -c
- search: rg -t rust "pattern" (avoid multiple greps)
- git: add -p, stash push -m "WIP: desc", diff --cached

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

## DEBUG CHECKLIST

NO_EVENTS:

1. systemctl status sinex-X
2. journalctl -u sinex-X -f
3. ls -la /run/sinex/ingest.sock
4. RUST_LOG=debug systemctl restart sinex-X

NO_PROCESSING:

1. redis-cli XINFO STREAM sinex:events
2. redis-cli XINFO GROUPS sinex:events
3. Check pending messages + automaton service

## RECOVERY

- DB backup: pg_dump sinex_dev >/tmp/backup.sql
- Redis reset: redis-cli --scan --pattern "sinex:*" | xargs redis-cli DEL
- Checkpoint reset: UPDATE core.automaton_checkpoints SET last_processed_id=NULL
- Service stuck: systemctl stop sinex-X && pkill -f sinex-X

## USEFUL QUERIES

```sql
-- Recent events
SELECT ts_orig, source, event_type FROM core.events ORDER BY ts_orig DESC LIMIT 20;

-- Throughput 
SELECT source, COUNT(*) FROM core.events 
WHERE ts_ingest > NOW()-'1h'::interval GROUP BY source;

-- Checkpoints
SELECT automaton_name, last_processed_id, processed_count 
FROM core.automaton_checkpoints;
```

## KEY DOCS

- spec/understand/ - top-down description of sinex
- spec/SADI.md - Architecture overview
- nixos/README.md - Deployment guide

