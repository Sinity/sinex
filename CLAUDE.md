# CLAUDE.md

## OPERATING_PRINCIPLES (READ FIRST)

BEFORE_ANY_WORK:
1. git status (check clean state)
2. git pull (if on shared branch)
3. nix develop (ALWAYS, sets up environment)
4. If modifying DB: just migrate (ensure current)
5. If big change: git checkout -b claude/feature-name

TASK_USAGE:
- Launch Tasks for work >30s or self-contained operations
- Can run 5 parallel (call sequentially in ONE message or lose slots)
- If >2 parallel, ask user first
- Split large work into parallel Tasks when possible
- Task for: research, large refactors, test fixes, doc updates
- Task prompt: include file paths, specific goals, "expect other agents working"

WORKFLOW:
1. Large request→Research(Grep/Read/Task)→Plan→Report plan→Execute
2. Compile once→save output→analyze saved file (NOT recompile)
3. Use TodoWrite immediately for multi-step work
4. Atomic git commits with descriptive messages
5. NEVER: rm -rf, git reset --hard, git clean without explicit intent
6. Question vague requests→get specifics before starting

EFFICIENCY_PATTERNS:
compile: cargo check --workspace>/tmp/out 2>&1 && {grep error /tmp/out|wc -l /tmp/out|head /tmp/out}
error_analysis: grep -E "^error\[E[0-9]+\]:" /tmp/out | sort | uniq -c | sort -rn (frequency of error types)
first_errors: grep -B2 -A2 "^error" /tmp/out | head -30 (context around first errors)
search: rg -t rust "pattern" --stats (NOT multiple greps)
git_workflow:
  - git add -p (selective staging)
  - git stash push -m "WIP: X" (named stashes)
  - git worktree add ../sinex-experiment (parallel experiments)
  - git diff --cached before commit (review staged)
  - git commit --fixup HEAD~n (for later rebase -i --autosquash)
memory: update CLAUDE.md when noticing patterns/issues

DECISION_HEURISTICS:
- Notice repeated commands→extract to CLAUDE.md pattern
- See compilation fail→save full output before re-running
- Multiple similar changes→use ast-grep or sed, not manual
- >50 similar items→ALWAYS automate (ast-grep for code, rg+sed for text)
- Uncertain about impact→create git branch first
- Long operation→launch Task agent instead of doing directly

ARCH: Satellites(systemd)→ingestd(gRPC:/run/sinex/ingest.sock)→core.events+Redis(sinex:events)→Processors→synthesis.events

SATELLITE_TYPES: Ingestors(fs-watcher,terminal-satellite,desktop-satellite,system-satellite), Automata(command-canonicalizer,health-aggregator,pkm-automaton)

CRITICAL_PATHS:
- Ingestion: ingestor_satellite→gRPC→ingestd→{PostgreSQL(batch),Redis(XADD)}
- Processing: Redis(XREADGROUP)→automaton_satellite→checkpoint→synthesis
- API: client→gateway→Redis(api.command.*)→service_automaton→Redis(api.response.*)→gateway

QUICK_NAVIGATION:
want_to_add_event_type→crate/sinex-events/src/lib.rs
want_new_ingestor→copy crate/sinex-fs-watcher, update Cargo.toml, add to flake.nix
want_new_automaton→copy crate/sinex-terminal-command-canonicalizer
config_loading→crate/sinex-satellite-sdk/src/config.rs
event_validation→crate/sinex-ingestd/src/validation.rs
checkpoint_logic→crate/sinex-satellite-sdk/src/checkpoint.rs:CheckpointManager
redis_integration→crate/sinex-satellite-sdk/src/redis_client.rs

NEW_INGESTOR_CHECKLIST:
1. Copy existing ingestor (e.g., sinex-fs-watcher)
2. Update Cargo.toml name, add to workspace
3. Implement StatefulStreamProcessor trait
4. Add to flake.nix outputs.packages
5. Add systemd service to nixos/modules/default.nix
6. Add event types to sinex-events/src/lib.rs
7. Create migration for any new schemas
8. Add to services.sinex.eventSources in NixOS config
9. Test: nix build .#sinex-new-ingestor
10. Document in CLAUDE.md QUICK_NAVIGATION


INVARIANTS: events_immutable, ulid_ids(sinex_ulid::Ulid), pg_jsonschema_validation, sqlx_offline(.sqlx/→git), satellites_isolated(systemd)

CMDS:
dev: nix develop && cargo check --workspace && just test
sqlx: just sqlx-prepare && git add .sqlx/
run: just {ingestd,gateway,fs-watcher}
debug: RUST_LOG=debug just X
db: just psql, just migrate-create X, dropdb sinex_dev && createdb sinex_dev && just migrate
deploy: systemctl {restart,status} sinex-X, sinex-preflight verify --timeout 120

MOST_USED_PATTERNS:
find_impl: rg "impl.*StructName" -t rust
find_usage: rg "function_name\(" -t rust -A 2
find_type_def: rg "struct StructName|enum EnumName|trait TraitName" -t rust
check_error: cargo check --workspace 2>&1 | tee /tmp/err && grep -n "error\[E" /tmp/err
quick_fix: cargo fix --workspace --allow-dirty
format_check: cargo fmt --all -- --check
clippy_fix: cargo clippy --workspace --fix --allow-dirty
git_undo_last: git reset --soft HEAD~1
git_fixup: git add -p && git commit --fixup HEAD && git rebase -i --autosquash HEAD~2

PITFALLS:
nix_fail→uncommitted_files, sqlx_offline→just sqlx-prepare&&git add .sqlx/, test→#[sinex_test], config_precedence(CLI>env>file>default), no_events→check_ingestd+satellite_conn, socket_fail→/run/sinex/ingest.sock(perms)

CRATES:
core(RawEvent,errors), db(models,pool), events(EventType trait), satellite-sdk(StatefulStreamProcessor,CLI)
hubs: ingestd(gRPC→batch), gateway(HTTP→commands)
ingestors: fs-watcher, terminal-satellite, desktop-satellite, system-satellite
automata: terminal-command-canonicalizer, health-aggregator, pkm-automaton
ops: preflight(7phases)
test/{unit,integration,system,common}

PATTERNS:
err: CoreError::database("X").with_context("K","V").build()
val: ValidationChain::validate(v,"f").not_empty().min_length(N).into_result()?
event: RawEventBuilder::new(src,type,payload).with_host(h).build()
test: #[sinex_test] async fn X(ctx:TestContext)->TestResult{ctx.pool()...}

DEEP_SYMMETRY:
trait StatefulStreamProcessor{async fn scan(&mut self,from:Checkpoint,until:TimeHorizon,args:ScanArgs)->SatelliteResult<()>}
TimeHorizon{Historical{end_time},Continuous,Snapshot}
CLI: X service|scan --since T --until T|explore
checkpoint: core.automaton_checkpoints(PostgreSQL)+Redis(consumer_groups) [BUG:HotlogAutomatonRunner not saving DB checkpoints]

DB_ULID:
sqlx::query!("INSERT...VALUES($1::uuid...)",ulid.to_uuid()) //NEVER raw ulid
auto: INSERT...RETURNING id::uuid as "id!"
checkpoint: automaton_checkpoints(automaton_name,last_processed_id[TEXT])
rules: ulid.to_uuid(), $1::uuid cast, RETURNING for auto-gen

ARCH_DETAILS:
satellites: systemd{ingestors(external→ingestd), automata(redis→processing)}
hubs: ingestd(/run/sinex/ingest.sock), gateway(HTTP/JSON-RPC), redis(sinex:events)
DB: core.events(ULID,hypertable), source_material_registry(external_data), operations_log(audit), core.automaton_checkpoints, sinex_schemas.*, conn:postgresql:///sinex_dev?host=/run/postgresql
redis: XADD→sinex:events→XREADGROUP(consumer_groups)→XACK
heartbeat: stdout(JSON)→journald→events→health_automaton
consts: sinex_core::{timeouts,limits,buffers,retry,filesystem}

EVENTS:
trait EventType{type Payload;type SourceImpl;const EVENT_NAME:&'static str}
EventRegistryBuilder auto-gen
sources: fs,shell.{kitty,atuin,history,recording,scrollback},wm.hyprland,clipboard,dbus,journald
naming: source.type(no redundancy)

EVENT_TYPES:
fs: file.{created,modified,deleted,moved}, dir.{created,deleted}
shell: command.{executed,failed,imported}, session.{started,ended}, recording.{started,ended}, output.captured
wm: window.{opened,closed,focused,moved,resized}, workspace.{switched,created,destroyed}, display.{connected,disconnected}, monitor.focused, state.captured
clip: copied,selected
dbus: {signal,method,notification,device,media,power,network,bluetooth,mount}.*
journal: entry.written, satellite.heartbeat
sinex: satellite.{started,stopped,error,healthy}

TEST:
structure: test/{unit,integration,system,common}
macro: #[sinex_test] async fn X(ctx:TestContext)->TestResult
ctx: pool(2000conns), start_test_{ingestd,satellite}(), wait_for_{event_type,redis_stream_length}(), verify_automaton_checkpoint()
builder: EventBuilder::filesystem().path(p).created().build()
run: just test{,-unit,-integration,-database}, just coverage

DEPLOY:
preflight: db_conn→extensions(pgx_ulid,pg_jsonschema,timescaledb)→migration_dryrun→resources→config→binaries→tests
nix: services.sinex{targetUser(REQ),database.url,eventSources.*,update.{gracePeriod,rollbackOnFailure}}
systemd: hubs(ingestd,gateway), ingestors(fs,terminal,desktop,system), automata(command-canonicalizer,health,pkm)
flow: systemctl restart sinex-update→preflight(7)→deploy|rollback

FEATURES:
core: ValidationChain,ErrorContext,ChannelSenderExt,ConfigExtractor,TestContext,EventRegistry(GitOps)
sdk: StatefulStreamProcessor,ProcessorCliRunner(processor_main!),CheckpointManager,HeartbeatManager,ExplorationProvider
bus: Redis(durable,ordered),ConsumerGroups(auto-track),gRPC(high-perf)
proc: HotlogAutomatonRunner,source_event_ids(provenance),pg_jsonschema

STATUS_2025-07-17:
done: satellite_constellation,deep_symmetry,redis_streams,journald_heartbeat,checkpoint_hybrid,gRPC,GitOps_schema,checkpoint_persistence_fix,raw_event_provenance,CLI_replay_explore,source_event_ids_impl,plan_md_unified_architecture,processor_manifests,StatefulStreamProcessor_migration
bugs: backwards_raw_events_references_from_cleanup
missing: test_coverage_gaps_from_architecture_migration,fix_backwards_schema_references
reality>vision: redis_streams_mature, sdk_abstractions_solid, checkpoint_hybrid>redis_only, journald>direct_db

CRITICAL: system captures entire digital life→reliability non-negotiable
STATE: sophisticated, core features complete, needs test_coverage_completion

RECOVERY_PROCEDURES:
corrupted_db: pg_dump sinex_dev>/tmp/backup.sql before any destructive ops
redis_inconsistent: redis-cli FLUSHDB (nuclear option) OR redis-cli --scan --pattern "sinex:*" | xargs redis-cli DEL
checkpoint_mismatch: UPDATE core.automaton_checkpoints SET last_processed_id=NULL WHERE automaton_name='X'
event_replay: DELETE FROM core.events WHERE source='X' AND ts_orig BETWEEN Y AND Z; restart processor
service_wedged: systemctl stop sinex-X && pkill -f sinex-X && systemctl start sinex-X

KEY_PATHS:
/run/sinex/ingest.sock (gRPC), postgresql:///sinex_dev?host=/run/postgresql
redis: sinex:events (main stream), consumer_group_pattern: automaton_name
checkpoint_bug: crate/sinex-satellite-sdk/src/automaton.rs:HotlogAutomatonRunner::run_batch() missing save

TYPE_SIGS:
RawEvent{id:Ulid,source:String,event_type:String,ts_orig:DateTime,payload:JsonValue}
CheckpointState{last_processed_id:Option<String>,processed_count:u64,data:Option<JsonValue>}
trait ExplorationProvider{async fn get_source_state()->SourceState; async fn get_coverage_analysis()->CoverageReport}

COMMON_ERRORS:
"unsupported type ulid"→use .to_uuid()
"socket connection refused"→check ingestd running+/run/sinex/ingest.sock
"SQLX offline"→just sqlx-prepare && git add .sqlx/
"duplicate key value"→ULID collision(extremely rare) or replay without delete
"no such file or directory"→likely in nix build, check git status (nix only sees committed files)
"ConnectionRefused"→Redis not running: redis-server or systemctl start redis
"consumer group already exists"→normal on restart, ignore or XGROUP DESTROY first

DEBUG_FLOWS:
satellite_not_sending_events:
1. systemctl status sinex-X
2. journalctl -u sinex-X -f
3. Check /run/sinex/ingest.sock exists
4. RUST_LOG=debug systemctl restart sinex-X
5. Check satellite can connect to ingestd

events_not_processing:
1. redis-cli XINFO STREAM sinex:events
2. redis-cli XINFO GROUPS sinex:events  
3. Check consumer group has pending messages
4. psql→SELECT * FROM core.automaton_checkpoints WHERE automaton_name='X'
5. Check automaton service running

DB_SCHEMA:
core.events(id ULID PK, source TEXT, event_type TEXT, ts_orig TIMESTAMPTZ, ts_ingest TIMESTAMPTZ, host TEXT, payload JSONB, source_event_ids ULID[], correlation_id ULID, source_material_id ULID, associated_blob_ids ULID[], ingestor_version TEXT, payload_schema_id TEXT)
core.automaton_checkpoints(id UUID, automaton_name TEXT, consumer_group TEXT, last_processed_id TEXT, state_data JSONB)
provenance: source_event_ids distinguishes external(NULL) from synthesis(Vec<ULID>), correlation_id for request tracking, source_material_id for external data lifecycle

USEFUL_QUERIES:
-- Recent events by type
SELECT ts_orig,source,event_type,payload FROM core.events WHERE event_type='X' ORDER BY ts_orig DESC LIMIT 20;
-- Event throughput
SELECT source,COUNT(*) as cnt FROM core.events WHERE ts_ingest > NOW()-'1 hour'::interval GROUP BY source ORDER BY cnt DESC;
-- Checkpoint status
SELECT automaton_name,last_processed_id,processed_count,last_activity FROM core.automaton_checkpoints;
-- Find events with specific payload content
SELECT * FROM core.events WHERE payload @> '{"path":"/some/file"}'::jsonb;
-- Find events by correlation
SELECT * FROM core.events WHERE correlation_id = $1::uuid;
-- Find source material
SELECT * FROM source_material_registry WHERE blake3_hash = $1;

TEST_PATTERNS:
quick_test_one: cargo test -p sinex-core test_name -- --nocapture
test_with_db: #[sinex_test] (auto transaction rollback)
integration_test: start actual services→test interaction→verify in DB
property_test: use proptest for edge cases (see test/property/)
benchmark: cargo bench -p sinex-X (see benches/)

KEY_DOCS:
/realm/project/sinex/design_discussion.md - VISION & architectural philosophy
/realm/project/sinex/spec/SADI.md - Architecture overview
/realm/project/sinex/spec/STAD.md - Technical details
/realm/project/sinex/CLAUDE.md - THIS FILE (my memory)

AVOID_THESE_PITFALLS:
- Starting coding without understanding goal (ask clarifying questions)
- Modifying without reading existing code first
- Making changes without running tests
- Assuming file exists without checking
- Using blocking I/O in async contexts
- Forgetting to handle Redis connection failures
- Not checking if service is already running before start
- Committing debug print statements

PERF_INVESTIGATION:
slow_ingestion: RUST_LOG=sinex_ingestd=trace→check batch sizes
high_memory: heaptrack or valgrind→find leaks
slow_queries: EXPLAIN ANALYZE→add indexes
redis_bottleneck: redis-cli --latency→check network

MEMORY_MAINTENANCE:
- Notice inefficiency→add to EFFICIENCY_PATTERNS
- Find new error→add to COMMON_ERRORS with fix
- Discover key path/config→add to KEY_PATHS
- See repeated task pattern→add to WORKFLOW
- Remove outdated info immediately (don't accumulate cruft)
- If something NOT in CLAUDE.md causes confusion→add it
- Review & compress periodically (token efficiency)
- CRITICAL: Before ANY commit, review CLAUDE.md for info obsoleted by changes and update it