# Unified Harmonized Plan: Sinex Evolution (v2)
**Date:** 2025-01-15 (Updated)
**Status:** Definitive Roadmap - Supersedes All Previous Plans
**Version:** 2.0 (Comprehensive)

---

## Executive Summary

This plan is **the single source of truth** for Sinex development. It fully internalizes all previous planning documents and provides concrete, executable steps with exit criteria.

**Current Reality:**
- ✅ **Infrastructure 70-85% complete:** Event sourcing, NATS/KV, TimescaleDB, testing framework
- ⚠️ **Hybrid state problem:** Half-migrated tests, configs, vocabulary create maintenance burden
- ❌ **DX layer missing:** Naming vocabulary old, CLI is Python, no unified tooling

**Strategic Approach:**
1. **Finish infrastructure** (Phases 1-2): Eliminate hybrid state, complete migrations
2. **Decisive cutover** (Phase 3): Break backend + replace frontend simultaneously (no compatibility shims)
3. **Aspirational features** (Phase 4): `sx` unified tool, `SimpleProcessor`, Wasm runtime
4. **Continuous polish** (Phase 5): Performance, observability, advanced features

**Timeline:** 11-15 weeks to production-ready (Phases 1-3 complete)

---

## Phase 1: Complete Infrastructure (3-4 weeks)

**Goal:** Finish all in-progress migrations to eliminate hybrid state.

### 1.1. Finish KV Coordination (85% → 100%)
**Priority:** CRITICAL
**Effort:** 1 week
**Status from backlog:** KV coordination in place, schema cache fallback + legacy tests incomplete

**Tasks:**

**1. Schema Broadcast Cache Fallback**
- **Location:** `crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs`
- **Current:** Cache exists, listener subscribes to `system.schemas.active`
- **Gap:** Validators don't prefer cache over DB; no fallback logic
- **Action:**
  ```rust
  // Validator logic flow:
  1. Check SchemaBroadcastCache.get(source, event_type, version)
  2. If hit: return schema
  3. If miss AND PgPool present: query DB, update cache
  4. If miss AND no PgPool: return SchemaNotAvailable error
  ```
- **Test:** Edge mode node validates schemas without DB connection

**2. Migrate Legacy Lifecycle Test**
- **Location:** `crate/lib/sinex-node-sdk/tests/integration/node_lifecycle_test.rs`
- **Lines:** 136-147 (old constructor), 412-417 (table queries)
- **Action:**
  - Replace `nodeCoordination::new(instance, pool)` with async KV constructor
  - Remove direct `core.node_instances` table queries
  - Use `CoordinationKvClient` for verification
- **Test:** All node SDK integration tests pass without legacy tables

**3. Gateway Coordination State Helpers**
- **Location:** `crate/core/sinex-gateway/src/` (new: `handlers/coordination.rs`)
- **Action:** Add RPC endpoints:
  ```
  GET  /coordination/instances          → [{ service, instance_id, hostname, version, last_heartbeat }]
  GET  /coordination/leader/{service}   → { instance_id, version, acquired_at, last_heartbeat }
  GET  /coordination/health/{instance}  → { status, heartbeat_age_ms, metadata }
  ```
- **Implementation:** Read from KV buckets (`KV_sinex_instances`, `KV_sinex_leadership`)
- **Test:** CLI can query coordination state via RPC

**4. Drop Legacy Coordination Tables**
- **Location:** `crate/lib/sinex-schema/`
- **Action:**
  - Create migration: DROP TABLE `core.node_instances`
  - Create DOWN migration: restore table (for rollback safety)
  - Update docs: Remove references to legacy coordination tables
- **Test:** All tests pass without `core.node_instances`

**Exit Criteria:**
- ✅ Edge mode nodes validate schemas via cache (no DB dependency)
- ✅ All tests use KV coordination (zero legacy table queries)
- ✅ Gateway exposes coordination state via RPC
- ✅ Legacy coordination tables dropped from schema

---

### 1.2. Gateway RPC API Completeness
**Priority:** HIGH (unblocks Phase 3 CLI work)
**Effort:** 2 weeks
**Status from backlog:** Gateway blob/event re-publish TBD; missing DLQ/nodes/ops endpoints

**Missing Endpoints:**

**DLQ Management** (currently requires `--use-db`)
```rust
POST /dlq/list
  → { streams: [{ subject, backlog_count, first_seq, last_seq }] }

POST /dlq/peek
  body: { subject, limit: 10 }
  → { messages: [{ seq, subject, data, headers, timestamp }] }

POST /dlq/requeue
  body: { subject, max_count: 100 }
  → { enqueued: 42, errors: [] }

POST /dlq/purge
  body: { subject, before_seq: Optional }
  → { deleted: 1000 }
```
- **Implementation:** Gateway queries NATS JetStream DLQ streams, performs operations, returns results
- **No direct DB access** from CLI after this

**Node Operations** (currently no RPC exists)
```rust
GET  /nodes
  query: ?role=capture&status=active
  → [{ id, role, version, hostname, status, last_heartbeat, metadata }]

POST /nodes/{id}/drain
  → { status: "draining", ack_timeout_ms: 30000 }

POST /nodes/{id}/resume
  → { status: "active" }

POST /nodes/{id}/set-horizon
  body: { horizon: "Continuous" | "Historical" | "Snapshot" }
  → { updated: true, effective_at: timestamp }
```
- **Implementation:** Gateway reads/writes KV coordination state, publishes to `sinex.control.nodes.*`

**Generic Operations Log** (partial - replay works, generic ops missing)
```rust
POST /ops/start
  body: { kind: "maintenance", payload: {...} }
  → { operation_id, status: "pending" }

GET  /ops
  query: ?state=running&kind=maintenance
  → [{ id, kind, state, created_at, updated_at, progress }]

GET  /ops/{id}
  → { id, kind, state, timeline: [...], counters: {...}, error: Optional }

POST /ops/{id}/cancel
  → { status: "cancelling" }
```
- **Implementation:** Gateway writes to `core.operations_log`, returns structured data

**Audit Trail** (currently no endpoint)
```rust
GET  /audit/{operation_id}
  → {
      operation_id,
      inputs: { query, parameters },
      outcomes: { events_processed, errors },
      timeline: [{ ts, event, details }],
      related_events: [{ id, type, summary }]
    }
```
- **Implementation:** Query operations log + related events, construct audit trail

**Blob/Event Re-publish Decision**
- **Context:** Gateway receives blobs via RPC, needs relay to nodes
- **Decision:** Implement "Gateway → NATS → node" relay
- **Action:**
  - Gateway publishes blob chunks to `content.ingress.{id}`
  - Document ingestor/content automaton subscribe, perform git-annex add
  - No direct git-annex calls from gateway
- **Test:** End-to-end blob flow via RPC → NATS → annex

**Exit Criteria:**
- ✅ All CLI operations work via RPC (no `--use-db` needed)
- ✅ Integration tests verify DLQ, nodes, ops, audit endpoints
- ✅ Gateway publishes all mutations as events to `sinex.control.*`
- ✅ Blob relay via NATS functional

---

### 1.3. Test Modernization Completion
**Priority:** HIGH
**Effort:** 1-2 weeks
**Status from backlog:** Test infrastructure done, content migration incomplete (180+ call sites)

**The "Massive Migration":**

**Inventory remaining DB-only test patterns:**
```bash
# Find all remaining direct DB inserts
rg -l 'create_test_event|EventRepository::insert(?!.*pipeline)|db_only' \
  tests/ \
  crate/core/sinex-ingestd/tests/ \
  crate/core/sinex-gateway/tests/ \
  crate/lib/sinex-node-sdk/tests/ \
  crate/lib/sinex-services/tests/
```

**Batch Migration Strategy:**
1. **Per-crate sweep:**
   - `sinex-ingestd`: Replace with `ctx.publish_json_event`
   - `sinex-gateway`: Replace with pipeline seeding
   - `sinex-node-sdk`: Replace with `TestnodePublisher`
   - `sinex-services`: Replace with dataset seeds

2. **Remove legacy helpers:**
   ```rust
   // Delete these entirely:
   - create_test_event (any variant)
   - ctx.db_only()
   - EventFactory::create_and_insert
   ```

3. **Update test templates:**
   - `test-template.md`: Only show pipeline patterns
   - `TEST_PATTERNS.md`: Remove DB-only sections
   - `PIPELINE_TESTING.md`: Mark as canonical

**God File Splits** (from backlog, already done but verify):

**Verify splits completed:**
```bash
# Check events module
ls -la crate/lib/sinex-core/src/db/repositories/events/
# Should show: mod.rs, conversions.rs, persistence.rs, queries.rs

# Check material_assembler module
ls -la crate/core/sinex-ingestd/src/material_assembler/
# Should show: mod.rs, state.rs, pipeline.rs, io.rs, finalize.rs
```

**If splits missing, execute:**
- Split `events.rs` → `events/{conversions,persistence,queries}.rs`
- Split `material_assembler.rs` → `material_assembler/{state,pipeline,io,finalize}.rs`
- Maintain SQLx macro compatibility (unique struct names per module)
- Goal: All modules <500 LOC

**Exit Criteria:**
- ✅ Zero `create_test_event` call sites remain
- ✅ `db_only()` helper deleted from test-utils
- ✅ God files verified split (events, material_assembler)
- ✅ All tests pass after migration
- ✅ Test documentation updated (no DB-only patterns)

---

### 1.4. Config Hygiene & Type Safety
**Priority:** MEDIUM
**Effort:** 3-5 days
**Status from backlog:** Most configs migrated to Seconds/Bytes; remaining stragglers + serde support needed

**Complete Newtype Migration:**
```bash
# Find remaining raw integers for time/size
rg 'timeout.*:\s*u64|interval.*:\s*u64|size.*:\s*u64|limit.*:\s*u64|age.*:\s*u64' \
  --type rust crate/ --glob '!**/target/**'
```

**Action:**
1. **Replace raw integers:**
   - All `timeout: u64` → `timeout: Seconds`
   - All `size: u64` → `size: Bytes`
   - All `interval: u64` → `interval: Seconds`

2. **Add serde support for human-readable syntax:**
   ```rust
   // In sinex-core/src/types/
   impl<'de> Deserialize<'de> for Seconds {
       fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
       where D: Deserializer<'de> {
           // Accept both: 30 (raw) or "30s" (string)
           // Parse: "30s", "500ms", "2m", "1h"
       }
   }

   impl<'de> Deserialize<'de> for Bytes {
       // Accept both: 1024 (raw) or "1KiB" (string)
       // Parse: "1KiB", "5MiB", "1GiB"
   }
   ```

3. **Add validation:**
   - `Seconds`: Range 0..=86400 (24 hours max)
   - `Bytes`: Range 0..=1GiB (configurable per use case)
   - Return clear errors on invalid values

4. **Update config examples:**
   ```toml
   # Before
   timeout = 30

   # After (both work)
   timeout = 30
   timeout = "30s"
   ```

**Env Var Audit:**
```bash
# Find non-SINEX_ prefixed vars
rg 'env::var\("(?!SINEX_|RUST_LOG|CARGO_|CI)' --type rust crate/
```

**Action:** Enforce `SINEX_` prefix universally
- Document approved exceptions: `RUST_LOG`, `CARGO_*`, `CI`, `HOME`, `USER`
- All application-specific vars must be `SINEX_*`

**Exit Criteria:**
- ✅ All timeout/interval/size configs use `Seconds`/`Bytes`
- ✅ Config files accept "30s" / "5MiB" syntax natively
- ✅ Validation prevents invalid ranges
- ✅ All env vars use `SINEX_` prefix (documented exceptions only)

---

## Phase 2: Native Integration & Security (4-6 weeks)

**Goal:** Move nodes from shell-outs to native Rust; harden security.

### 2.1. Desktop node: Native APIs
**Priority:** HIGH
**Effort:** 1-2 weeks
**Status from backlog:** Clipboard shells out; window manager fragile; stubs in production

**Current Problems:**
- `clipboard.rs`: Shells out to `wl-paste` (Wayland), `xclip` (X11), `pbpaste` (macOS)
- `window_manager.rs`: Hyprland-only via socket parsing
- No graceful degradation when tools missing

**Native Clipboard Implementation:**
```rust
// Use native libraries instead of shell-outs
dependencies:
- arboard = "3.x"              // Cross-platform clipboard
- wayland-client = "0.31"      // Native Wayland protocol
- x11-clipboard = "0.9"        // Native X11 clipboard

// Architecture:
1. Detect platform at runtime
2. Use native API for clipboard access
3. Poll interval: reduce from 2s to 100ms (native is fast)
4. Handle binary data properly (detect UTF-8 vs binary)
```

**Graceful Degradation:**
```rust
// Config: SINEX_DESKTOP_REQUIRE_HYPRLAND
- true  → fail-fast if Hyprland socket missing (explicit requirement)
- false → sleep-retry with exponential backoff (optional feature)

// Health check per monitor:
struct DesktopMonitorHealth {
    clipboard_active: bool,
    window_manager_active: bool,
    last_error: Option<String>,
}

// node can run partially (e.g., clipboard works, WM disabled)
```

**Window Manager (Hyprland):**
- Keep socket-based approach for now (Hyprland-specific protocol)
- Add explicit timeout on socket reads (30s)
- Future (Phase 4): Add Sway, i3 via `i3ipc`/`swayipc` crates

**Exit Criteria:**
- ✅ No shell-outs to `wl-paste`, `xclip`, `pbpaste`
- ✅ Clipboard monitoring uses native APIs
- ✅ node runs on headless server (degraded mode)
- ✅ Health checks report which monitors are active
- ✅ Integration tests verify native clipboard + WM event flow

---

### 2.2. System node: Native Journal/Systemd
**Priority:** HIGH
**Effort:** 2-3 weeks
**Status from backlog:** journalctl subprocess remains; duplicate watchers; lifecycle invariants missing

**Consolidate Duplicate Watchers:**
```rust
// Current: Two separate watchers spawn journalctl
- journal_watcher.rs:273     → journalctl -f -o json
- systemd_watcher.rs:354     → journalctl -f -o json -u *.service

// After: Single JournalWatcher
- One journalctl process provides all events
- Filter by _SYSTEMD_UNIT field for systemd-specific events
- Reduce process count by 50%
```

**Native Bindings (Evaluate):**
```rust
// Option A: libsystemd bindings
dependencies:
- libsystemd-sys = "0.9"
- systemd = "0.10"

// Option B: Pure Rust journal reader
- Read /var/log/journal directly
- Parse journal binary format
- No subprocess overhead

// Decision criteria:
- If libsystemd stable: use native API
- If unstable: keep consolidated subprocess with better supervision
```

**Watcher Lifecycle Hardening:**
```rust
// All watchers must implement:
trait WatcherLifecycle {
    fn health_snapshot(&self) -> WatcherHealth;
    fn shutdown(&mut self, graceful: bool) -> Result<()>;
    fn last_event_timestamp(&self) -> Option<Instant>;
}

// Cancellation tokens for all reader threads
- Each watcher gets CancellationToken
- Shutdown: send cancel signal, await thread drain
- Timeout: force-kill after 30s if drain hangs
```

**Udev Watcher (from deep-analysis Issue #42):**
```rust
// Current: 5-second polling loop
// Problem: Misses transient devices, 0-5s latency

// After: inotify on /sys/class/
use notify::{Watcher, RecursiveMode};
watcher.watch("/sys/class/net", RecursiveMode::NonRecursive)?;
watcher.watch("/sys/class/block", RecursiveMode::NonRecursive)?;

// Real-time device detection, zero polling overhead
```

**Exit Criteria:**
- ✅ Single journalctl process (or native API) for all journal events
- ✅ Udev uses inotify (not polling)
- ✅ All watchers implement lifecycle protocol
- ✅ Health checks report last event timestamp
- ✅ Shutdown drains all watcher threads cleanly

---

### 2.3. Security Hardening
**Priority:** HIGH
**Effort:** 1 week
**Status:** mTLS implemented but not tested in CI; token rotation missing; build stamping incomplete

**mTLS Integration Testing:**
```rust
// Wire SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1 into full E2E suite
- Location: tests/e2e/nixos-vm/
- Action:
  1. Generate test CA + client certs in CI
  2. Configure gateway to require mTLS
  3. Configure CLI with client cert
  4. Run full E2E suite
- Test scenarios:
  - CLI with valid cert: succeeds
  - CLI without cert: rejected
  - CLI with expired cert: rejected
  - Gateway rejects non-TLS connections
```

**Token Rotation:**
```rust
// Location: crate/core/sinex-gateway/src/rpc_server.rs
// Current: Token loaded once at startup

// After: Watch token file for changes
use notify::{Watcher, RecursiveMode};

impl GatewayServer {
    fn watch_token_file(&mut self) {
        // inotify on token file
        // On modify: reload token
        // On delete: disable auth temporarily (log warning)
        // On recreate: reload token
    }
}

// Test: Update token file, next request uses new token
```

**Build Stamping:**
```rust
// Location: Workspace-wide build.rs
// Action: Inject git revision at build time

// build.rs:
fn main() {
    let git_rev = Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=SINEX_GIT_REV={}", git_rev);
}

// Update EventBuilder:
impl Event {
    pub fn builder() -> EventBuilder {
        EventBuilder {
            ingestor_version: env!("SINEX_GIT_REV").to_string(),
            // ...
        }
    }
}

// Test: Events contain git revision in metadata
```

**Exit Criteria:**
- ✅ mTLS enforced in CI E2E test runs
- ✅ Token rotation works without gateway restart
- ✅ All events contain build version/git revision
- ✅ Integration tests verify TLS scenarios

---

## Phase 3: The Decisive Cutover (2-3 weeks)

**Goal:** Execute naming revamp + CLI replacement as single atomic operation.

**Strategic Decision:** Break backend (rename) + replace frontend (Rust CLI) simultaneously. No compatibility shims. Forces completion.

### 3.1. Backend Rename (node → node)
**Priority:** CRITICAL
**Effort:** 1 week
**Impact:** BREAKS OLD CLI (intentional)

**Filesystem & Crates:**
```bash
# Directory structure
git mv crate/nodes crate/nodes
mkdir -p crate/nodes/capture crate/nodes/synthesis

# Move capture nodes
git mv crate/nodes/sinex-fs-watcher crate/nodes/capture/
git mv crate/nodes/sinex-terminal-node crate/nodes/capture/
git mv crate/nodes/sinex-desktop-node crate/nodes/capture/
git mv crate/nodes/sinex-system-node crate/nodes/capture/
git mv crate/nodes/sinex-document-ingestor crate/nodes/capture/

# Move synthesis nodes
git mv crate/nodes/sinex-analytics-automaton crate/nodes/synthesis/
git mv crate/nodes/sinex-content-automaton crate/nodes/synthesis/
git mv crate/nodes/sinex-search-automaton crate/nodes/synthesis/
git mv crate/nodes/sinex-pkm-automaton crate/nodes/synthesis/
git mv crate/nodes/sinex-health-aggregator crate/nodes/synthesis/
git mv crate/nodes/sinex-terminal-command-canonicalizer crate/nodes/synthesis/

# Rename SDK crates
git mv crate/lib/sinex-node-sdk crate/lib/sinex-node-sdk
git mv crate/lib/sinex-processor-runtime crate/lib/sinex-node-runtime

# Optional: Rename crate packages (in Cargo.toml)
sinex-terminal-node → sinex-terminal-node
sinex-fs-watcher → sinex-fs-node
sinex-desktop-node → sinex-desktop-node
# ... etc
```

**Type System (rust-analyzer SSR):**

**Use rust-analyzer "Rename Symbol" (F2):**
```rust
// In VSCode/editor with RA:
1. Open: crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs
2. Place cursor on: StatefulStreamProcessor
3. Press F2 (rename symbol)
4. Type: Node
5. RA auto-fixes all imports and impls
```

**SSR Recipes (paste into RA "Structural Search & Replace"):**
```
# Trait implementations
Find:    impl StatefulStreamProcessor for $T
Replace: impl Node for $T

# Enum variants
Find:    ProcessorType::Ingestor
Replace: NodeRole::Capture

Find:    ProcessorType::Automaton
Replace: NodeRole::Synthesis

# Runner types
Find:    StreamProcessorRunner<$A>
Replace: NodeRunner<$A>

# State types
Find:    ProcessorRuntimeState
Replace: NodeRuntimeState

# CLI/Args
Find:    ProcessorCli
Replace: NodeCli

Find:    nodeArgs
Replace: NodeArgs
```

**Bulk String Renames (after type renames):**
```bash
# Use sd for fast, idempotent string replacement
# (Install: cargo install sd)

# Crate names
sd -s 'sinex-node-sdk' 'sinex-node-sdk' crate/
sd -s 'sinex-processor-runtime' 'sinex-node-runtime' crate/

# Documentation strings
sd -s 'node SDK' 'Node SDK' crate/
sd -s 'node' 'node' crate/ --glob '!archive/**'

# Roles
sd -s 'automaton' 'synthesis' crate/ --glob '!archive/**'
sd -s 'ingestor' 'capture' crate/ --glob '!archive/**'

# Guard: exclude archive and test snapshots
--glob '!docs/archive/**' --glob '!**/*.snap'
```

**Database Schema:**
```sql
-- Migration: rename_to_node_vocabulary.sql

-- Tables
ALTER TABLE core.processor_manifests RENAME TO node_manifests;

-- Columns
ALTER TABLE core.node_manifests RENAME COLUMN processor_name TO node_name;
ALTER TABLE core.node_manifests RENAME COLUMN processor_type TO role;

-- Constraints
ALTER TABLE core.node_manifests DROP CONSTRAINT IF EXISTS processor_type_check;
ALTER TABLE core.node_manifests ADD CONSTRAINT role_check
  CHECK (role IN ('capture', 'synthesis', 'core', 'gateway'));

-- Indexes (if any reference old names)
ALTER INDEX IF EXISTS idx_processor_manifests_type RENAME TO idx_node_manifests_role;

-- Down migration (rollback safety)
-- Recreate processor_manifests table with old schema
```

**JSON Schema:**
```bash
# Move and rename
git mv schemas/v1/processor/ schemas/v1/node/
git mv schemas/v1/node/processor_registered.json schemas/v1/node/node_registered.json

# Update schema contents
sd -s '"processor_type"' '"role"' schemas/v1/node/node_registered.json
sd -s '"ingestor"' '"capture"' schemas/v1/node/node_registered.json
sd -s '"automaton"' '"synthesis"' schemas/v1/node/node_registered.json
```

**Repository Code:**
```rust
// Update SQLx queries and models
// Location: crate/lib/sinex-core/src/db/repositories/state.rs

// Before:
struct ProcessorManifest {
    processor_name: String,
    processor_type: String,
}

// After:
struct NodeManifest {
    node_name: String,
    role: String,
}

// Update all queries:
sqlx::query!("SELECT node_name, role FROM core.node_manifests WHERE ...")

// Regenerate SQLx offline cache if used:
cargo sqlx prepare --workspace
```

**Cargo.toml Updates:**
```toml
# Update workspace members
[workspace]
members = [
    "crate/lib/sinex-node-sdk",          # was: sinex-node-sdk
    "crate/lib/sinex-node-runtime",      # was: sinex-processor-runtime
    "crate/nodes/capture/sinex-fs-node", # etc
    # ...
]

# Update dependencies throughout workspace
[dependencies]
sinex-node-sdk = { path = "../../lib/sinex-node-sdk" }
sinex-node-runtime = { path = "../../lib/sinex-node-runtime" }
```

**Impact & Verification:**
```bash
# After rename:
1. Compile:
   cargo build 2>&1 | tee compilation.log

2. Expected: Many import errors, all fixable by RA quick-fix

3. Fix imports:
   - Use RA "organize imports" (Shift+Alt+O)
   - Use RA quick-fix (Ctrl+.) on errors

4. Verify all tests pass:
   cargo nextest run --profile reliable

5. Python CLI (exo.py) will FAIL with schema errors
   - This is INTENTIONAL
   - Do not fix, proceed to 3.2
```

**Exit Criteria:**
- ✅ `cargo build` succeeds with new names
- ✅ All tests pass using new vocabulary
- ✅ Database uses `node_manifests` table with `role` column
- ✅ Python CLI (`exo.py`) fails (expected - will be replaced)

---

### 3.2. Rust CLI Replacement (`sinexctl`)
**Priority:** CRITICAL
**Effort:** 1-2 weeks
**Note:** This is **NOT** the `sx` tool (Phase 4). This is the RPC client that replaces Python CLI.

**Crate Structure:**
```
cli/
  sinexctl/              # Binary (main.rs + clap CLI)
  sinex-cli/             # Library (all logic)
    src/
      client/
        gateway.rs       # GatewayClient - JSON-RPC over HTTP/Unix socket
        admin.rs         # AdminClient - node operations
      commands/
        replay.rs        # plan, submit, watch, abort, ls
        nodes.rs         # list, status, drain, resume, set-horizon
        ops.rs           # start, ls, watch, cancel
        dlq.rs           # ls, peek, requeue, purge
        core.rs          # health, audit
        gateway.rs       # ping, version
      fmt/
        table.rs         # Human-readable tables (comfy-table)
        json.rs          # --json output (one object per line)
        progress.rs      # Progress bars (indicatif)
      model/
        mod.rs           # Shared serde types with gateway
        replay.rs        # ReplayPlan, ReplayOperation, etc.
        nodes.rs         # NodeInfo, NodeRole, etc.
      auth/
        token.rs         # Token loading from file/env
        tls.rs           # Client certificate loading
```

**Dependencies:**
```toml
[dependencies]
clap = { version = "4", features = ["derive", "env"] }
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.11", features = ["json", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
color-eyre = "0.6"
comfy-table = "7"
indicatif = "0.17"
tracing = "0.1"
tracing-subscriber = "0.3"
```

**Command Tree:**
```rust
#[derive(Parser)]
#[command(name = "sinexctl", about = "Sinex control CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Node operations
    Node {
        #[command(subcommand)]
        cmd: NodeCommands,
    },

    /// Replay operations
    Replay {
        #[command(subcommand)]
        cmd: ReplayCommands,
    },

    /// Generic operations
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },

    /// Dead letter queue
    Dlq {
        #[command(subcommand)]
        cmd: DlqCommands,
    },

    /// Core system operations
    Core {
        #[command(subcommand)]
        cmd: CoreCommands,
    },

    /// Gateway operations
    Gateway {
        #[command(subcommand)]
        cmd: GatewayCommands,
    },
}

#[derive(Subcommand)]
enum NodeCommands {
    /// List all nodes
    List {
        /// Filter by role
        #[arg(long)]
        role: Option<NodeRole>,

        /// Output format
        #[arg(long, default_value = "table")]
        format: OutputFormat,
    },

    /// Show node status
    Status {
        /// Node ID or name
        node: String,
    },

    /// Drain node for maintenance
    Drain {
        node: String,
    },

    /// Resume drained node
    Resume {
        node: String,
    },

    /// Set time horizon
    SetHorizon {
        node: String,

        #[arg(long, group = "horizon")]
        continuous: bool,

        #[arg(long, group = "horizon")]
        snapshot: Option<String>,

        #[arg(long, group = "horizon")]
        historical: Option<String>,
    },
}

// Similar for ReplayCommands, OpsCommands, etc.
```

**Output Policy:**
```rust
enum OutputFormat {
    Table,  // Human-readable (default)
    Json,   // One JSON object per line
    Yaml,   // YAML output
}

// Exit codes:
// 0   - Success
// 2   - In progress (for watch commands)
// 3   - Precondition failed
// 4   - Not found
// 10+ - Internal error

// Examples:
// $ sinexctl node list
// ROLE        ID                NAME              STATUS  HEARTBEAT
// capture     01HQ2...          fs-watcher        active  2s ago
// synthesis   01HQ3...          analytics         active  1s ago
//
// $ sinexctl node list --format json
// {"role":"capture","id":"01HQ2...","name":"fs-watcher","status":"active","heartbeat_ms":2000}
// {"role":"synthesis","id":"01HQ3...","name":"analytics","status":"active","heartbeat_ms":1000}
//
// $ sinexctl replay watch 01HQ4... --format json
// {"status":"running","progress":0.42,"events_processed":1000}
// {"status":"running","progress":0.85,"events_processed":2000}
// {"status":"completed","progress":1.0,"events_processed":2345}
```

**Authentication:**
```rust
// Token from file or env
impl GatewayClient {
    fn new(config: ClientConfig) -> Result<Self> {
        let token = config.token
            .or_else(|| env::var("SINEX_RPC_TOKEN").ok())
            .or_else(|| fs::read_to_string(&config.token_file).ok())
            .ok_or_else(|| eyre!("No token provided"))?;

        let client = reqwest::Client::builder()
            .user_agent("sinexctl/1.0")
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(config.insecure) // dev only
            .build()?;

        Ok(Self { client, token, base_url: config.url })
    }
}
```

**Safety Rails:**
```rust
// Destructive commands require confirmation
async fn drain_node(node: &str, yes: bool) -> Result<()> {
    if !yes && !prompt_confirm(&format!("Drain node {}?", node))? {
        return Ok(());
    }

    // Execute
}

// Dry-run support
async fn replay_plan(query: &str, dry_run: bool) -> Result<()> {
    let plan = client.create_replay_plan(query).await?;

    if dry_run {
        println!("Would process {} events", plan.event_count);
        return Ok(());
    }

    // Prompt and execute
}
```

**MVP Commands (Must Ship):**
```bash
# System
sinexctl gateway ping
sinexctl gateway version
sinexctl core health

# Nodes
sinexctl node list [--role capture|synthesis]
sinexctl node status <id>

# Replay
sinexctl replay plan --query '...'
sinexctl replay submit <plan-id>
sinexctl replay watch <operation-id>
sinexctl replay ls

# DLQ
sinexctl dlq ls
sinexctl dlq peek <subject>
```

**Exit Criteria:**
- ✅ `sinexctl` binary ships with all MVP commands
- ✅ All commands work via RPC (no direct DB access)
- ✅ `--json` output format works for all commands
- ✅ Integration tests verify CLI ↔ gateway communication
- ✅ Authentication (token + mTLS) works
- ✅ Exit codes follow spec

---

### 3.3. Python CLI Deletion
**Priority:** CRITICAL
**Effort:** 1 day

**Action:**
```bash
# Remove all Python CLI code
git rm -rf cli/

# This includes:
- cli/exo.py (main CLI)
- cli/rpc_client.py (JSON-RPC client)
- cli/replay_commands.py (replay logic)
- cli/interactive.py (interactive mode)
- cli/completion.py (shell completion)
- cli/README.md (Python CLI docs)
```

**Documentation Updates:**
```bash
# Update all references
rg -l 'exo\.py|exo |`exo' docs/ | while read f; do
    sd 'exo\.py' 'sinexctl' "$f"
    sd '`exo ' '`sinexctl ' "$f"
    sd 'exo ' 'sinexctl ' "$f"
done

# Update README quickstart
# docs/README.md, root README.md, etc.
```

**Migration Guide (create):**
```markdown
# docs/migration/python-cli-to-sinexctl.md

## Command Mapping

| Python CLI (old)              | Rust CLI (new)                    |
|-------------------------------|-----------------------------------|
| exo query ...                 | sinexctl replay plan --query ...  |
| exo replay submit <id>        | sinexctl replay submit <id>       |
| exo processor list            | sinexctl node list                |
| exo automaton list            | sinexctl node list --role synthesis |
| exo dlq list                  | sinexctl dlq ls                   |
| exo stats                     | sinexctl core health              |

## Breaking Changes

- No `--use-db` flag (all operations via RPC)
- Node vocabulary (not node/processor/automaton)
- JSON output format changed (see docs)
```

**Exit Criteria:**
- ✅ No Python CLI code remains in repository
- ✅ All documentation uses `sinexctl`
- ✅ Migration guide published
- ✅ README updated with new CLI examples

---

### 3.4. NixOS Module & Ops Alignment
**Priority:** HIGH
**Effort:** 2-3 days

**NixOS Module Rename:**
```bash
# Rename files
git mv nixos/modules/node-services.nix nixos/modules/node-services.nix

# Update default.nix
sd 'node-services' 'node-services' nixos/modules/default.nix
```

**Module Options:**
```nix
# Before:
services.sinex.nodes.terminal.enable = true;
services.sinex.nodes.desktop.enable = true;

# After:
services.sinex.nodes.capture.terminal.enable = true;
services.sinex.nodes.capture.desktop.enable = true;
services.sinex.nodes.synthesis.analytics.enable = true;
```

**Service Builders:**
```nix
# Before:
mknodeService = name: config: { /* ... */ };

# After:
mkNodeService = name: role: config: { /* ... */ };

# Per-role defaults:
mkCaptureNodeService = name: mkNodeService name "capture";
mkSynthesisNodeService = name: mkNodeService name "synthesis";
```

**Justfile:**
```bash
# Update targets
sd 'nodes' 'Nodes' justfile
sd 'node' 'node' justfile

# Example:
# Before:
# Start all nodes
# just nodes-start

# After:
# Start all nodes
# just nodes-start
```

**VM Test Scenarios:**
```bash
# Rename test files
git mv tests/e2e/nixos-vm/test-scenarios/node-matrix.nix \
       tests/e2e/nixos-vm/test-scenarios/node-matrix.nix

# Update scenario configs
sd 'node' 'node' tests/e2e/nixos-vm/test-scenarios/*.nix
```

**Exit Criteria:**
- ✅ NixOS module uses "node" vocabulary
- ✅ Service configuration works with new options
- ✅ Justfile targets updated
- ✅ VM test scenarios use new names
- ✅ All NixOS services start successfully

---

## Phase 4: Aspirational Features & `sx` Tool (Ongoing)

**Goal:** Implement advanced features that provide exceptional developer experience.

**Note:** These features are **aspirational** (desired future state), not required for production. They can be implemented incrementally after Phase 3 completes.

### 4.1. `SimpleProcessor` Trait (High Priority)
**Priority:** HIGH (unblocks rapid node development)
**Effort:** 2-3 weeks
**Value:** 90% of nodes don't need manual checkpoint/state management

**Current Problem:**
All nodes must implement `StatefulStreamProcessor` (now `Node`), which requires:
- Manual checkpoint management
- Explicit state serialization/deserialization
- Full NATS consumer group coordination
- Scan operation implementation

**Solution: High-Level Abstraction**
```rust
/// Simple processor for stateless or auto-managed state nodes
#[async_trait]
pub trait SimpleProcessor: Send + Sync {
    type Input: DeserializeOwned;
    type Output: Serialize;

    /// Process a single event
    async fn process(&mut self, input: Self::Input) -> Result<Option<Self::Output>>;

    /// Optional: custom error handling
    fn handle_error(&self, error: Error) -> ErrorAction {
        ErrorAction::SendToDLQ
    }
}

enum ErrorAction {
    Retry,
    SendToDLQ,
    Skip,
}

/// Auto-implementation of Node trait
impl<P> Node for SimpleProcessorWrapper<P>
where
    P: SimpleProcessor,
{
    // Automatically handles:
    // - NATS consumption
    // - Checkpoint persistence
    // - Provenance tracking
    // - Error routing (DLQ)
    // - State management (if needed)
}
```

**Example Usage:**
```rust
// Before (manual Node implementation): 50+ lines
impl Node for TerminalCanonicalizer {
    async fn initialize(&mut self, ctx: &mut StreamContext) -> Result<()> {
        // Load checkpoint manually
        // Setup NATS consumer
        // Initialize state
    }

    async fn process_batch(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        // Manual batch processing
        // Error handling
        // Checkpoint updates
    }

    // ... more boilerplate
}

// After (SimpleProcessor): 10 lines
struct TerminalCanonicalizer;

#[async_trait]
impl SimpleProcessor for TerminalCanonicalizer {
    type Input = TerminalCommandEvent;
    type Output = CanonicalCommandEvent;

    async fn process(&mut self, input: Self::Input) -> Result<Option<Self::Output>> {
        // Just the business logic
        let canonical = canonicalize_command(&input.command);
        Ok(Some(CanonicalCommandEvent { canonical }))
    }
}
```

**Auto-Plumbing Features:**
- NATS subscription + consumer group setup
- Checkpoint persistence to KV (auto-save every 10s or 1000 events)
- Provenance auto-assignment (synthesis from input event)
- DLQ routing on errors
- Metrics (events processed, errors, latency)
- Graceful shutdown (drain in-flight work)

**Migration Strategy:**
- Keep `Node` trait for complex cases (ingestd, fs-watcher)
- Provide `SimpleProcessor` for new nodes
- Gradually migrate existing simple nodes (canonicalizers, aggregators)

**Exit Criteria:**
- ✅ `SimpleProcessor` trait defined in node-sdk
- ✅ Auto-wrapper implements full `Node` trait
- ✅ At least 2 nodes migrated (terminal-canonicalizer, health-aggregator)
- ✅ Documentation + examples for new node developers

---

### 4.2. `sx` Unified Tool (Medium Priority)
**Priority:** MEDIUM (valuable but not blocking)
**Effort:** 2-4 months
**Value:** Unified developer experience like `cargo` or `git`

**Vision (from deployment paradigm doc):**

**`sx` is NOT `sinexctl`:**
- `sinexctl`: Thin RPC client for production operations (Phase 3)
- `sx`: Unified developer tool for local development + deployment (Phase 4)

**Command Structure:**
```bash
sx
  # Development
  dev [node]               # Holographic dev environment
  dev --tether prod        # Live debugging against production

  # Deployment
  deploy                   # Build artifacts from devenv.nix
  deploy --oci             # Build OCI container
  deploy --systemd         # Generate systemd units

  # Operations (wraps sinexctl for convenience)
  run seal                 # Seal current run, archive events
  run reset                # Reset to new run (truncate events)

  # Tooling
  schema check             # Verify schema consistency
  schema migrate           # Apply migrations
  tls bootstrap            # Generate TLS fixtures

  # Monitoring (TUI)
  monitor                  # Real-time dashboard
  logs [service]           # Tail logs
```

**Key Features:**

**1. Holographic Development Environment:**
```bash
$ sx dev analytics-automaton

# Auto-detection:
[sx] Detected: needs DB → starting pg_tmp
[sx] Detected: needs NATS → starting ephemeral nats-server
[sx] Detected: depends on terminal-node → starting mock terminal
[sx] Your automaton is running: http://localhost:9999
[sx] NATS: nats://localhost:4222
[sx] DB: postgresql:///sinex_dev?host=/tmp/pg_tmp_...
[sx] Mock terminal emitting events every 5s

# Auto-reload on code changes (preserve state)
[sx] File changed: src/main.rs
[sx] Stopping v1 gracefully...
[sx] V1 serialized state to /tmp/sx-state-...
[sx] Starting v2...
[sx] V2 resumed from checkpoint
```

**2. The Tether (Live Debugging):**
```bash
$ sx dev --tether prod

# Establishes mTLS tunnel to production
[sx] Connecting to prod gateway: https://prod.example.com:9999
[sx] Authenticating via mTLS...
[sx] Connected

# Shadow consumer (fan-out, not steal)
[sx] Creating shadow consumer group: dev-user-20250115
[sx] Subscribing to: sinex.events.terminal.*
[sx] Receiving production events (read-only)

# Writes redirected to local shadow tables
[sx] Warning: Writes will go to local DB only
[sx] Production data is READ-ONLY
```

**3. Deployment Artifact Generation:**
```bash
$ sx deploy --oci

# Reads devenv.nix, builds OCI container
[sx] Building container from devenv.nix
[sx] Services included:
     - sinex-ingestd
     - sinex-gateway
     - sinex-terminal-node
     - sinex-analytics-node
[sx] State volume: /var/lib/sinex (must be mounted)
[sx] Image: sinex:latest
[sx] Size: 487 MB

$ sx deploy --systemd

# Generates systemd units
[sx] Generated:
     - /etc/systemd/system/sinex-ingestd.service
     - /etc/systemd/system/sinex-gateway.service
     - /etc/systemd/system/sinex-terminal-node@.service (template)
[sx] State directory: /var/lib/sinex
[sx] Run: systemctl daemon-reload && systemctl start sinex-ingestd
```

**Implementation Notes:**
- `sx` wraps `cargo`, `nix`, `systemd`, `docker` as needed
- Uses `devenv.nix` as single source of truth
- Integrates with existing `xtask` commands (absorb xtask functionality)
- TUI built with `ratatui` crate

**Exit Criteria:**
- ✅ `sx dev` auto-detects dependencies and starts services
- ✅ `sx dev --tether` connects to production safely
- ✅ `sx deploy --oci` builds container from devenv.nix
- ✅ `sx monitor` provides real-time TUI dashboard
- ✅ Documentation for all sx commands

---

### 4.3. Wasm Runtime Integration (Medium Priority)
**Priority:** MEDIUM
**Effort:** 3-4 months
**Value:** Hot-swappable plugins, memory isolation for refinement logic

**Architecture (from deployment paradigm):**

**Core Logic:** Native binaries (Rust) for privileged I/O
**Refinement Logic:** Wasm modules (WASI) for transformations/enrichments

**Use Cases:**
- Custom canonicalizers (load without recompiling)
- Text extractors (PDF, DOCX) as plugins
- Entity recognition models
- Custom aggregators

**Integration Point: Gateway:**
```rust
// Gateway embeds Wasmtime runtime
use wasmtime::{Engine, Module, Store, Instance};
use wasmtime_wasi::WasiCtxBuilder;

struct WasmPlugin {
    instance: Instance,
    process_fn: TypedFunc<(u32, u32), u32>,
}

impl GatewayServer {
    fn load_plugin(&mut self, path: &Path) -> Result<WasmPlugin> {
        let engine = Engine::default();
        let module = Module::from_file(&engine, path)?;

        // Restrict capabilities
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .preopened_dir(Dir::open("./data")?, "/data")?
            .build();

        let mut store = Store::new(&engine, wasi);
        let instance = Instance::new(&mut store, &module, &[])?;

        // Get exported function
        let process_fn = instance
            .get_typed_func::<(u32, u32), u32>(&mut store, "process")?;

        Ok(WasmPlugin { instance, process_fn })
    }
}
```

**Plugin Interface:**
```rust
// Plugin SDK (compiles to Wasm)
#[no_mangle]
pub extern "C" fn process(input_ptr: *const u8, input_len: usize) -> *const u8 {
    // Deserialize input event
    let input: Event = from_bytes(input_ptr, input_len);

    // Process
    let output = transform(input);

    // Return output pointer
    to_bytes(&output)
}
```

**Capabilities & Sandboxing:**
```rust
// Plugins restricted to:
- NATS subjects (whitelist): sinex.events.{plugin_namespace}.*
- Filesystem (sandboxed): /data/{plugin_namespace}/
- Network: None (can be granted per-plugin)
- Memory: 128 MiB limit per instance
- CPU: Fuel metering (prevent infinite loops)
```

**Hot Reload:**
```bash
$ sx plugin reload text-extractor

[sx] Stopping old plugin instance...
[sx] Loading: ./plugins/text-extractor.wasm
[sx] Validating exports: process()
[sx] Starting new instance...
[sx] Plugin ready: text-extractor v2.1.0
```

**Exit Criteria:**
- ✅ Gateway embeds Wasmtime runtime
- ✅ Plugin SDK (Rust → Wasm) with examples
- ✅ Capability sandboxing (subjects, filesystem, memory)
- ✅ Hot reload without gateway restart
- ✅ At least 1 production plugin (e.g., PDF text extractor)

---

### 4.4. Standard Aggregation Runner (Medium Priority)
**Priority:** MEDIUM (from backlog #10)
**Effort:** 2-3 weeks
**Value:** Eliminates bespoke reducer logic in stateful automata

**Problem:**
Every stateful automaton (health, analytics, search) implements its own:
- State hydration from snapshots
- Reduce loop over events
- State persistence
- Replay handling

**Solution: Universal Runner**
```rust
/// Generic aggregation trait
#[async_trait]
pub trait Aggregator: Send + Sync {
    type State: Serialize + DeserializeOwned + Default;
    type Event: DeserializeOwned;
    type Output: Serialize;

    /// Reduce function: state + event → new state + optional output
    async fn reduce(
        &self,
        state: &mut Self::State,
        event: Self::Event,
    ) -> Result<Option<Self::Output>>;

    /// Optional: snapshot interval (default: every 1000 events)
    fn snapshot_interval(&self) -> usize {
        1000
    }
}

/// Universal runner (provided by SDK)
pub struct AggregationRunner<A: Aggregator> {
    aggregator: A,
    state: A::State,
    event_count: usize,
    last_snapshot: Instant,
}

impl<A: Aggregator> AggregationRunner<A> {
    /// Automatically handles:
    /// - NATS consumption
    /// - State hydration from KV snapshot
    /// - Reduce loop
    /// - Periodic state snapshots to KV
    /// - Replay isolation
    pub async fn run(&mut self) -> Result<()> {
        // Load latest snapshot from KV
        self.hydrate_state().await?;

        // Subscribe to input stream
        let mut sub = self.subscribe().await?;

        loop {
            let event = sub.next().await?;

            // Reduce
            if let Some(output) = self.aggregator
                .reduce(&mut self.state, event).await? {
                // Emit output event
                self.publish(output).await?;
            }

            self.event_count += 1;

            // Snapshot every N events
            if self.event_count % self.aggregator.snapshot_interval() == 0 {
                self.snapshot_state().await?;
            }
        }
    }
}
```

**Migration Example:**
```rust
// Before: Health Aggregator (bespoke logic, 200+ lines)
impl Node for HealthAggregator {
    // Manual state management
    // Manual snapshot logic
    // Manual replay handling
}

// After: Health Aggregator (just business logic, 30 lines)
struct HealthAggregatorLogic;

#[async_trait]
impl Aggregator for HealthAggregatorLogic {
    type State = HashMap<String, NodeHealthState>;
    type Event = HeartbeatEvent;
    type Output = HealthAlertEvent;

    async fn reduce(
        &self,
        state: &mut Self::State,
        event: HeartbeatEvent,
    ) -> Result<Option<HealthAlertEvent>> {
        // Update node health
        state.insert(event.node_id.clone(), event.into());

        // Detect failures
        let alerts = state.values()
            .filter(|h| h.is_unhealthy())
            .map(|h| HealthAlertEvent { node_id: h.node_id.clone() })
            .collect();

        Ok(Some(alerts))
    }
}
```

**Exit Criteria:**
- ✅ `Aggregator` trait defined in node-sdk
- ✅ `AggregationRunner` implements universal logic
- ✅ At least 2 automata migrated (health, analytics)
- ✅ Snapshots persist to KV automatically
- ✅ Replay isolation works correctly

---

### 4.5. Additional Backlog Items (Lower Priority)

**From consolidated-backlog.md, internalized:**

**Transaction Isolation Audit (Backlog #4):**
- Status: Most critical paths use REPEATABLE READ
- Action: Audit remaining mutators, apply where needed
- Priority: Medium (security/correctness)

**Hot-Path Allocation Audit (Backlog #11):**
- Status: Deep analysis found 786 `.clone()` calls
- Action: Profile ingestd + event persistence, reduce clones
- Priority: Low (performance optimization)

**JetStream Soak Tests in CI (Backlog #7):**
- Status: `profile.soak` exists, not wired to CI
- Action: Add scheduled CI runs, alert on failures
- Priority: Low (operational confidence)

**Universal Event Processing Middleware (Backlog #11):**
- Action: Composable validation → enrichment → transformation chain
- Priority: Low (architectural cleanup)

**Stateful Automata Sharding (Backlog #12):**
- Action: Consistent hashing router for JetStream subjects
- Priority: Low (scalability feature)

**HTTP Dependency Consolidation (Backlog Dependencies #1):**
- Status: Mixed tower 0.4/0.5, axum 0.6/0.7
- Action: Align on latest versions
- Priority: Low (dependency hygiene)

**sinex-core Flattening Guardrails (Backlog Core Surface #1):**
- Action: Add xtask commands for depgraph, timings, udeps
- Priority: Low (developer visibility)

**Schema Drift Control (Backlog Schema #1):**
- Action: Enforce changelog blocks for payload changes
- Priority: Low (process improvement)

---

## Phase 5: Ongoing Polish & Operations (Continuous)

**Goal:** Observability, performance, advanced features not blocking production.

### 5.1. Observability & Tracing
**Priority:** LOW
**Value:** Production debugging, performance insights

**OpenTelemetry Integration:**
- Tracing propagation (gateway → services → ingestd)
- Metrics export (Prometheus format)
- OTLP/Jaeger integration for distributed tracing
- Per-node metrics (Stage-as-You-Go handles, JetStream lag)

**Implementation:**
```rust
use tracing_opentelemetry::OpenTelemetryLayer;
use opentelemetry_otlp::WithExportConfig;

// In gateway/ingestd/nodes:
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    .install_simple()?;

tracing_subscriber::registry()
    .with(OpenTelemetryLayer::new(tracer))
    .init();
```

---

### 5.2. Performance Optimization
**Priority:** LOW
**Value:** Improved throughput, lower latency

**Hot-Path Optimizations:**
- Clone audit: Reduce `.clone()` calls in ingestion path
- Zero-copy deserialization: Use `&[u8]` references where safe
- Buffer pooling: Reuse allocations in material assembler
- Batch processing: Increase batch sizes for DB inserts

**Profiling:**
```bash
# CPU profiling
cargo flamegraph --bin sinex-ingestd

# Memory profiling
cargo run --bin sinex-ingestd --release
# Then use heaptrack/valgrind
```

---

### 5.3. Advanced Testing
**Priority:** LOW
**Value:** Higher confidence, fewer regressions

**Fuzzing:**
```bash
# Add fuzz/ directory with libFuzzer harnesses
cargo install cargo-fuzz
cargo fuzz add fuzz_event_payload
cargo fuzz add fuzz_material_assembly

# Run continuously in CI
cargo fuzz run fuzz_event_payload -- -max_total_time=300
```

**Property Tests for Database:**
```rust
// Test database invariants
proptest! {
    #[test]
    fn event_insertion_preserves_ulid_ordering(events in vec(arbitrary_event(), 1..100)) {
        // Insert events
        // Query back
        // Assert: returned order matches ULID order
    }
}
```

**Chaos Engineering:**
```bash
# Network partitions
sx test --chaos network-partition

# Process crashes
sx test --chaos kill-random-node

# Clock skew
sx test --chaos clock-skew-5s
```

---

## Execution Principles

### Principle 1: Finish Before Starting
**Rule:** Complete each phase before moving to next.
**Why:** Prevents accumulation of half-finished work.
**Enforcement:** Gate phase transitions on exit criteria.

### Principle 2: No Shims for Compatibility
**Rule:** Phase 3 breaks old CLI; no aliases or compatibility layers.
**Why:** Forces completion, avoids indefinite migration.
**Enforcement:** Delete old code immediately after new code works.

### Principle 3: Ship Incrementally
**Rule:** Each phase ships a working system.
**Why:** Maintains releasability; no "big bang" integration.
**Enforcement:** CI must be green after each phase.

### Principle 4: Test Everything
**Rule:** Every migration must have passing tests.
**Why:** Prevents regressions; maintains confidence.
**Enforcement:** `cargo xtask test --profile reliable` in exit criteria.

### Principle 5: Document As You Go
**Rule:** Update docs immediately when code changes.
**Why:** Prevents documentation drift.
**Enforcement:** Doc updates in same commit as code changes.

---

## Success Metrics

**Phase 1 Complete:**
- [ ] All tests pass without legacy DB tables
- [ ] Gateway exposes all RPC endpoints (DLQ, nodes, ops, audit)
- [ ] Zero `create_test_event` call sites remain
- [ ] All configs use `Seconds`/`Bytes` newtypes

**Phase 2 Complete:**
- [ ] Desktop node uses native APIs (no shell-outs)
- [ ] System node uses native journal APIs
- [ ] mTLS enforced in CI
- [ ] All events contain git revision

**Phase 3 Complete:**
- [ ] Codebase uses "node" vocabulary universally
- [ ] `sinexctl` binary ships with all MVP commands
- [ ] Python CLI deleted
- [ ] NixOS modules updated

**Phase 4 Progress:**
- [ ] `SimpleProcessor` implemented and documented
- [ ] `sx dev` functional (auto-detection works)
- [ ] Wasm plugin system demonstrated (1+ plugin)

**Phase 5 Ongoing:**
- [ ] OpenTelemetry tracing active in production
- [ ] Performance benchmarks tracked in CI
- [ ] Fuzzing runs continuously

---

## Timeline & Resource Estimates

| Phase | Duration | Effort (person-weeks) | Risk | Can Parallelize |
|-------|----------|----------------------|------|-----------------|
| **Phase 1** | 3-4 weeks | 6-8 | Low | High (split 1.1/1.2/1.3/1.4) |
| **Phase 2** | 4-6 weeks | 8-12 | Medium | Medium (2.1/2.2 can parallel) |
| **Phase 3** | 2-3 weeks | 4-6 | Medium | Low (sequential: 3.1→3.2→3.3) |
| **Phase 4** | Ongoing | 12-20 | Low | High (4.1/4.2/4.3 independent) |
| **Phase 5** | Continuous | Variable | Low | High |

**Total to Production-Ready (Phases 1-3):** 9-13 weeks, 18-26 person-weeks

**Critical Path:**
1. Weeks 1-2: KV coordination + Gateway RPC endpoints (Phase 1.1-1.2)
2. Weeks 3-4: Test modernization + Config hygiene (Phase 1.3-1.4)
3. Weeks 5-8: Native nodes + Security (Phase 2.1-2.3)
4. Weeks 9-10: Backend rename + Rust CLI (Phase 3.1-3.2)
5. Weeks 11-12: Python deletion + NixOS updates (Phase 3.3-3.4)
6. Week 13: Buffer for slippage

---

## Conclusion

This plan provides **the definitive roadmap** for Sinex evolution. It:

1. **Finishes infrastructure** (Phases 1-2) to eliminate hybrid state
2. **Executes decisive cutover** (Phase 3) for naming/CLI with no compatibility debt
3. **Defines aspirational features** (Phase 4) with concrete implementation details
4. **Provides continuous improvement path** (Phase 5) for long-term excellence

**Key Differentiators:**
- ✅ Concrete automation (SSR recipes, sd commands, SQL migrations)
- ✅ Clear separation (`sinexctl` vs `sx` clarified)
- ✅ Internalized backlog (no external tracking documents needed)
- ✅ Aspirational features preserved with detail (not just "defer")

**Next Actions:**
1. Review and approve this plan
2. Assign Phase 1 tasks to team
3. Set milestone: Phase 1 complete in 3-4 weeks
4. Start with Phase 1.1 (Finish KV Coordination)

**This is the only plan. All previous plans are superseded.**
