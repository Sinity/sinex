# Aspirational SDK Features

These features represent the long-term vision for Sinex SDK improvements. They are not blocking production but would significantly improve developer experience.

---

## Prompt-to-Node Development

**The SDK becomes so simple that a prompt template is all you need to define a new node.**

```
User: "I need a node that detects git activity from terminal commands"

→ LLM generates AutomatonNode (10 lines)
→ Hot reload picks it up
→ State persists across iterations
→ Test with real production data via Tether
→ Iterate until correct
```

**Bespoke event-sourced data flows become routine** because:
1. AutomatonNode is trivial enough for LLM to generate reliably
2. Seamless Developer Experience means no manual compile/deploy friction
3. The Tether provides real data for immediate validation

### The Workflow

**Step 1: Describe what you want**
```markdown
# Node Spec: Git Activity Detector

## Input
- terminal.command.executed events

## Logic
- Filter commands starting with "git"
- Extract subcommand (commit, push, pull, etc.)
- Track repo path from cwd

## Output
- git.activity.detected events with { subcommand, repo_path, timestamp }
```

**Step 2: LLM generates AutomatonNode**
```rust
struct GitActivityDetector;

#[async_trait]
impl AutomatonNode for GitActivityDetector {
    type State = GitActivityState;
    type Input = TerminalCommandEvent;
    type Output = GitActivityEvent;

    async fn process(&mut self, state: &mut Self::State, input: Self::Input) -> Result<Option<Self::Output>> {
        if !input.command.starts_with("git ") {
            return Ok(None);
        }
        let subcommand = input.command.split_whitespace().nth(1).unwrap_or("unknown");
        Ok(Some(GitActivityEvent {
            subcommand: subcommand.to_string(),
            repo_path: input.cwd,
            timestamp: input.timestamp,
        }))
    }
}
```

**Step 3: Hot reload + real data**
```bash
$ xtask dev git-activity-detector --tether prod

[xtask] Generated node from spec...
[xtask] Building...
[xtask] Running with production terminal events...
[xtask] Event: git.activity.detected { subcommand: "status", repo: "/home/user/sinex" }
```

**Step 4: Iterate until correct**
- See real behavior with real data
- Adjust spec or code
- Hot reload (state preserved)
- Repeat

No manual compilation. No synthetic test data. No lost state.

### Why This Works

**AutomatonNode is LLM-friendly:**
- Single `process()` method
- Clear input/output types
- State is just a struct with Serialize/Deserialize
- No async complexity (SDK handles it)
- No NATS/checkpoint boilerplate

**The Tether validates instantly:**
- LLM-generated code might have edge cases
- Real production patterns expose: unusual formats, unicode, concurrent events, timing

---

## Seamless Developer Experience: The Infrastructure

**The system IS the development environment.**

Current model:
```
DEVELOPER ──> Write Rust ──> cargo build ──> restart service ──> lose state ──> replay events
```

Integrated model:
```
DEVELOPER ──> Edit Rust ──> [auto-rebuilds] ──> [state transfers] ──> continue from where you were
                              (invisible)       (automatic)
```

### The Three Pillars

1. **Invisible Compilation** (`xtask dev`): File watcher triggers rebuild, state serializes automatically, new binary resumes from checkpoint.

2. **The Tether** (`xtask dev --tether prod`): Connect to production event streams for testing with real data, writes go to local shadow DB.

3. **State Continuity**: State survives code changes, crashes, restarts, and version upgrades through automatic checkpoint management.

### Inspirations

| System | Insight |
|--------|---------|
| **Smalltalk** | The image IS development. Code changes are live. |
| **Erlang/OTP** | Hot code reload. Running system accepts new modules without restart. |
| **Jupyter** | Interactive exploration with persistent state across cell executions. |

All features below enable prompt-to-node development. AutomatonNode is the LLM-friendly API. xtask orchestrates the integrated experience. The Tether provides real data validation. Wasm plugins offer instant reload for non-Rust logic.

---

## 1. AutomatonNode Trait

**Goal:** 90% of nodes don't need manual checkpoint/state management. Provide a high-level abstraction with **automatic state persistence that survives hot reload**.

### Current Problem

All nodes must implement `Node` trait, which requires:
- Manual checkpoint management
- Explicit state serialization/deserialization
- Full NATS consumer group coordination
- Scan operation implementation

### Solution: High-Level Abstraction with State

```rust
/// Node logic with auto-managed state
#[async_trait]
pub trait AutomatonNode: Send + Sync {
    /// Custom state - automatically persisted and restored
    type State: Serialize + DeserializeOwned + Default;
    type Input: DeserializeOwned;
    type Output: Serialize;

    /// Process a single event with access to persistent state
    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
    ) -> Result<Option<Self::Output>>;

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
impl<P> Node for AutomatonNodeAdapter<P>
where
    P: AutomatonNode,
{
    // Automatically handles:
    // - NATS consumption
    // - Checkpoint persistence (includes custom state)
    // - Provenance tracking
    // - Error routing (DLQ)
    // - State serialization on SIGTERM
    // - State restoration from --restore-state flag
}
```

### Example Usage

```rust
// Developer's node - just implement business logic
#[derive(Serialize, Deserialize, Default)]
struct MyState {
    command_counts: HashMap<String, u64>,
    last_seen: Option<DateTime<Utc>>,
}

struct TerminalCanonicalizer;

#[async_trait]
impl AutomatonNode for TerminalCanonicalizer {
    type State = MyState;
    type Input = TerminalCommandEvent;
    type Output = CanonicalCommandEvent;

    async fn process(
        &mut self,
        state: &mut MyState,
        input: Self::Input,
    ) -> Result<Option<Self::Output>> {
        // Business logic only - state is auto-persisted
        *state.command_counts.entry(input.command.clone()).or_default() += 1;
        state.last_seen = Some(Utc::now());

        let canonical = canonicalize_command(&input.command);
        Ok(Some(CanonicalCommandEvent { canonical }))
    }
}
```

### Auto-Plumbing Features

- NATS subscription + consumer group setup
- Checkpoint persistence to KV (auto-save every 10s or 1000 events)
- **State serialization on SIGTERM** (enables hot reload)
- **State restoration from temp file** (continues after rebuild)
- Provenance auto-assignment (synthesis from input event)
- DLQ routing on errors
- Metrics (events processed, errors, latency)
- Graceful shutdown (drain in-flight work)

### Migration Strategy

- Keep `Node` trait for complex cases (ingestd, fs-watcher)
- Provide `AutomatonNode` for new nodes
- Gradually migrate existing simple nodes (canonicalizers, aggregators)

---

## 2. Standard Aggregation Runner

**Goal:** Eliminate bespoke reducer logic in stateful automata, with **state that survives hot reload**.

### Problem

Every stateful automaton (health, analytics, search) implements its own:
- State hydration from snapshots
- Reduce loop over events
- State persistence
- Replay handling

### Solution: Universal Runner

```rust
/// Generic aggregation trait
#[async_trait]
pub trait Aggregator: Send + Sync {
    type State: Serialize + DeserializeOwned + Default;
    type Event: DeserializeOwned;
    type Output: Serialize;

    /// Reduce function: state + event -> new state + optional output
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
    /// - State hydration from KV snapshot OR temp file (hot reload)
    /// - Reduce loop
    /// - Periodic state snapshots to KV
    /// - State serialization on SIGTERM
    /// - Replay isolation
    pub async fn run(&mut self) -> Result<()> {
        // Load state (KV snapshot or --restore-state temp file)
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

### Migration Example

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

---

## 3. `xtask` Integrated Developer Tool

**Goal:** Single binary that makes the compile/deploy cycle invisible.

**Note:** This is distinct from `sinexctl` (production RPC client). `xtask` is the integrated development orchestrator.

### Command Structure

```bash
xtask
  # Development (core integrated experience)
  dev [node]               # Integrated dev environment
  dev --tether prod        # Live debugging against production

  # Operations (wraps sinexctl for convenience)
  run seal                 # Seal current run, archive events
  run reset                # Reset to new run (truncate events)

  # Tooling
  schema check             # Verify schema consistency
  schema migrate           # Apply migrations
  tls bootstrap            # Generate TLS fixtures
  plugin reload [name]     # Hot-reload Wasm plugin

  # Monitoring (TUI)
  monitor                  # Real-time dashboard
  logs [service]           # Tail logs
```

### Integrated Development Environment

```bash
$ xtask dev analytics-automaton

# Auto-detection:
[xtask] Detected: needs DB -> starting pg_tmp
[xtask] Detected: needs NATS -> starting ephemeral nats-server
[xtask] Detected: depends on terminal-node -> starting mock terminal
[xtask] Your automaton is running: http://localhost:9999
[xtask] NATS: nats://localhost:4222
[xtask] DB: postgresql:///sinex_dev?host=/tmp/pg_tmp_...
[xtask] Mock terminal emitting events every 5s
[xtask] Watching src/ for changes...

# Developer edits src/main.rs

[xtask] File changed: src/main.rs
[xtask] V1 graceful shutdown, serializing state...
[xtask] State saved: 47 events processed, checkpoint at uuid_xyz
[xtask] Building V2...
[xtask] V2 started, restored from checkpoint
[xtask] Continuing from event 48...
```

**What happens invisibly:**
1. File watcher (notify-rs) detects change
2. Current process receives graceful shutdown signal
3. Process serializes CheckpointState + custom state to temp file
4. `cargo build` runs in background
5. New binary starts with `--restore-state /tmp/xtask-state-xxx`
6. Processing continues from exact position

---

## 4. The Tether (Live Production Debugging)

**Goal:** Test against real production events without affecting production.

This is a major feature that enables debugging with actual production data patterns.

### How It Works

```bash
$ xtask dev --tether prod terminal-canonicalizer

# Establishes mTLS tunnel to production
[xtask] Connecting to prod gateway: https://prod.example.com:9999
[xtask] Authenticating via mTLS...
[xtask] Connected ✓

# Shadow consumer (fan-out, not steal)
[xtask] Creating shadow consumer: dev-sinity-20250117
[xtask] Subscribing to: sinex.events.terminal.* (read-only fan-out)
[xtask] Local writes → shadow DB only

[xtask] Receiving production events...
[xtask] Event #1: terminal.command.executed (git status)
[xtask] Event #2: terminal.command.executed (cargo build)
...
```

### Architecture

```
Production                          Development Machine
┌─────────────┐                    ┌─────────────────────────┐
│ NATS Cluster│                    │  xtask dev --tether prod│
│             │                    │                         │
│  terminal.* ├────────────────────┤  Shadow Consumer        │
│  events     │   mTLS tunnel      │  (fan-out, not steal)   │
│             │                    │                         │
└─────────────┘                    │         │               │
                                   │         v               │
┌─────────────┐                    │  ┌─────────────────┐    │
│ Production  │                    │  │ Your Node       │    │
│ PostgreSQL  │  (read-only)       │  │ (local process) │    │
│             │◄───────────────────│  └────────┬────────┘    │
└─────────────┘                    │           │             │
                                   │           v             │
                                   │  ┌─────────────────┐    │
                                   │  │ Shadow DB       │    │
                                   │  │ (local pg_tmp)  │    │
                                   │  └─────────────────┘    │
                                   └─────────────────────────┘
```

### What This Enables

- Test against REAL production event patterns
- No need to craft synthetic test data
- Debug issues with actual problematic events
- Shadow tables prevent production pollution
- Combined with hot reload: edit code, see results with real data

---

## 5. Wasm Runtime Integration

**Goal:** Hot-swappable plugins with memory isolation - instant reload for extension logic.

### Architecture

- **Core Logic:** Native binaries (Rust) for privileged I/O
- **Refinement Logic:** Wasm modules (WASI) for transformations/enrichments

Wasm plugins complement the seamless Developer Experience by providing instant reload for extension logic without any compilation wait.

### Use Cases

- Custom canonicalizers (load without recompiling)
- Text extractors (PDF, DOCX) as plugins
- Entity recognition models
- Custom aggregators

### Integration Point: Gateway

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

### Capabilities & Sandboxing

```rust
// Plugins restricted to:
- NATS subjects (whitelist): sinex.events.{plugin_namespace}.*
- Filesystem (sandboxed): /data/{plugin_namespace}/
- Network: None (can be granted per-plugin)
- Memory: 128 MiB limit per instance
- CPU: Fuel metering (prevent infinite loops)
```

### Hot Reload

```bash
$ xtask plugin reload text-extractor

[xtask] Stopping old plugin instance...
[xtask] Loading: ./plugins/text-extractor.wasm
[xtask] Validating exports: process()
[xtask] Starting new instance...
[xtask] Plugin ready: text-extractor v2.1.0
```

---

## Design Principles

These principles guide Sinex development:

### 1. Finish Before Starting
Complete each phase before moving to next. Prevents accumulation of half-finished work.

### 2. No Shims for Compatibility
Breaking changes break cleanly. No aliases or compatibility layers that prolong migration.

### 3. Ship Incrementally
Each change ships a working system. No "big bang" integration.

### 4. Test Everything
Every migration must have passing tests. Maintains confidence.

### 5. Document As You Go
Update docs immediately when code changes. Prevents drift.

---

## What Already Exists (Infrastructure)

| Component | Status | Location |
|-----------|--------|----------|
| CheckpointState with custom data | ✅ | `sinex-node-sdk/src/checkpoint.rs` |
| NATS KV checkpoint persistence | ✅ | `sinex-node-sdk/src/checkpoint.rs` |
| Graceful shutdown in SDK | ✅ | `sinex-node-sdk/src/runtime/` |
| mTLS gateway auth | ✅ | `sinex-gateway/` |
| Replay state machine | ✅ | `sinex-db/src/replay/` |
| NodeRuntimeState | ✅ | `sinex-node-sdk/src/runtime/stream/runtime_state.rs` |

## What's Missing (To Build)

| Component | Description |
|-----------|-------------|
| State-on-SIGTERM hook | Serialize custom state when receiving shutdown signal |
| `--restore-state` flag | CLI flag to load state from temp file on startup |
| File watcher integration | notify-rs watching src/, triggering rebuild |
| Process supervisor | Manages child process lifecycle, state handoff |
| Shadow consumer API | Gateway endpoint to create read-only fan-out consumer |
| Shadow DB routing | Redirect writes to local DB when tethered |
| `xtask` CLI scaffolding | Unified dev tool binary |
