# Aspirational SDK Features

These features represent the long-term vision for Sinex SDK improvements. They are not blocking production but would significantly improve developer experience.

---

## 1. SimpleProcessor Trait

**Goal:** 90% of nodes don't need manual checkpoint/state management. Provide a high-level abstraction.

### Current Problem

All nodes must implement `Node` trait, which requires:
- Manual checkpoint management
- Explicit state serialization/deserialization
- Full NATS consumer group coordination
- Scan operation implementation

### Solution: High-Level Abstraction

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

### Example Usage

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

### Auto-Plumbing Features

- NATS subscription + consumer group setup
- Checkpoint persistence to KV (auto-save every 10s or 1000 events)
- Provenance auto-assignment (synthesis from input event)
- DLQ routing on errors
- Metrics (events processed, errors, latency)
- Graceful shutdown (drain in-flight work)

### Migration Strategy

- Keep `Node` trait for complex cases (ingestd, fs-watcher)
- Provide `SimpleProcessor` for new nodes
- Gradually migrate existing simple nodes (canonicalizers, aggregators)

---

## 2. Standard Aggregation Runner

**Goal:** Eliminate bespoke reducer logic in stateful automata.

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

## 3. `sx` Unified Developer Tool

**Goal:** Single binary for all development and operations.

**Note:** This is distinct from `sinexctl` (production RPC client). `sx` is for local development.

### Command Structure

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

### Holographic Development Environment

```bash
$ sx dev analytics-automaton

# Auto-detection:
[sx] Detected: needs DB -> starting pg_tmp
[sx] Detected: needs NATS -> starting ephemeral nats-server
[sx] Detected: depends on terminal-node -> starting mock terminal
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

### The Tether (Live Debugging)

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

### Deployment Artifact Generation

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

---

## 4. Wasm Runtime Integration

**Goal:** Hot-swappable plugins with memory isolation.

### Architecture

- **Core Logic:** Native binaries (Rust) for privileged I/O
- **Refinement Logic:** Wasm modules (WASI) for transformations/enrichments

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
$ sx plugin reload text-extractor

[sx] Stopping old plugin instance...
[sx] Loading: ./plugins/text-extractor.wasm
[sx] Validating exports: process()
[sx] Starting new instance...
[sx] Plugin ready: text-extractor v2.1.0
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

## Implementation Priority

| Feature | Effort | ROI | Dependencies |
|---------|--------|-----|--------------|
| SimpleProcessor | 2-3 weeks | High | None |
| Aggregation Runner | 2-3 weeks | High | SimpleProcessor helpful |
| `sx dev` | 3-4 weeks | High | None |
| `sx deploy` | 2-3 weeks | Medium | `sx` foundation |
| Wasm Runtime | 4-6 weeks | Medium | Gateway stable |
