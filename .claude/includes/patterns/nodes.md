## Node Patterns

### Choosing Your Node Type

| You're building... | Use | Key trait |
|--------------------|-----|-----------|
| Raw data capture from external source | `IngestorNode` + `IngestorNodeAdapter` | 3 scan modes + continuous |
| 1:1 event transformation | `TransducerNode` + `DerivedNodeAdapter` | Stateless process() |
| Accumulate-then-emit (sessions, summaries) | `WindowedNode` + `DerivedNodeAdapter` | accumulate() + emit_window() |
| Per-scope state reconciliation | `ScopeReconcilerNode` + `DerivedNodeAdapter` | Per-scope state + reconcile() |

All nodes use `node_entrypoint!` macro for CLI, lifecycle, sd_notify, heartbeat.

### Ingestor Pattern

```rust
use serde::{Deserialize, Serialize};
use sinex_node_sdk::{IngestorNode, IngestorNodeAdapter};
use sinex_node_sdk::runtime::stream::*;
use tokio::sync::watch;

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state — persisted automatically */ }

#[derive(Default)]
struct MyIngestor;

impl IngestorNode for MyIngestor {
    type Config = serde_json::Value;
    type State = MyState;

    fn name(&self) -> &str { "my-ingestor" }

    async fn initialize(&mut self, state: &mut Self::State, _config: Self::Config,
        _runtime: &NodeRuntimeState) -> sinex_node_sdk::NodeResult<()> { Ok(()) }

    async fn scan_snapshot(&mut self, _state: &mut Self::State,
        _args: ScanArgs) -> sinex_node_sdk::NodeResult<ScanReport> { Ok(ScanReport::empty()) }

    async fn scan_historical(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _until: TimeHorizon, _args: ScanArgs) -> sinex_node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }

    async fn run_continuous(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _shutdown_rx: watch::Receiver<bool>) -> sinex_node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }
}

pub type MyIngestorNode = IngestorNodeAdapter<MyIngestor>;
```

### Derived Node Pattern (Transducer — Stateless)

```rust
use sinex_node_sdk::{TransducerNode, DerivedNodeAdapter, NodeEventContext, NodeLogicError};

struct MyTransducer;

impl TransducerNode for MyTransducer {
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "my-transducer" }
    fn input_event_type(&self) -> &'static str { "command.executed" }
    fn output_event_type(&self) -> &'static str { "command.canonical" }

    async fn process(&mut self, input: Self::Input, _ctx: &NodeEventContext)
        -> Result<Option<Self::Output>, NodeLogicError>
    {
        // Return Some(output) to emit, None to filter
        Ok(Some(transform(input)))
    }
}
```

### Derived Node Pattern (Windowed — Accumulate Then Emit)

```rust
use sinex_node_sdk::{WindowedNode, DerivedNodeAdapter, DerivedOutput, NodeLogicError};
use sinex_node_sdk::automaton_node::DerivedTriggerContext;  // TODO: extract from deprecated module

struct SessionDetector;

impl WindowedNode for SessionDetector {
    type State = SessionState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "session-detector" }
    fn input_event_type(&self) -> &'static str { "*" }
    fn output_event_type(&self) -> &'static str { "activity.session.boundary" }

    // Accumulate events into the window state.
    async fn accumulate(&mut self, state: &mut Self::State, input: Self::Input,
        ctx: &DerivedTriggerContext) -> Result<(), NodeLogicError>
    {
        state.events.push(input);
        state.last_ts = Some(ctx.event_timestamp());
        Ok(())
    }

    // Check if the window should emit.
    fn window_complete(&self, state: &Self::State) -> bool {
        state.last_ts.map_or(false, |last| {
            Timestamp::now() - last > Duration::minutes(5)
        })
    }

    // Emit session boundary from accumulated state.
    async fn emit(&mut self, state: &mut Self::State)
        -> Result<Option<DerivedOutput>, NodeLogicError>
    {
        Ok(Some(DerivedOutput::windowed(json!({
            "start_time": state.start_ts,
            "end_time": state.last_ts,
            "event_count": state.events.len(),
        }))))
    }
}
```

### Node SDK Components Reference

```rust
use sinex_node_sdk::{
    // Core node types
    IngestorNode, IngestorNodeAdapter,
    TransducerNode, WindowedNode, ScopeReconcilerNode,
    DerivedNodeAdapter,
    // Config + CLI
    NodeConfig, NodeArgs, NodeCli, NodeCliRunner, node_entrypoint,
    // Runtime
    CheckpointManager, LifecycleManager, ServiceStatus,
    NatsPublisher, HeartbeatEmitter, DlqRetryHandler,
    NodeCoordination, InstanceMode,
    SelfObserver, SelfObserverConfig,
    ShutdownHandler, ShutdownSignal,
    // Storage
    AnnexConfig, GitAnnex, BlobManager,
    // Health
    HealthReporter, HealthMetrics,
};
```

Reference: `crate/lib/sinex-node-sdk/docs/overview.md`
