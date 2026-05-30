## Node Patterns

### Choosing Your Node Type

| You're building... | Use | Key trait |
|--------------------|-----|-----------|
| Raw data capture from external source | `SourceUnit` + `SourceUnitRuntime` | 3 scan modes + continuous |
| 1:1 event transformation | `Transducer` + `AutomatonRuntime` | Stateless process() |
| Accumulate-then-emit (sessions, summaries) | `Windowed` + `AutomatonRuntime` | accumulate() + emit_window() |
| Per-scope state reconciliation | `ScopeReconciler` + `AutomatonRuntime` | Per-scope state + reconcile() |

Automata are registered via `AutomatonSpec` in `automata::registry` and driven by `NodeCliRunner`. Source units are registered via `register_node_factory!` / `register_adapter_ingestor!` and driven by `NodeCliRunner` through `sources::bindings`.

### Ingestor Pattern

```rust
use serde::{Deserialize, Serialize};
use crate::node_sdk::{SourceUnit, SourceUnitRuntime};
use crate::node_sdk::runtime::stream::*;
use tokio::sync::watch;

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state — persisted automatically */ }

#[derive(Default)]
struct MyIngestor;

impl SourceUnit for MyIngestor {
    type Config = serde_json::Value;
    type State = MyState;

    fn name(&self) -> &str { "my-ingestor" }

    async fn initialize(&mut self, state: &mut Self::State, _config: Self::Config,
        _runtime: &NodeRuntimeState) -> crate::node_sdk::NodeResult<()> { Ok(()) }

    async fn scan_snapshot(&mut self, _state: &mut Self::State,
        _args: ScanArgs) -> crate::node_sdk::NodeResult<ScanReport> { Ok(ScanReport::empty()) }

    async fn scan_historical(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _until: TimeHorizon, _args: ScanArgs) -> crate::node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }

    async fn run_continuous(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _shutdown_rx: watch::Receiver<bool>) -> crate::node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }
}

pub type MyIngestorNode = SourceUnitRuntime<MyIngestor>;
```

### Derived Node Pattern (Transducer — Stateless)

```rust
use crate::node_sdk::{Transducer, AutomatonRuntime, NodeLogicError};
use crate::node_sdk::derived_node::AutomatonContext;

struct MyTransducer;

impl Transducer for MyTransducer {
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
use crate::node_sdk::{Windowed, AutomatonRuntime, DerivedOutput, NodeLogicError};
use crate::node_sdk::derived_node::AutomatonContext;

struct SessionDetector;

impl Windowed for SessionDetector {
    type State = SessionState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "session-detector" }
    fn input_event_type(&self) -> &'static str { "*" }
    fn output_event_type(&self) -> &'static str { "activity.session.boundary" }

    // Accumulate events into the window state.
    async fn accumulate(&mut self, state: &mut Self::State, input: Self::Input,
        ctx: &AutomatonContext) -> Result<(), NodeLogicError>
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
use crate::node_sdk::{
    // Core node types
    SourceUnit, SourceUnitRuntime,
    Transducer, Windowed, ScopeReconciler,
    AutomatonRuntime,
    // Config + CLI
    NodeConfig, NodeCli, NodeCliRunner,
    // Runtime
    CheckpointManager,
    NatsPublisher, HeartbeatEmitter, DlqRetryHandler,
    NodeCoordination, InstanceMode,
    SelfObserver, SelfObserverConfig,
    // Health
    HealthReporter, HealthMetrics,
};
```

Reference: `crate/sinexd/src/node_sdk/` (the node SDK lives inline in sinexd post-collapse)
