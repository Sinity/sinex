## Source And Automaton Runtime Patterns

### Choosing Your Runtime Shape

| You're building... | Use | Key trait |
|--------------------|-----|-----------|
| Raw data capture from external source | `SourceDriver` + `SourceDriverRuntime` | 3 scan modes + continuous |
| 1:1 event transformation | `Transducer` + `AutomatonRuntime` | Stateless process() |
| Accumulate-then-emit (sessions, summaries) | `Windowed` + `AutomatonRuntime` | accumulate() + emit_window() |
| Per-scope state reconciliation | `ScopeReconciler` + `AutomatonRuntime` | Per-scope state + reconcile() |

Automata are registered via `AutomatonSpec` in `automata::registry`. Source
units are semantic capture/parser contracts registered via
`register_source_contract!`; deployment bindings come from
`register_source_runtime_binding!` plus the NixOS-generated binding manifest.
The historical `RuntimeCliRunner`/`runtime` names still exist in code, but do not
imply a separate node crate or per-source systemd unit.

### Ingestor Pattern

```rust
use serde::{Deserialize, Serialize};
use crate::runtime::{SourceDriver, SourceDriverRuntime};
use crate::runtime::stream::*;
use tokio::sync::watch;

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state — persisted automatically */ }

#[derive(Default)]
struct MyIngestor;

impl SourceDriver for MyIngestor {
    type Config = serde_json::Value;
    type State = MyState;

    fn name(&self) -> &str { "my-ingestor" }

    async fn initialize(&mut self, state: &mut Self::State, _config: Self::Config,
        _runtime: &RuntimeContext) -> crate::runtime::RuntimeResult<()> { Ok(()) }

    async fn scan_snapshot(&mut self, _state: &mut Self::State,
        _args: ScanArgs) -> crate::runtime::RuntimeResult<ScanReport> { Ok(ScanReport::empty()) }

    async fn scan_historical(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _until: TimeHorizon, _args: ScanArgs) -> crate::runtime::RuntimeResult<ScanReport> {
        Ok(ScanReport::empty())
    }

    async fn run_continuous(&mut self, _state: &mut Self::State, _from: Checkpoint,
        _shutdown_rx: watch::Receiver<bool>) -> crate::runtime::RuntimeResult<ScanReport> {
        Ok(ScanReport::empty())
    }
}

pub type MyIngestorNode = SourceDriverRuntime<MyIngestor>;
```

### Derived RuntimeModule Pattern (Transducer — Stateless)

```rust
use crate::runtime::{Transducer, AutomatonRuntime, AutomatonLogicError};
use crate::runtime::automaton::AutomatonContext;

struct MyTransducer;

impl Transducer for MyTransducer {
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "my-transducer" }
    fn input_event_type(&self) -> &'static str { "command.executed" }
    fn output_event_type(&self) -> &'static str { "command.canonical" }

    async fn process(&mut self, input: Self::Input, _ctx: &NodeEventContext)
        -> Result<Option<Self::Output>, AutomatonLogicError>
    {
        // Return Some(output) to emit, None to filter
        Ok(Some(transform(input)))
    }
}
```

### Derived RuntimeModule Pattern (Windowed — Accumulate Then Emit)

```rust
use crate::runtime::{Windowed, AutomatonRuntime, DerivedOutput, AutomatonLogicError};
use crate::runtime::automaton::AutomatonContext;

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
        ctx: &AutomatonContext) -> Result<(), AutomatonLogicError>
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
        -> Result<Option<DerivedOutput>, AutomatonLogicError>
    {
        Ok(Some(DerivedOutput::windowed(json!({
            "start_time": state.start_ts,
            "end_time": state.last_ts,
            "event_count": state.events.len(),
        }))))
    }
}
```

### Runtime Support Components Reference

```rust
use crate::runtime::{
    // Core node types
    SourceDriver, SourceDriverRuntime,
    Transducer, Windowed, ScopeReconciler,
    AutomatonRuntime,
    // Config + CLI
    RuntimeConfig, RuntimeCli, RuntimeCliRunner,
    // Runtime
    CheckpointManager,
    NatsPublisher, HeartbeatEmitter, DlqRetryHandler,
    RuntimeCoordination, InstanceMode,
    SelfObserver, SelfObserverConfig,
    // Health
    HealthReporter, HealthMetrics,
};
```

Reference: `crate/sinexd/src/runtime/` (historical module name; runtime support lives inline in sinexd)
