## Ingestor Pattern (`IngestorNode`)

```rust
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::{IngestorNode, IngestorNodeAdapter, IngestorState};
use sinex_node_sdk::runtime::stream::{Checkpoint, NodeRuntimeState, ScanArgs, ScanReport, TimeHorizon};
use tokio::sync::watch;

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state */ }

#[derive(Default)]
struct MyIngestor;

#[async_trait]
impl IngestorNode for MyIngestor {
    type Config = serde_json::Value;
    type State = MyState;

    fn name(&self) -> &str { "my-ingestor" }

    async fn initialize(
        &mut self,
        state: &mut Self::State,
        _config: Self::Config,
        _runtime: &NodeRuntimeState,
    ) -> sinex_node_sdk::NodeResult<()> {
        let _ = state;
        Ok(())
    }

    async fn scan_snapshot(
        &mut self,
        _state: &mut Self::State,
        _args: ScanArgs,
    ) -> sinex_node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }

    async fn scan_historical(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _until: TimeHorizon,
        _args: ScanArgs,
    ) -> sinex_node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }

    async fn run_continuous(
        &mut self,
        _state: &mut Self::State,
        _from: Checkpoint,
        _shutdown_rx: watch::Receiver<bool>,
    ) -> sinex_node_sdk::NodeResult<ScanReport> {
        Ok(ScanReport::empty())
    }
}

pub type MyIngestorNode = IngestorNodeAdapter<MyIngestor>;
```

---

## Automaton Pattern (`AutomatonNode` + `AutomatonNodeAdapter`)

```rust
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_node_sdk::{AutomatonNode, AutomatonNodeAdapter, NodeEventContext, NodeLogicError};
use sinex_primitives::JsonValue;

#[derive(Default, Serialize, Deserialize)]
struct MyState {
    events_seen: u64,
    // Checkpoint state — persisted automatically
}

#[derive(Default)]
struct MyAutomaton;

#[async_trait]
impl AutomatonNode for MyAutomaton {
    type State = MyState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str { "my-automaton" }
    fn input_event_type(&self) -> &'static str { "*" }        // Subscribe pattern
    fn output_event_type(&self) -> &'static str { "derived.insight" }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &NodeEventContext,
    ) -> Result<Option<Self::Output>, NodeLogicError> {
        state.events_seen += 1;
        // Return Some(output) to emit, None to filter
        Ok(Some(input))
    }
}

pub type MyAutomatonNode = AutomatonNodeAdapter<MyAutomaton>;
```

**Note:** `AutomatonFields<C>` remains shared infrastructure for lower-level automata,
while most new nodes should use `AutomatonNodeAdapter`.

Reference: `crate/lib/sinex-node-sdk/docs/overview.md`
