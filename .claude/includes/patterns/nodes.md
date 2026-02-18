## Ingestor Pattern (SimpleNode trait)

```rust
use sinex_node_sdk::simple_node::{SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state */ }

#[derive(Default)]
struct MyIngestor;

impl SimpleNode for MyIngestor {
    type State = MyState;
    type Input = serde_json::Value;  // or typed event payload
    type Output = serde_json::Value;

    fn name(&self) -> &'static str { "my-ingestor" }
    fn input_event_type(&self) -> &'static str { "raw.input" }
    fn output_event_type(&self) -> &'static str { "processed.output" }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        _context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        // Transform/enrich/filter events
        // Return Some(output) to emit, None to filter
        Ok(Some(input))
    }
}

pub type MyIngestorNode = SimpleNodeWrapper<MyIngestor>;
```

---

## Automaton Pattern (SimpleNode + SimpleNodeWrapper)

Automatons use the same `SimpleNode` trait as ingestors. The difference is semantic
(derived events vs raw capture), not structural.

```rust
use sinex_node_sdk::simple_node::{SimpleNode, SimpleNodeContext, SimpleNodeError, SimpleNodeWrapper};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
struct MyState {
    events_seen: u64,
    // Checkpoint state — persisted automatically
}

#[derive(Default)]
struct MyAutomaton;

impl SimpleNode for MyAutomaton {
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
        _context: &SimpleNodeContext,
    ) -> Result<Option<Self::Output>, SimpleNodeError> {
        state.events_seen += 1;
        // Return Some(output) to emit, None to filter
        Ok(None)
    }
}

// Wrap for production use (adds checkpoint, persistence, health, provenance)
pub type MyAutomatonNode = SimpleNodeWrapper<MyAutomaton>;
```

**Note:** `AutomatonFields<C>` exists as shared infrastructure (runtime state, stats,
consumer management) but automatons compose via `SimpleNodeWrapper`, not by embedding
`AutomatonFields` directly. `AutomatonEventHandler` is a concrete adapter struct for
confirmed event tracking, not a trait to implement.

Reference: `crate/lib/sinex-node-sdk/docs/overview.md`
