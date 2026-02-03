## Ingestor Pattern (SimpleNode trait)

```rust
use sinex_node_sdk::simple_node::{SimpleNode, NodeContext};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
struct MyState { /* checkpoint state */ }

struct MyIngestor;

impl SimpleNode for MyIngestor {
    type State = MyState;
    type Input = serde_json::Value;  // or typed event payload
    type Output = serde_json::Value;

    async fn process(&self, ctx: &NodeContext<Self::State>, input: Self::Input)
        -> Result<Vec<Self::Output>, SinexError>
    {
        // Transform/enrich/filter events
        Ok(vec![input])
    }
}
```

---

## Automaton Pattern (events → derived events/state)

```rust
use sinex_node_sdk::{AutomatonFields, AutomatonEventHandler};

struct MyAutomaton {
    fields: AutomatonFields,
}

impl AutomatonEventHandler for MyAutomaton {
    async fn handle_event(&mut self, event: Event<JsonValue>) -> NodeResult<()> {
        // Process event, update state, optionally emit derived events
        self.fields.emit_event("derived.type", json!({...})).await?;
        Ok(())
    }
}
```

Reference: `crate/lib/sinex-node-sdk/docs/overview.md`
