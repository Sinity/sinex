## Services Layer

High-level business logic (PKM, content) consumed by gateway. Analytics and search
functionality has been subsumed by `EventQuery` in the gateway's RPC handlers.

### Service Instantiation

```rust
use sinex_services::{ContentService, PkmService};

// Services take a pool reference
let pkm = PkmService::new(pool.clone());
let content = ContentService::new(pool.clone(), Arc::new(blob_manager));
```

### Service Capabilities

| Service | Purpose | Key methods |
|---------|---------|-------------|
| `PkmService` | Entity/relationship tracking | `register_source_material()`, `create_entities_from_source_material()`, `link_entities()` |
| `ContentService` | Binary blob storage | `store_content()`, `retrieve_content()`, `verify_content()` |

### Gateway Query Layer

Event analytics and search are handled directly by the gateway's RPC handlers
using repository methods (`pool.events()`) rather than separate service structs.

Reference: `crate/lib/sinex-services/src/`
