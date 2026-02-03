## Services Layer

High-level business logic (analytics, search, PKM, content) consumed by gateway.

### Service Instantiation

```rust
use sinex_services::{AnalyticsService, SearchService, ContentService, PkmService};

// Services take a pool reference
let analytics = AnalyticsService::new(pool.clone());
let search = SearchService::new(pool.clone());
let pkm = PkmService::new(pool.clone());
let content = ContentService::new(pool.clone(), Arc::new(blob_manager));
```

### Service Capabilities

| Service | Purpose | Key methods |
|---------|---------|-------------|
| `AnalyticsService` | Event aggregation, time-series | `get_event_count_by_source()`, `activity_heatmap()`, `get_top_commands()` |
| `SearchService` | Full-text search with filters | `search_events(SearchQuery { text, sources, time_range, ... })` |
| `PkmService` | Entity/relationship tracking | `register_source_material()`, `create_entities_from_source_material()`, `link_entities()` |
| `ContentService` | Binary blob storage | `store_content()`, `retrieve_content()`, `verify_content()` |

Reference: `crate/lib/sinex-services/src/`
