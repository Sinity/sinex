# sinex-services Overview

The services crate packages higher-level workflows on top of the repositories
exposed by `sinex-core`. Gateway handlers, satellites, and automation jobs rely
on these modules to coordinate multi-step operations without duplicating SQL or
business rules.

## Modules

- [`analytics`](./analytics.md) – rollups for dashboards and telemetry (`event_count_by_source`,
  `event_count_by_type`, `events_over_time`, `top_commands`, `activity_heatmap`).
- [`content`](./content.md) – blob storage helpers backed by the annex manager
  (`store_content`, `retrieve_content`, `get_content_metadata`, `verify_content`).
- [`pkm`](./pkm.md) – personal knowledge management utilities (`create_note`,
  `create_entities_from_source_material`, `link_entities`).
- [`search`](./search.md) – parameterised event search over Postgres (`search_events`).

Each module owns its own documentation describing contracts, error reporting,
and invariants. When adding a new service:

1. Create `doc/<module>.md` detailing the API surface and dependencies.
2. Keep `include_str!` pointers up to date so `cargo doc` surfaces the narrative.
3. Link to cross-cutting architecture references under `docs/` when behaviour
   spans multiple components.
