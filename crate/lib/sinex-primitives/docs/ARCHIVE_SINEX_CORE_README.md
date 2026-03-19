# sinex-core Documentation

## Core Documentation

- `overview.md` – Crate architecture, core tables, and principles
- `types_overview.md` – Catalog of major type families (events, errors, IDs)
- `domain_types.md` – Strongly-typed domain primitives and validation strategy (NEW)
- `newtypes.md` – Configuration newtypes (Seconds, Bytes, Milliseconds)

## Implementation Guides

- `event_persistence.md` – Event repository, batch strategies, and provenance (NEW)
- `distributed_coordination.md` – NATS KV primitives for leadership and discovery (NEW)
- `db_repositories.md` – Repository pattern and usage examples
- `pool.md` – Database pooling performance notes
- `error.md` – Unified error type features and usage
- `query_helpers.md` – Query builder patterns
- `events_blanket_impls.md` – Blanket event impl caveats

## See Also

- Global architecture: `README.md#architecture`
- Type system patterns: `crate/lib/sinex-primitives/docs/type_system_patterns.md`
