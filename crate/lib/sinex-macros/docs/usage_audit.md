# Macro Usage Audit

**Status**: 2026-01-24

## Usage Summary

| Macro | Usage Count | Status | Notes |
|-------|-------------|--------|-------|
| `#[derive(EventPayload)]` | 115 | ✅ Active | Production-critical. Used in `sinex-core`. |
| `#[with_context]` | 0 | ❌ Broken | BUG-020: Generates non-functional code. |
| `#[derive(ValidateRecord)]` | 0 | ❌ Broken | BUG-019: No-op implementation. |
| `db_query!` | 0 | ⚠️ Unused | Has tests, but no production usage. |
| `db_transaction!` | 0 | ⚠️ Unused | Has tests, but no production usage. |
| `event_registry!` | 0 | ⚠️ Unused | Legacy pattern. |
| `typed_event_envelope` | 0 | ⚠️ Unused | Legacy pattern. |
| `define_id_type!` | 0 | ⚠️ Unused | Superseded by `Id<T>`. |

## Recommendations

-   **EventPayload**: Keep and maintain.
-   **with_context / ValidateRecord**: Deprecate and remove.
-   **Others**: Evaluate for removal if no use case arises.
