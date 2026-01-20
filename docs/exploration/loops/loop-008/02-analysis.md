# Loop 008 - RPC Input Validation Boundaries

Scope
- Gateway RPC handlers under `crate/core/sinex-gateway/src/handlers`.

Handler Map and Validation

1) Ops handlers (`handlers/ops.rs`)
- `handle_ops_start`: accepts `operation_type`, `operator`, `scope` with no validation beyond JSON parsing.
- `handle_ops_list`: optional filters; defaults if params fail to parse (tolerant).
- `handle_ops_get` / `handle_ops_cancel`: takes `operation_id` as string; no ULID validation before DB lookup.
- Uses parameterized SQL, so injection risk is low, but semantic validation (format, length) is absent.

2) DLQ handlers (`handlers/dlq.rs`)
- `handle_dlq_list`: no parameters.
- `handle_dlq_peek`: `limit` has a default; no max enforced.
- `handle_dlq_requeue`: accepts `event_id` or `all` boolean; requires one of them, but does not validate event_id format.
- `handle_dlq_purge`: requires `confirm: true`.

3) Nodes handlers (`handlers/nodes.rs`)
- `handle_nodes_drain/resume/set_horizon`: validates JSON; set_horizon validates RFC3339 timestamp.
- Uses `env.nats_subject(...)` for subject construction.

4) Legacy handlers (`handlers/legacy.rs`)
- Central `RpcParams` helper enforces string/ULID parsing and provides a few validators.
- Analytics:
  - `handle_event_count_by_source`: `days_back` optional, no range clamp.
  - `handle_activity_heatmap`: validates bucket size (`validate_bucket_size_minutes`), limit defaults.
- PKM:
  - `handle_create_note`: base64 decode + UTF-8 validation; tags are strings.
  - `handle_create_entities`: validates entity name; entity type not validated.
  - `handle_link_entities`: validates entity IDs not equal; relationship_type unchecked.
- Search: `handle_search_events` uses typed `SearchQuery` from JSON.
- Content:
  - `handle_store_blob`: base64 decode with size limit; filename/content_type/source optional but not validated.
  - `handle_retrieve_blob`: validates annex_key presence only.
- Replay and coordination: use `require_ulid` and typed parsing for replay state.

5) Shadow handlers (`handlers/shadow.rs`)
- `handle_shadow_create`: enforces `dev-` prefix; subject_filter is accepted as-is if provided.
- `handle_shadow_list`: optional prefix filter, no other validation.
- `handle_shadow_delete`: enforces `dev-` prefix.

6) Audit handlers (`handlers/audit.rs`)
- `handle_audit_get`: takes `operation_id` as string; no ULID validation before DB lookup.

Findings
- Validation is inconsistent across handler modules: `legacy.rs` uses a helper with ULID parsing, others accept raw strings.
- Ops/audit handlers allow any `operation_id` string; malformed IDs only fail at DB lookup.
- Some domain fields (entity type, relationship_type, content_type, subject_filter) are not validated.

Risks
- Unbounded `limit` (DLQ peek) can create large responses or long pulls.
- Raw strings without format validation can generate confusing errors and log noise.
- Subject filters in shadow consumers are unvalidated; malformed subjects can cause NATS errors.

Opportunities
- Introduce a shared `RpcParams` helper or validator module for all handlers.
- Add format validation for `operation_id` in ops/audit handlers (ULID check).
- Add reasonable bounds for DLQ `limit` and search/result parameters.
