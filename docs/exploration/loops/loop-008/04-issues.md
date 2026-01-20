# Loop 008 - Concrete Issues

1) Ops and audit handlers accept raw operation_id strings without ULID validation
- Evidence: `handle_ops_get`, `handle_ops_cancel` in `crate/core/sinex-gateway/src/handlers/ops.rs` and `handle_audit_get` in `crate/core/sinex-gateway/src/handlers/audit.rs` parse `operation_id` as plain string only.
- Impact: malformed IDs cause confusing DB errors; handler behavior is inconsistent with replay handlers that require ULIDs.

2) DLQ peek limit is unbounded
- Evidence: `handle_dlq_peek` in `crate/core/sinex-gateway/src/handlers/dlq.rs` defaults to 10 but does not enforce a max.
- Impact: callers can request very large peeks, creating large responses and long-running pulls.

3) Shadow subject filters are not validated
- Evidence: `handle_shadow_create` uses `subject_filter` as provided when present.
- Impact: malformed subjects can cause runtime errors or unexpected subscription behavior.
