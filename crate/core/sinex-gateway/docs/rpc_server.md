# JSON-RPC Server

Implements the JSON-RPC 2.0 compliant server that fronts Sinex gateway services for CLI tools.

## Supported RPC Methods

- `system.health` – basic service health probe.
- `analytics.event_count_by_source` – counts per source across a time window.
- `analytics.activity_heatmap` – time-bucketed activity totals.
- `analytics.sources_statistics` – per-source totals, ranges, and ingest delay.
- `search.search_events` – query events with filters and pagination.
- `pkm.create_note` – create a note annotation.
- `pkm.create_entities_from_list` – create multiple entities.
- `pkm.link_entities` – link entities together.
- `content.store_blob` – store base64 payloads in git-annex.
- `content.retrieve_blob` – fetch stored blobs.
- `replay.create_operation` – create a new replay operation.
- `replay.preview_operation` – preview replay cascades for a scope.
- `replay.approve_operation` – mark a replay operation approved.
- `replay.execute_operation` – start executing a replay operation.
- `replay.cancel_operation` – cancel a replay operation.
- `replay.operation_status` – fetch replay status.
- `replay.list_operations` – list replay operations by state.

## Protocol Specification

- Requests: `{"jsonrpc": "2.0", "method": "...", "params": {...}, "id": 1}`.
- Success: `{"jsonrpc": "2.0", "result": {...}, "id": 1}`.
- Error: `{"jsonrpc": "2.0", "error": {"code": -1, "message": "..."},"id": 1}`.

## Security & Resource Guards

- CORS headers configured for local development.
- Request/response logging for audit trails.
- Error sanitisation to avoid leaking sensitive details.
- TLS is mandatory for the RPC server; configure `SINEX_GATEWAY_TLS_CERT` + `SINEX_GATEWAY_TLS_KEY` (optional `SINEX_GATEWAY_TLS_CLIENT_CA` for mTLS).
- Non-loopback binds require mTLS; set `SINEX_GATEWAY_TLS_CLIENT_CA` to a trusted client CA bundle.
- Mandatory RPC auth token (`SINEX_RPC_TOKEN` or `SINEX_GATEWAY_ADMIN_TOKEN_FILE` / `SINEX_RPC_TOKEN_FILE`); requests must send `Authorization: Bearer <token>`.
- Concurrency limit (`SINEX_GATEWAY_MAX_CONCURRENCY`, default 32) enforced via tower middleware.
- Request timeout (`SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`, default 30s) returns JSON-RPC gateway timeout errors.
- Payload size cap (`SINEX_GATEWAY_MAX_BODY_BYTES`, default 2MB) returns 413 errors when exceeded.
- Blob uploads have an explicit content quota (`SINEX_GATEWAY_MAX_BLOB_BYTES`, default 5MB) that is enforced after base64 decoding to keep git-annex writes bounded.
