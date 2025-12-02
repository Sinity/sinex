# JSON-RPC Server

Implements the JSON-RPC 2.0 compliant server that fronts Sinex gateway services for CLI tools.

## Supported RPC Methods

- `query_events` – query events with filtering and pagination.
- `replay_analyze` – analyse replay cascades for a set of events.
- `replay_create` – create a new replay operation.
- `replay_approve` – mark a replay operation approved for execution.
- `replay_status` – fetch replay operation status.
- `health_check` – basic service health probe.

## Protocol Specification

- Requests: `{"jsonrpc": "2.0", "method": "...", "params": {...}, "id": 1}`.
- Success: `{"jsonrpc": "2.0", "result": {...}, "id": 1}`.
- Error: `{"jsonrpc": "2.0", "error": {"code": -1, "message": "..."},"id": 1}`.

## Security & Resource Guards

- CORS headers configured for local development.
- Request/response logging for audit trails.
- Error sanitisation to avoid leaking sensitive details.
- Mandatory RPC auth token (`SINEX_RPC_TOKEN` or `SINEX_RPC_TOKEN_FILE`); requests must send `Authorization: Bearer <token>` (or `X-Sinex-Rpc-Token`). Set `SINEX_GATEWAY_ALLOW_INSECURE=1` only for tests.
- Concurrency limit (`SINEX_GATEWAY_MAX_CONCURRENCY`, default 32) enforced via tower middleware.
- Request timeout (`SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`, default 30s) returns JSON-RPC gateway timeout errors.
- Payload size cap (`SINEX_GATEWAY_MAX_BODY_BYTES`, default 2MB) returns 413 errors when exceeded.
- Blob uploads have an explicit content quota (`SINEX_GATEWAY_MAX_BLOB_BYTES`, default 5MB) that is enforced after base64 decoding to keep git-annex writes bounded.
