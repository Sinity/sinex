# JSON-RPC Server

Implements the JSON-RPC 2.0 compliant server that fronts Sinex gateway services for CLI tools.

## Security Architecture

The RPC server implements a **defense-in-depth** strategy with 7 layers of protection:
1. **Network**: TLS is mandatory. Non-loopback connections require mTLS with client certificate validation.
2. **Middleware**: Tower layers enforce concurrency limits, timeouts (30s default), and request body limits (2MB default).
3. **Auth**: Bearer token authentication uses **constant-time comparison** to prevent timing attacks.
4. **Rate Limit**: Per-token leaky bucket (default 100 req/s) prevents `DoS` from compromised or buggy clients.
5. **Protocol**: Strict JSON-RPC 2.0 validation rejects malformed requests early.
6. **Authorization**: Dangerous operations (e.g., `ops.cancel`) require explicit auth context.
7. **Fail-Closed**: System refuses to start without a configured token; if a watched token file is deleted, the gateway keeps the last valid token loaded.

## Performance Characteristics

- **Request Pipeline**: ~2-5ms overhead (TLS handshake + auth + dispatch).
- **Concurrency**: Default limit of 100 concurrent requests matches the typical `PostgreSQL` connection pool size to prevent resource exhaustion.
- **Connection Handling**: Uses a **spawn-per-connection** pattern for TLS handshakes, isolating the accept loop from slowloris-style attacks.

## Authentication & Rate Limiting

- **Token File**: Supports live reloading for zero-downtime rotation. If the file is deleted, the gateway keeps using the last valid token until a new token file value is loaded.
- **Token Format**: Tokens must include a role suffix: `<token>:readonly`, `<token>:write`, or `<token>:admin`.
- **Rate Limiting**: Rate limits are isolated per-token. A single compromised token cannot exhaust the global quota.

## Supported RPC Methods

### System
- `system.health` – Detailed health probe. Returns `healthy`, `degraded`, or `unhealthy` plus `serving` and `degradation_reasons`. `serving` now requires DB, NATS, and replay control to be live, so a degraded gateway is not go-live ready.

### Analytics
- `analytics.event_count_by_source` – counts per source across a time window.
- `analytics.activity_heatmap` – time-bucketed activity totals.
- `analytics.sources_statistics` – per-source totals, ranges, and ingest delay.

### Knowledge Management (PKM)
- `search.search_events` – query events with filters and pagination.
- `pkm.create_note` – create a note annotation.
- `pkm.create_entities_from_list` – create multiple entities.
- `pkm.link_entities` – link entities together.

### Content & Blobs
- `content.store_blob` – store base64 payloads in the content store.
- `content.retrieve_blob` – fetch stored blobs.

### Replay Control
- `replay.create_operation` – create a new replay operation.
- `replay.preview_operation` – preview replay cascades for a scope.
- `replay.approve_operation` – mark a replay operation approved.
- `replay.submit_operation` – atomically approve and execute a previewed replay.
- `replay.execute_operation` – start executing a replay operation.
- `replay.cancel_operation` – cancel a replay operation.
- `replay.operation_status` – fetch replay status.
- `replay.list_operations` – list replay operations by state.

## Configuration

- `SINEX_GATEWAY_TLS_CERT` / `SINEX_GATEWAY_TLS_KEY`: Mandatory TLS certificate paths.
- `SINEX_GATEWAY_TLS_CLIENT_CA`: Trusted client CA bundle (required for mTLS).
- `SINEX_RPC_TOKEN`: Bearer token for authentication (`<token>:<role>` format).
- `SINEX_GATEWAY_MAX_CONCURRENCY`: Max concurrent requests (default 100).
- `SINEX_GATEWAY_REQUEST_TIMEOUT_SECS`: Request timeout (default 30s).
- `SINEX_GATEWAY_MAX_BODY_BYTES`: Request body size limit (default 2MB).
- `SINEX_GATEWAY_MAX_BLOB_BYTES`: Blob content size limit (default 5MB).

## Protocol Specification

- Requests: `{"jsonrpc": "2.0", "method": "...", "params": {...}, "id": 1}`.
- Success: `{"jsonrpc": "2.0", "result": {...}, "id": 1}`.
- Error: `{"jsonrpc": "2.0", "error": {"code": -1, "message": "..."},"id": 1}`.
