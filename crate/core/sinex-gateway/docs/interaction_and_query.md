# User Interaction & Query Architecture

* **Purpose:** Describe how users and tools interact with Sinex today: gateway service, CLI, and
  supporting service layer.
* **Scope:** Current behaviour.

## 1. Components Overview

| Component | Location | Role | Status |
|-----------|----------|------|--------|
| `sinex-gateway` | `crate/core/sinex-gateway` | Hosts a JSON-RPC server (TLS-only TCP) and an optional native-messaging bridge | ✅ operational |
| `sinexctl` CLI | `crate/cli` | Primary operator tooling for gateway RPC; also exposes direct DB commands under `db` | ✅ operational |
| PKM module | `crate/lib/sinex-db/src/pkm.rs` | DB-owned PKM orchestration invoked by gateway handlers | ✅ operational |
## 2. Gateway Architecture

### 2.1 Execution Modes

- **RPC server (`sinex-gateway rpc-server`)**  
  * Binds to TLS TCP by default on `127.0.0.1:9999` (override with `--tcp-listen <host:port>` or `SINEX_GATEWAY_TCP_LISTEN`).
  * Accepts JSON-RPC 2.0 POST requests at `/rpc`.
* **Native messaging (`sinex-gateway native-messaging`)**  
  * Runs a stdin/stdout loop for a browser extension; reuses the same RPC dispatch table.

### 2.2 Request Handling

1. Client submits JSON-RPC payload (method + params).
2. `rpc_server::handle_rpc` deserialises the message and forwards it to `dispatch_rpc_method`.
3. Dispatch routes into gateway-local handlers plus their owned services; PKM currently flows
   through `sinex-db::pkm`, while blob/content workflows stay inside `sinex-gateway`.
4. Responses are sent synchronously; errors become JSON-RPC failures (`-32601` unknown method, `-32603` internal error).

**Key point:** the gateway does **not** publish or consume `api.command.*` / `api.response.*` events on `JetStream` today. All work is handled within the process using synchronous database calls.

### 2.3 Authentication & Transport Limits

- RPC traffic is guarded by a shared secret exported via `SINEX_RPC_TOKEN` (or `SINEX_GATEWAY_ADMIN_TOKEN_FILE` / `SINEX_RPC_TOKEN_FILE`). Gateway startup fails if no token is present.
* Tokens must include a role suffix (`<token>:readonly|write|admin`), and clients present them via `Authorization: Bearer <token-with-role>`. `sinexctl` injects the header when `--token`, `--token-file`, or `SINEX_RPC_TOKEN` are configured.
* TLS is mandatory; set `SINEX_GATEWAY_TLS_CERT` + `SINEX_GATEWAY_TLS_KEY` (optional `SINEX_GATEWAY_TLS_CLIENT_CA` for mTLS).
* Non-loopback binds require mTLS; configure `SINEX_GATEWAY_TLS_CLIENT_CA` and pass `SINEX_RPC_CLIENT_CERT` + `SINEX_RPC_CLIENT_KEY` to clients.
* Set `SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1` to enforce mTLS even on loopback/test hosts.
* Resource guards are configurable via:
  * `SINEX_GATEWAY_MAX_CONCURRENCY` (default 100).
  * `SINEX_GATEWAY_REQUEST_TIMEOUT_SECS` (default 30 seconds).
  * `SINEX_GATEWAY_MAX_BODY_BYTES` (default 2 MiB).
  * `SINEX_GATEWAY_MAX_BLOB_BYTES` (default 5 MiB) limits decoded blob payloads before writing to the content store.
* NixOS deployments should set these via `services.sinex.core.gateway.limits` rather than ad-hoc env vars.
* Requests that exceed these guards receive JSON-RPC errors (`401` for missing token, `429/504/413` for the respective limits).

### 2.3 Method Surface (current)

- Read/query: `system.health`, `search.search_events`, `analytics.*`, `audit.get`, `ops.list/get`, `coordination.*`, `nodes.list`, `dlq.list/peek`, replay status/list.
* Write/mutate: `pkm.*`, `content.store_blob`, `nodes.{drain,resume,set_horizon}`, `ops.start`, replay create/preview.
* Admin-only: replay approve/execute/cancel, `dlq.requeue/purge`, lifecycle archive/restore/tombstone, `ops.cancel`, gitops source management, shadow create/delete.

Adding a method requires registering it in `rpc_registry.rs`, wiring a handler in the gateway/service layer, and optionally exposing it in `sinexctl`.

### 2.4 Deployment Considerations

- Keep RPC on loopback unless you explicitly need remote access; enable mTLS + firewalling for non-local binds.
* Gateway shares a database pool with the service layer; long-running queries block the handler thread. Move heavy work to background tasks before revisiting asynchronous fan-out.
* Authentication is enforced by bearer token + role checks; transport and request guards (timeouts/concurrency/body size/rate limiting) are enforced in the gateway middleware stack.

## 3. CLI Integration (`sinexctl`)

### 3.1 Modes of Operation

- **Gateway-backed commands (default):**
  * `sinexctl` creates a `GatewayClient` targeting `--rpc-url` / `SINEX_RPC_URL` (default `https://127.0.0.1:9999`).
  * Auth is configured via `--token`, `--token-file`, or `SINEX_RPC_TOKEN`.
  * TLS trust is configured via `--ca-cert`; mTLS client auth uses `--client-cert` + `--client-key` (or env equivalents).
* **Direct database commands (`sinexctl db ...`):**
  * The `db` command family bypasses the gateway and connects directly via `DATABASE_URL`.
  * Use for diagnostics/testing when RPC is unavailable or when you explicitly need SQL-level visibility.

### 3.2 Error Handling & UX

- Gateway failures surface JSON-RPC errors and transport errors with command-level context.
* `db` commands propagate SQLx/database connectivity errors directly with additional hints.
* Completion and help output derive from live metadata where possible (see `crate/cli/DESIGN.md`).

## 4. Service Layer Responsibilities

Gateway handlers split across two ownership shapes today:
* **PKM (`sinex-db::pkm`)** – entity/relation/source-material orchestration owned by the database layer.
* **Content (`sinex-gateway::content_service`)** – blob storage/retrieval via the content store.

These modules run synchronously and use shared database pools. Keep transactions small to avoid
blocking other RPCs.

## 5. Reference Material

- Gateway source: `crate/core/sinex-gateway/src/main.rs`, `rpc_server.rs`, `handlers.rs`, `service_container.rs`.
* CLI docs: `crate/cli/README.md`, `crate/cli/DESIGN.md`.
* PKM module documentation: `crate/lib/sinex-db/docs/pkm.md`.
