Status: canonical
# User Interaction & Query Architecture

*   **Version:** 2.1
*   **Date:** 2025-07-24
*   **Implementation Status:** ✅ **OPERATIONAL** – Gateway + CLI in production; JetStream command bus remains planned
*   **Purpose:** Describe how users and tools interact with Sinex today: gateway service, CLI, and supporting service layer.
*   **Scope:** Current behaviour. Future enhancements are called out explicitly.

> **Historical context**  
> Earlier iterations of this document described a JetStream-backed command/response loop. That work has not shipped; the implementation below reflects the code in this repository.

## 1. Components Overview

| Component | Location | Role | Status |
|-----------|----------|------|--------|
| `sinex-gateway` | `crate/core/sinex-gateway` | Hosts a JSON-RPC server (Unix socket or TCP) and an optional native-messaging bridge | ✅ operational |
| `exo` CLI | `cli/exo.py` | Primary user tooling; prefers RPC, can fall back to direct Postgres access | ✅ operational |
| Service layer | `crate/lib/sinex-services` | Analytics, search, PKM, and content APIs invoked by gateway handlers | ✅ operational |
| JetStream command bus | — | Planned async command/response fabric | 🚧 planned |

## 2. Gateway Architecture

### 2.1 Execution Modes
- **RPC server (`sinex-gateway rpc-server`)**  
  - Binds to a Unix socket by default (non-dev) or `127.0.0.1:9999` in development.  
  - Accepts JSON-RPC 2.0 POST requests at `/rpc` and `/`.
  - Binding is controlled by `SINEX_GATEWAY_HOST`, `SINEX_GATEWAY_PORT`, and the current `SinexEnvironment`.
- **Native messaging (`sinex-gateway native-messaging`)**  
  - Runs a stdin/stdout loop for a browser extension; reuses the same RPC dispatch table.

### 2.2 Request Handling
1. Client submits JSON-RPC payload (method + params).
2. `rpc_server::handle_rpc` deserialises the message and forwards it to `dispatch_rpc_method`.
3. Dispatch routes into the appropriate module in `sinex-services`, which talks to PostgreSQL via `sinex-core`.
4. Responses are sent synchronously; errors become JSON-RPC failures (`-32601` unknown method, `-32603` internal error).

**Key point:** the gateway does **not** publish or consume `api.command.*` / `api.response.*` events on JetStream today. All work is handled within the process using synchronous database calls.

### 2.3 Method Surface (current)
- `analytics.event_count_by_source`
- `analytics.activity_heatmap`
- `search.search_events`
- `pkm.create_note`, `pkm.create_entities_from_list`, `pkm.link_entities`
- `content.store_blob`, `content.retrieve_blob`

Adding a method requires extending `dispatch_rpc_method`, exposing functionality in `sinex-services`, and (optionally) wiring a CLI command.

### 2.4 Deployment Considerations
- Guard Unix socket permissions (default) or secure the loopback HTTP port behind SSH tunnelling when accessed remotely.
- Gateway shares a database pool with the service layer; long-running queries block the handler thread. Move heavy work to background tasks before revisiting asynchronous fan-out.
- Authentication and rate limiting are TODOs; current deployments rely on OS-level controls.

## 3. CLI Integration (`exo`)

### 3.1 Modes of Operation
- **RPC mode (default):**  
  - `exo` instantiates `SinexRPCClient`, targeting the gateway URL from `--rpc-url` or `SINEX_RPC_URL` (default `http://127.0.0.1:9999`).  
  - Commands such as `query`, `sources`, and `stats` map directly to the gateway methods above.
- **Database mode (`--use-db`):**  
  - Connects to PostgreSQL using `DATABASE_URL`.  
  - Unlocks low-level operations not yet exposed via RPC (schema introspection, DLQ management, blob utilities).

### 3.2 Error Handling & UX
- RPC failures prompt the user to retry with `--use-db` and surface the JSON-RPC error code.
- Database mode propagates SQLx errors directly; most commands wrap them with context.
- Completion and help output derive from live metadata where possible (see `cli/DESIGN.md`).

## 4. Service Layer Responsibilities

Gateway handlers delegate to `sinex-services`, which provides cohesive APIs over `sinex-core`:
- **Analytics (`analytics.rs`)** – timed aggregates over `core.events`.
- **Search (`search.rs`)** – filtered event queries with pagination.
- **PKM (`pkm.rs`)** – CRUD operations for knowledge-management entities.
- **Content (`content.rs`)** – blob storage/retrieval via annex.

These modules run synchronously and use shared database pools. Keep transactions small to avoid blocking other RPCs.

## 5. Roadmap

- **JetStream command/response:** Revisit once ingestion and automata have stabilised on JetStream (`docs/way.md`). Expected benefits include async processing and richer auditing.
- **Streaming / WebSocket APIs:** Layer on top of the gateway after command bus work lands.
- **Authentication & authorisation:** Add token or mTLS enforcement plus per-method access control.
- **Observability:** Instrument RPC handlers with tracing and metrics once performance hotspots are identified.

## 6. Reference Material
- Gateway source: `crate/core/sinex-gateway/src/main.rs`, `rpc_server.rs`, `handlers.rs`, `service_container.rs`.
- CLI docs: `cli/README.md`, `cli/DESIGN.md`.
- Service documentation: `crate/lib/sinex-services/doc/*.md`.
- Future architecture: `docs/way.md`, `docs/vision/streaming-architecture.md`.
