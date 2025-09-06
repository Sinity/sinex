Status: reference
# Glossary

Key terms used consistently across Sinex documentation and code.

- Ingestd: The central ingestion daemon (`sinex-ingestd`) that validates and persists events; the only writer to `core.events`.
- Sensd: The sensor daemon (`sinex-sensd`) responsible for capturing source material; satellites do not capture material directly.
- Satellite: A service that produces or processes events (sensors/scanners) using the `sinex-satellite-sdk`.
- Automaton: A stream processor that derives new events from existing ones (e.g., analytics, content, PKM automatons).
- Gateway: The Axum‑based API server (`sinex-gateway`) exposing JSON‑RPC for queries and control.
- Event: An immutable record stored in Postgres (`core.events`) with ULID id, strict provenance, and JSON payload.
- Material: Source blobs and large artifacts managed via annex/registries, referenced from events via IDs.
- Schema ID: A stable, versioned payload identifier in the form `<domain>/<entity>@v<semver>`.
- ULID: Universally Unique Lexicographically Sortable Identifier used for ordered IDs and sharding friendliness.
- Operations Log: Table used to track long‑running/user‑triggered operations and replay/cascade activities.
