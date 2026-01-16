# Testing Priorities and Roadmap

This roadmap tracks the testing-focused work needed to keep gateway, ingestd,
and nodes aligned with production behavior. It is intended to be updated
as reliability gaps are closed.

## Immediate Priorities

- **Gateway RPC health + replay coordination**: add explicit health reporting,
  replay control lifecycle coverage, and ensure bypass modes are observable.
- **System watcher resilience**: add restart coverage to the system-node
  watchers and document failure behavior in the node docs.
- **Gateway pool isolation**: add tests that prove long analytics/search queries
  do not starve other RPC handlers.

## JetStream Harness and TLS Coverage

- Pin `nats-server` binaries for e2e and reliable profiles.
- Add secure test profiles (TLS + creds) across ingestd/gateway/nodes.
- Expand harness APIs to allow per-test broker policies (retention, auth).

## Data Integrity and Regression Safety

- Extend ingestion tests to cover confirmation/DLQ failure paths and retries.
- Add fixture helpers that exercise Stage-as-You-Go end-to-end rather than
  publishing synthetic events directly.

## Documentation and Tooling

- Keep `docs/current/testing/README.md` aligned with this roadmap.
- Fold gateway/system testing tasks into the main development priorities doc
  when they become cross-team milestones.
