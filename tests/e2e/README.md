# Sinex End-to-End Tests

This crate hosts Rust integration coverage that spans multiple runtime crates
(staging nodes ➜ ingestd ➜ core ➜ services ➜ gateway) as well as the NixOS
module assertions and VM harness.

## Prerequisites

- `nats-server` must be available on `$PATH` for the ingest pipeline tests.
  The devShell pins it via `NATS_SERVER_BIN`, so prefer running under the
  project devShell (`direnv` or `nix develop`).
- PostgreSQL must be reachable at the location used by `sinex_test_utils`
  (the standard dev shell sets the required environment variables).
- Python 3 is required for the CLI smoke check (`python3 -m compileall`).

## Running

```bash
xtask test -p sinex-e2e-tests
```

For the exported NixOS VM checks, use:

```bash
xtask test vm --category smoke
xtask test vm --list
```
