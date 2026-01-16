# Sinex End-to-End Tests

This crate hosts Rust integration coverage that spans multiple runtime crates
(staging satellites ➜ ingestd ➜ core ➜ services ➜ gateway) as well as the NixOS
module assertions and VM harness.

## Prerequisites

- `nats-server` must be available on `$PATH` for the ingest pipeline tests.
  The dev shell pins it via `NATS_SERVER_BIN` (see `devenv.nix`), so prefer
  running under `direnv exec /realm/project/sinex` or `devenv shell`.
- PostgreSQL must be reachable at the location used by `sinex_test_utils`
  (the standard dev shell sets the required environment variables).
- Python 3 is required for the CLI smoke check (`python3 -m compileall`).

## Running

```bash
cargo nextest run -p sinex-e2e-tests
```

To execute the VM scenarios, use the helper script (requires Nix):

```bash
./tests/e2e/nixos-vm/run-vm-tests.sh -c smoke
```
