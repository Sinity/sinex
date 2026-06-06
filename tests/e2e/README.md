# Sinex End-to-End Tests

This crate hosts Rust integration coverage that spans the deployed runtime path
(source drivers -> event engine -> core storage -> API) as well as the NixOS module
assertions and VM harness.

## Prerequisites

- `nats-server` must be available on `$PATH` for the ingest pipeline tests.
  The devShell pins it via `NATS_SERVER_BIN`, so prefer running under the
  project devShell (`direnv` or `nix develop`).
- PostgreSQL must be reachable at the location used by `sinex_test_utils`
  (the standard dev shell sets the required environment variables). `xtask test`
  runs the repo preflight that starts/repairs the local infra stack when needed.
- Tests that spawn `sinexd` require the runtime binary in the active
  target directory. Use `xtask test`, not bare `cargo nextest`; xtask prepares
  stale or missing runtime binaries before launching nextest.
- Python 3 is required for the CLI smoke check (`python3 -m compileall`).

## Running

```bash
xtask test -p sinex-e2e-tests
xtask test -p sinex-e2e-tests -E 'test(test_batch_large_payloads)'
```

For simple `test(name)` filters, xtask infers the owning e2e test binary and
passes the matching nextest `--test` target internally. Use explicit `--test
<binary>` only when the filter is too complex to infer.

For the exported NixOS VM checks, use:

```bash
xtask test vm --category smoke
xtask test vm --list
```
