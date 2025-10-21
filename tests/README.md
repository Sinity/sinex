# Workspace Test Harness

Most high-signal coverage now lives beside the crates it exercises. In
particular, `sinex-core` owns the former `unit`, `system`, `performance`, and
`adversarial` suites, while the satellite SDK exports its integration and
property harness from `crate/lib/sinex-satellite-sdk/tests/`.

What remains under `tests/` is deliberately narrow and cross-cutting:

- **`integration/`** – scenarios that still touch multiple crates (e.g. Nix
  module wiring, Stage-as-you-go) or depend on legacy harness glue. Plan to
  migrate into crate-owned suites when feasible.
- **`property/`** – workspace-level fuzzing that genuinely spans crates. These
  modules back the `property_tests.rs` harness.
- **`property_tests.rs`** – thin wrapper that pulls the property modules into a
  single integration target for Nextest.
- **`examples/`** – documentation-style snapshots that demonstrate modern
  testing patterns with `sinex-test-utils`.
- **`nixos-vm/`** – NixOS VM blueprints and chaos scenarios. See
  `tests/nixos-vm/README.md` for runner details.
- **`scripts/` & `unified-test-runner.py`** – helper utilities for migrating and
  running suites.

## Running the Workspace Suites

```bash
# Property harness (uses proptest regressions under tests/property/)
cargo nextest run --test property_tests

# Remaining cross-crate integration cases
cargo nextest run --test multi_source_integration_test
cargo nextest run --test stage_as_you_go_integration_test
# ...or simply: just test --tests integration
```

When adding new coverage, default to the owning crate’s `tests/` directory.
Only keep scenarios under `tests/` when the flow truly spans multiple crates or
requires the legacy harness.
