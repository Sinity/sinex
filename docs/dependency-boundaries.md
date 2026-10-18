# Dependency boundaries

`xtask boundaries [workspace]` reads Cargo metadata (all features, locked) and checks the dependencies between the workspace packages:

- `sinex-model` and `xtask` depend on no other workspace package.
- `sinex-protocol` and `sinex-store` may depend on `sinex-model`.
- `sinexd` may depend on `sinex-model`, `sinex-protocol` and `sinex-store`.
- `sinexctl` may depend on `sinex-model` and `sinex-protocol`.
- `sinex-testkit` (test helpers) may depend on `sinex-model`, `sinex-protocol` and `sinex-store`; any package may use it as a dev-dependency, none as a normal one.

A new workspace package must be added to the policy. Every declared dependency counts, including renamed, optional, dev, build and target-specific ones. `sinex-model` and `sinex-protocol` may not pull in async runtimes, database drivers, TLS, ciphers or logging frameworks, directly or through their normal and build dependency graph.

Only `sinex-store` (and the test helpers in `sinex-testkit`) may use SQLx for PostgreSQL. `sinexd` also reads SQLite sources, so it may depend on SQLx with default features off and only `sqlite` and `runtime-tokio` requested. No other package may depend on SQLx or its component crates.

The pre-push hook, `xtask check` and the Nix package build run the check. Its unit tests are in `xtask/src/boundaries/tests.rs`.
