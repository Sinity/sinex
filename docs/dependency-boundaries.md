# Dependency boundaries

`xtask boundaries [workspace]` reads Cargo metadata (all features, locked) and checks the dependencies between the six workspace packages:

- `sinex-model` and `xtask` depend on no other workspace package.
- `sinex-protocol` and `sinex-store` may depend on `sinex-model`.
- `sinexd` may depend on `sinex-model`, `sinex-protocol` and `sinex-store`.
- `sinexctl` may depend on `sinex-model` and `sinex-protocol`.

A new workspace package must be added to the policy. Every declared dependency counts, including renamed, optional, dev, build and target-specific ones. `sinex-model` and `sinex-protocol` may not pull in async runtimes, database drivers, TLS, ciphers or logging frameworks, directly or through their normal and build dependency graph.

Only `sinex-store` may use SQLx for PostgreSQL. `sinexd` also reads SQLite sources, so it may depend on SQLx with default features off and only `sqlite` and `runtime-tokio` requested. No other package may depend on SQLx or its component crates.

`xtask build`, `test` and `check` run this check before calling Cargo, and refuse `--manifest-path`, `--config`, `--lockfile-path` and `-C`, which could point Cargo at a different workspace. Run the frontend from the workspace you want to check. The Nix package build and the pre-push hook also run the check.
