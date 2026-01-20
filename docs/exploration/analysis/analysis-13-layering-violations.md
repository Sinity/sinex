# Layering Violations (Core vs SDK vs Tests)

Scope
- Ensure core crates do not depend on higher-level crates in production.

Method
- rg for sinex_node_sdk and sinex_test_utils usage in non-test code; inspect Cargo.toml deps.

Findings
- sinex-core depends on sinex-node-sdk only as a dev-dependency for tests (crate/lib/sinex-core/Cargo.toml:97-106). No production code path imports sinex_node_sdk.
- Non-test modules that mention sinex_test_utils are typically inside #[cfg(test)] blocks in the same file; there were no production (non-test) uses found via rg filters.

Observations
- Current layering is clean: sinex-node-sdk depends on sinex-core (expected), not the other way around in production builds.

Follow-ups
- Keep an eye on new core modules that might pull in sinex-node-sdk or test utils outside #[cfg(test)].
- Consider a lint that fails if sinex-test-utils is used in non-test code paths.
