# Opportunities & Tooling Directions

## Recent Completions (2026-01-23)

The following items from this exploration have been implemented:

### CI & Quality Gates

- **Mutation testing**: Added `cargo xtask mutants` command wrapping cargo-mutants with `--package`, `--file`, `--timeout`, `--jobs` options. Reference: `xtask/src/main.rs`

- **Coverage enforcement**: Added `cargo xtask coverage enforce` subcommand with configurable `--threshold` (default 60%), optional `--html` report generation, and CI-friendly JSON output. Reference: `xtask/src/main.rs`

- **SQLx compile-time verification**: Added `cargo xtask sqlx` command with `check` (verify against .sqlx/), `prepare` (regenerate cache), and `verify` (prepare then check) subcommands. Wired `sqlx_check()` into `ci_preflight()`. Reference: `xtask/src/main.rs`

- **cargo-deny integration**: Wired `cargo deny check` into `ci_preflight()` for supply chain security scanning.

- **Dependency visualization**: Added `cargo xtask graph` command using cargo-depgraph for dependency graph rendering.

### Testing Infrastructure

- **Property-based testing**: Expanded property tests with adversarial strategies for SQL injection, path traversal, command injection, and overflow payloads. Added `sinex_prop` macro and builtin strategies. Reference: `sinex-test-utils/src/property_testing.rs`

- **Test timing histograms**: Added `cargo xtask history tests slowest` and `getting-slower` commands for test performance regression detection. Reference: `xtask/src/main.rs`

- **Secure TLS nextest profile**: Added nextest profile for TLS/nkey-enabled test suites with chaos injection support. Reference: `.config/nextest.toml`

- **Chaos testing integration**: Added `ChaosConfig` and chaos injection support to node SDK for fault tolerance testing. Reference: `sinex-node-sdk/src/chaos.rs`

- **EphemeralNats nkey support**: Extended `EphemeralNatsBuilder` with nkey authentication knobs for isolated test accounts. Reference: `sinex-test-utils/src/nats/`

### TLS & Security

- **TLS bootstrap utilities**: Added `cargo xtask tls generate-dev-certs`, `check`, `generate-client-cert`, and `setup-env` commands for local/CI TLS setup. Reference: `xtask/src/tls.rs`

- **TLS NixOS integration**: Documented NixOS module integration for TLS certificate management. Reference: `docs/current/configuration/tls-nixos-integration.md`

### Documentation

- **GitOps workflow**: Added GitOps workflow documentation for deployment patterns. Reference: `docs/current/operations/`

### Previous Completions (2026-01-22)

- **Pool acquire timeout metrics**: Added `#[tracing::instrument]` to `acquire_with_timeout()` with pool metrics (size, idle, acquire_ms). Warning threshold configurable via `SINEX_POOL_ACQUIRE_WARN_MS`. Reference: `sinex-core/src/db/mod.rs`

- **Schema validation coverage metrics**: Added `ValidationStats` and `ValidationStatsSnapshot` to track Valid/Skipped/NoSchema/SchemaNotFound/Invalid outcomes. Accessible via `EventValidator::stats()`. Reference: `sinex-ingestd/src/validator.rs`

- **NATS subject registry**: Created canonical documentation at `docs/current/architecture/nats-subjects.md` covering all subjects, streams, and naming conventions.

- **Lint tooling**: Extended `cargo xtask lint-forbidden` with:
  - unwrap/expect count reporting (informational)
  - SQLx compile-time vs runtime query statistics
  - sinex_test_utils layering check

## Data & Analytics

- Embed columnar engines (DataFusion, Polars) alongside Postgres exports to keep analytics/service workloads off the shared pool. We can mirror `core.events` into Parquet snapshots or DuckDB databases, letting gateway/CLI queries run locally without bypassing auth.
- Adopt dbt or SQLGlot-based schema manifests that consume the `sinex-schema` SeaQuery definitions. That would let us lint for predicate pushdown regressions and generate documentation from the single source of truth.
- Introduce an internal `sinex-analytics` crate that wraps DataFusion’s logical plan API and exposes pre-built views (`events_by_offset`, `material_latency`), so nodes/CLI tests can reuse the same aggregations without touching production tables. Pair it with `arrow-flight` to ship snapshot queries over RPC securely.
- Use `SeaORM`’s entity scaffolding or `sqlc`-style codegen against the SeaQuery builders to guarantee RPC schemas (gateway handlers) stay in lockstep with DB models. That reduces the manual JSON parsing noted in `sinex-gateway/src/handlers.rs`.

## Streaming & Automata

- For command canonicalization, search, and PKM automata, evaluate streaming DBs (Materialize, RisingWave). They already handle temporal dedupe, checkpointing, and replay isolation, eliminating the custom JetStream consumer glue that currently stalls.
- Stage-as-You-Go would benefit from an async streaming API—e.g., expose `AsyncRead`/`AsyncWrite` traits so nodes can stream large captures without buffering twice. Pair that with deterministic tmp dirs (via `tempfile::NamedTempFile`) and atomic finalize helpers.
- Adopt `nats-supercluster` or `nsc` (NATS account tooling) during tests so we can spin up per-suite JetStream + TLS accounts automatically. `EphemeralNats` could shell out to `nsc` to generate creds/nkeys on the fly, giving suites isolation without manual config.
- Consider `opendal` or `s3fs` integrations for `git-annex` remotes so material assembler pipelines can push directly to object storage, letting automata read from S3-compatible caches instead of local annex stores.

## Observability & Security

- Introduce tracing propagation (tracing-opentelemetry with OTLP/Jaeger) through gateway Tower stacks and ingestd/service layers. That gives us visibility into rate-limit hits, TLS decisions, and replay bypass toggles.
- Ship OpenTelemetry metrics from nodes via the resurrected `auto_metrics` macro, so we can monitor Stage-as-You-Go handle counts, acquisition retries, and JetStream lag uniformly.
- Extend tracing to include `async_nats` spans via `tracing-nats` or custom layers that record subject/consumer metadata. Coupled with OTLP metrics, we could alert on JetStream ack latency per node.
- Use `rustls-platform-verifier` (or `webpki-roots`) plus `step-ca`/`mkcert` inside the harness to exercise TLS/nkey flows automatically. Providing a `sinex tls bootstrap` xtask that generates CA/client certs keeps local and CI configs aligned.
- For security reviews, integrate `cargo audit`, `cargo deny`, and `trivy` scans into xtask so TLS/auth regressions surface in a single “security” gate.

## Testing & Infrastructure

- Pin `nats-server` binaries via Nix or container images for tests/e2e. Add TLS-enabled fixtures so `async_nats::ConnectOptions` gets exercised. Replace the global `OnceCell` JetStream with per-test handles or a pool keyed by namespace.
- Extend `sinex-test-utils` with harness modes that start RisingWave/Materialize or DataFusion sessions, letting us test analytics pipelines end-to-end before introducing new dependencies in production.
- Provide `EphemeralNatsBuilder` that exposes TLS/nkey knobs, merges YAML config fragments, and spawns multiple brokers for fanout/replication tests. Pair it with `TestContext::with_nats_builder(builder)` so suites can request TLS or custom retention while still benefiting from shared teardown.
- Bake `nextest` profiles for “secure transport” suites that enable TLS/nkeys plus chaos injection. This ensures every binary (ingestd, gateway, nodes) hits the TLS paths before release.
- Add `cargo-nextest` aware fixtures for `git-annex` (maybe via `git-annex testremote`) ensuring document/desktop nodes exercise annex interactions even during unit tests.

## Workflow & Dev Experience

- Reinstate procedural macros (auto_metrics, validation_chain) on syn 2.x and move doc `include_str!` to `html_root_url` to cut rebuild cost. Supplement the flat `sinex-core` facade with optional “facet” preludes (types/db/env) plus xtask reports that highlight root-level exports, so developers keep the convenience while still seeing the dependency weight.
- Expose typed RPC client generation (maybe via `tonic-build`-style codegen) so CLI and nodes don’t hand-roll JSON payloads.
- Build xtask “flattening health” commands (depgraphs, udeps, timings) so PR authors know when they’ve added heavy exports. Pair that with CI bots that annotate PRs when `sinex-core` changes trigger large rebuild cascades.
- Introduce an `xtask graph` command that renders dependency graphs (via `cargo depgraph` or `guppy`) so we can visualize flattening hot spots. Combine with `cargo machete`/`cargo udeps` to trim unused re-exports.
- Build a `sinex-labs` playground (maybe leveraging `wry` or `tauri`) where developers experiment with Stage-as-You-Go capture pipelines interactively. Feeding those experiments back into CLI docs improves onboarding.
- Document and extend the new `xtask dev tls-fixtures` helper so TLS fixtures stay fresh and we can add nkey/account scenarios without bespoke scripts per developer.
