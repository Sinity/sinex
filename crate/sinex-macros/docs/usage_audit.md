# Macro Usage Audit

**Status**: 2026-06-01

## Usage Summary

| Macro | Status | Notes |
|-------|--------|-------|
| `#[derive(EventPayload)]` | Active in production | Implements `EventPayload`, fluent setters, and monomorphic schema inventory registration for payload structs. |
| `#[derive(SinexConfig)]` | Active in production | Generates env-driven `from_env()` constructors for declarative configuration structs. Rollout tracked by #1589. |

## `SinexConfig` Rollout State

The derive is appropriate when each field can be loaded independently from a
stable env key, optional custom parser, nested config, or default expression.
The current branch has the duration support needed by #1589 and uses it on the
remaining sinexd duration-shaped configs.

| Config | State | Evidence |
|---|---|---|
| `NatsConnectionConfig` | Derived | `crate/sinex-primitives/src/nats.rs` derives `SinexConfig`. |
| `HealthThresholds` | Derived | `crate/sinexd/src/node_sdk/health_reporter.rs` derives `SinexConfig` with fallible normalization. |
| `CascadeAnalyzerConfig` | Derived | `crate/sinexd/src/api/cascade_analyzer.rs` uses `duration_secs` for `SINEX_CASCADE_TIMEOUT_SECS`. |
| `CheckpointCleanupConfig` | Derived | `crate/sinexd/src/node_sdk/checkpoint.rs` derives `SinexConfig` and keeps day/hour duration parsers. |
| `GatewayConfig` | Derived | `crate/sinexd/src/api/config.rs` derives `SinexConfig`; `load()` performs post-load database URL resolution. |

## Permanent Hand-Rolled Configs

These are intentional exceptions, not missed derive rollout items.

| Config | Why it remains hand-rolled |
|---|---|
| `NativeMessagingConfig` | Env values feed `from_raw()`, which constructs private parsed maps, extension role maps, trusted host maps, and rate-limiter state rather than assigning env fields directly. |
| `SelfObserverConfig` | `from_env(component: &str)` takes a runtime component identity; the derive intentionally generates zero-argument constructors only. |
| `HealthAggregatorConfig` | Parses `SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS` as a JSON map and validates component intervals as one unit. |
| `PrivacyConfig` | Loads a TOML file selected by env/state-dir first, then applies env overrides with typed privacy parsers and config-specific errors. |
| `PoolConfig` | The hand-written loader preserves a narrow env surface for only `max_connections`, `min_connections`, and `acquire_timeout_secs`; deriving would either expand the env contract for the remaining fields or need a new no-env/default-field attribute. |
| `CoordinationTiming` | Private runtime timing helper with nonstandard env names and zero-as-default fallback semantics for three coordinated intervals. |

## Recommendations

- Keep `EventPayload` and `SinexConfig` derive behavior documented beside the
  proc macro implementation.
- Treat new hand-written `from_env()` impls as design exceptions: document why
  the config cannot be expressed as declarative env-to-field mapping.
- Add targeted macro tests for each new `SinexConfig` attribute before using it
  in production config.
