# Runtime Configuration

NixOS modules are the canonical deployment surface. `sinexd` reads env/CLI into
typed config for its modules:

```rust
let event_engine = EventEngineConfig::from_args(..);      // event-engine CLI/env construction
let module = RuntimeConfig::load_from_env("my-source");  // source/automaton env-first typed config
let api = GatewayConfig::load();                      // API env-first typed config
```

Deployment config: `nixos/modules/README.md`. Per-module env vars live in the
owning crate docs, especially `crate/sinexd/docs/api/` and
`crate/sinexd/docs/event_engine/`.

## Startup storage preflight

The deployed `sinexd.service` invokes `sinexd ... serve`. That route runs the
fail-closed storage preflight before constructing event-engine or API
configuration and before starting the supervisor. If any required filesystem
cannot be inspected or has less than its configured floor, `sinexd` exits
non-zero and does not start ingestion or serving.

The check uses the configured state, content-store/data, temporary, log, and
work paths. A path that has not been created yet is checked through its nearest
existing ancestor, so the gate measures the filesystem that would receive the
runtime data. The current gate is a point-in-time free-space floor; it does not
project storage growth or estimate time-to-full.

## Runtime liveness and startup readiness

Runtime-facing status uses the shared `sinex_primitives::runtime_liveness`
evaluation. Its default freshness window is 300 seconds and callers may pass a
different `stale_after_secs` value explicitly; source coverage views carry that
same value through their request instead of selecting a separate consumer
threshold. The result is recency-aware and includes the latest observation,
age, status, and evidence. Failed or stopped runs remain unhealthy even when
their last output is recent; draining and paused runs are degraded.

The continuity list, family report, and gap explanation routes accept the same
window as `stale_after_secs`; `sinexctl sources continuity` and its gap command
expose it as `--stale-after-seconds`. Source readiness retains its separate
`stale_after_seconds` material-freshness input because readiness rows do not
have a safe source-to-runtime identity join. Runtime failure and heartbeat
evidence therefore come from source status and continuity, while readiness
describes staged material and parser coverage.

Hosted source bindings and automata receive a small randomized startup delay,
and crash retries add bounded jitter while preserving the 30-second maximum
backoff. A source runtime sends `READY=1` only after its snapshot and any
required gap-fill phase complete. Bridge-backed automata similarly wait for
historical catch-up and the durable live consumer before notifying readiness.
