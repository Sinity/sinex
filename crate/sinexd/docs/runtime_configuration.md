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
