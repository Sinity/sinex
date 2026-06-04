# Runtime Configuration

NixOS modules are the canonical deployment surface. `sinexd` reads env/CLI into
typed config for its modules:

```rust
let event_engine = EventEngineConfig::from_args(..);      // event-engine CLI/env construction
let node = NodeConfig::load_from_env("my-node");      // source/automaton env-first typed config
let api = GatewayConfig::load();                      // API env-first typed config
```

Deployment config: `nixos/modules/README.md`. Per-module env vars live in the
owning crate docs, especially `crate/sinexd/docs/api/` and
`crate/sinexd/docs/event_engine/`.
