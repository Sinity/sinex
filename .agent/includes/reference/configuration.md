## Configuration & Agent Patterns

### Runtime Configuration

NixOS modules are the canonical deployment surface. Binaries read env/CLI into typed config:

```rust
let ingestd = IngestdConfig::from_args(..);           // CLI/env construction
let node = NodeConfig::load_from_env("my-node");      // Env-first typed config
let gateway = GatewayConfig::load();                   // Env-first typed config
```

Deployment config: `nixos/modules/README.md`. Per-binary env vars: owning crate `docs/`.
