## Configuration & Agent Patterns

### JSON Output (Always For Agents)

```bash
xtask check --json | jq '.status'           # "success" or "failed"
xtask test --json | jq '.duration_secs'
xtask doctor --json
```

### Test Filtering

`-p` and `-E` are first-class flags. Do NOT use `--` passthrough.

```bash
xtask test -p sinex-primitives                           # Single package
xtask test --debug -E 'test(my_test_name)'               # Specific test, full output
xtask test -E 'package(sinex-primitives) & test(unit::)' # Filter expression
xtask test --heavy                                        # Include #[ignore] tests
xtask test --update-snapshots                             # Insta snapshot updates
```

### sinexctl CLI Commands

```bash
# Querying
sinexctl query --source fs-watcher --limit 10    # Event search
sinexctl trace <event-id>                        # Provenance chain walk
sinexctl trace <event-id> --format dot           # Graphviz output

# Telemetry (reads continuous aggregates)
sinexctl telemetry window-focus                  # Desktop focus tracking
sinexctl telemetry command-frequency             # Shell command frequency
sinexctl telemetry file-activity                 # Filesystem event counts
sinexctl telemetry recent-activity               # Cross-source recent activity
sinexctl telemetry system-state                  # CPU/memory/disk/systemd

# Context & Reports
sinexctl context                                 # "What was I doing?" — last session summary
sinexctl report today                            # Daily summary (top sources, types, heatmap)
sinexctl report yesterday                        # Yesterday's summary

# Import
sinexctl import atuin                            # Import Atuin shell history
sinexctl import activitywatch                    # Import ActivityWatch events

# Operations
sinexctl gateway ingest --source test --type test.ping --payload '{}'  # Smoke test
sinexctl status                                  # System health overview
sinexctl node list                               # Active nodes
```

### Runtime Configuration

NixOS modules are the canonical deployment surface. Binaries read env/CLI into typed config:

```rust
let ingestd = IngestdConfig::from_args(..);           // CLI/env construction
let node = NodeConfig::load_from_env("my-node");      // Env-first typed config
let gateway = GatewayConfig::load();                   // Env-first typed config
```

Deployment config: `nixos/modules/README.md`. Per-binary env vars: owning crate `docs/`.
