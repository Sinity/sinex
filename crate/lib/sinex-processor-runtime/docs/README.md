# sinex-processor-runtime

Standardized CLI framework and runtime utilities for all Sinex node binaries.

## Purpose

This crate provides the shared CLI infrastructure used by all nodes (fs-ingestor, terminal-ingestor, desktop-ingestor, health-automaton, etc.). It ensures consistent:

- **CLI structure** - All nodes use the same subcommand pattern
- **Configuration** - Unified NATS, database, and logging args
- **Execution modes** - Service, Scan, Explore, Replay, Checkpoint commands

## Relationship to sinex-node-sdk

```
sinex-node-sdk          sinex-processor-runtime
────────────────        ─────────────────────────
Core node logic         CLI + runtime wrappers
├── EventProcessor      ├── ProcessorCli (clap)
├── StreamProcessor     ├── ProcessorCommand enum
├── Checkpoint          ├── ProcessorCliRunner
├── ReplayService       └── Re-exports replay types
└── HeartbeatEmitter
```

- **sinex-node-sdk**: Library code for building nodes (processing logic, checkpoints, heartbeats)
- **sinex-processor-runtime**: Binary entrypoint code (CLI parsing, command dispatch, runtime setup)

## Standard Subcommands

All nodes built with this framework support:

```
my-node service     # Run as persistent service (normal mode)
my-node scan        # One-shot scan with exit
my-node explore     # Interactive exploration mode
my-node replay      # Replay historical events
my-node checkpoint  # View/manage checkpoints
```

## Usage

```rust
use sinex_processor_runtime::{ProcessorCli, ProcessorCliRunner, ProcessorCommand};
use clap::Parser;

fn main() -> color_eyre::Result<()> {
    let cli = ProcessorCli::parse();

    // Dispatch based on command
    match &cli.command {
        ProcessorCommand::Service { .. } => run_service(&cli),
        ProcessorCommand::Scan { .. } => run_scan(&cli),
        ProcessorCommand::Explore { .. } => run_explore(&cli),
        // ...
    }
}
```

## Configuration

Common CLI arguments available to all nodes:

| Flag | Env Variable | Description |
|------|--------------|-------------|
| `--nats-url` | `SINEX_NATS_URL` | NATS server URL |
| `--database-url` | `DATABASE_URL` | PostgreSQL connection |
| `--service-name` | - | Service identifier for metrics |
| `--work-dir` | - | Temp file directory |
| `-v/-vv/-vvv` | - | Verbosity level |

## See Also

- Node SDK: `crate/lib/sinex-node-sdk/docs/overview.md`
- Example node: `crate/nodes/sinex-fs-ingestor/`
