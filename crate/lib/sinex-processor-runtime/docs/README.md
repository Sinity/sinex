# Sinex Processor Runtime

A unified CLI and runtime framework for Sinex nodes (processors).

## Features

-   **Standardized CLI**: `service`, `scan`, `explore` subcommands for all nodes.
-   **Configuration**: Layered config (CLI > Env > JSON) with validation.
-   **Replay**: Built-in support for replaying event streams.
-   **Coordination**: Leader election and heartbeat integration.

## Usage

Nodes typically use the `processor_main!` macro:

```rust
use sinex_processor_runtime::processor_main;

processor_main!(MyProcessor);
```

See `docs/cli_framework.md` for architectural details.