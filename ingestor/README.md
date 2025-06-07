# Sinex Ingestors

This directory contains the event ingestors for the Sinex system.

## Structure

```
ingestor/
├── shared/           # Shared libraries and utilities for all ingestors
│   ├── src/         # Source code
│   └── tests/       # Unit tests for shared components
├── filesystem/       # Filesystem change monitoring ingestor
│   ├── src/         # Source code
│   └── tests/       # Filesystem ingestor tests
├── kitty/           # Kitty terminal emulator ingestor
│   └── src/         # Source code
├── hyprland/        # Hyprland window manager ingestor
│   └── src/         # Source code
└── unified/         # Example unified collector using SimpleIngestor

```

## Ingestor Architecture

All ingestors follow the SimpleIngestor pattern:
1. Implement the `SimpleIngestor` trait (focus only on event capture)
2. Use `IngestorRuntime` for lifecycle management (heartbeats, retries, DLQ)
3. Send events through `EventSink` abstraction

## Module Organization

Each ingestor has been organized to reduce file atomization:
- `watcher.rs` - Main implementation with SimpleIngestor trait
- `config.rs` - Configuration structures  
- `cli.rs` - Command-line interface
- `main.rs` - Entry point using IngestorRuntime
- `lib.rs` - Public API exports

## Testing

Tests are organized as follows:
- Unit tests: In `#[cfg(test)]` modules within source files
- Integration tests: In `tests/` directories within each ingestor
- System tests: In `/tests` at project root

Run tests with:
```bash
# All tests
cargo test --workspace

# Specific ingestor tests
cargo test --package filesystem-ingestor
cargo test --package kitty-ingestor
cargo test --package hyprland-ingestor

# Shared component tests
cargo test --package sinex-shared
```