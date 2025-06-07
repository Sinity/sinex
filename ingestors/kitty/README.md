# Kitty Terminal Ingestor

Captures terminal activity from Kitty terminal emulator sessions.

## Features

- Monitors all Kitty terminal instances
- Tracks window metadata (CWD, PID)
- Polls for terminal state changes
- Foundation for command tracking

## Usage

```bash
# Run with default config
cargo run --bin kitty-ingestor

# Dry run (logs events to console)
cargo run --bin kitty-ingestor -- --dry-run

# Output to file instead of database
cargo run --bin kitty-ingestor -- --output-file events.json

# Use custom config
cargo run --bin kitty-ingestor -- --config config/kitty/production.toml

# Show current configuration
cargo run --bin kitty-ingestor -- config

# Check database connection
cargo run --bin kitty-ingestor -- check
```

## Configuration

Configuration uses TOML format. See `config/kitty/` for examples.

Key settings:
- `socket_path` - Pattern for finding Kitty sockets
- `polling_interval_secs` - How often to check for changes
- `max_tracked_windows` - Limit on tracked windows

## Current Limitations

Kitty's remote control API doesn't directly expose command execution events. The current implementation provides infrastructure for future enhancements:

1. **Shell Integration** - Configure shells to emit markers
2. **Scrollback Parsing** - Extract commands from terminal output
3. **History File Monitoring** - Watch shell history files

## Events Captured

Currently limited - the infrastructure is ready but needs shell integration for meaningful command capture:

### terminal.command_executed (future)
```json
{
  "command_string": "ls -la",
  "cwd": "/home/user/projects",
  "exit_code": 0,
  "ts_start_orig": "2024-01-15T10:30:00Z",
  "ts_end_orig": "2024-01-15T10:30:01Z"
}
```

## Architecture

Uses the SimpleIngestor pattern - the ingestor polls Kitty state while IngestorRuntime handles:
- Heartbeats
- Error recovery and retries
- Dead letter queue
- Graceful shutdown