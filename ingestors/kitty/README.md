# Kitty Terminal Ingestor

The Kitty terminal ingestor captures command execution events from the Kitty terminal emulator.

## Current Implementation

The current implementation uses Kitty's remote control protocol to:
1. List active Kitty sessions and windows
2. Get window metadata (PID, CWD, title)
3. Poll for changes at regular intervals

## Limitations

Kitty's remote control protocol doesn't directly expose command history or execution events. The current implementation is a foundation that needs enhancement through one of these approaches:

### Option 1: Shell Integration (Recommended)

Configure your shell to emit special markers that Kitty recognizes:

```bash
# Add to .bashrc or .zshrc
if [[ "$TERM" == "xterm-kitty" ]]; then
    # Shell integration for command tracking
    precmd() {
        echo -ne "\033]133;A\007"
    }
    preexec() {
        echo -ne "\033]133;C\007"
    }
fi
```

### Option 2: Terminal Scrollback Parsing

Use `kitty @ get-text` to retrieve terminal scrollback and parse for command patterns:
- Detect shell prompts
- Extract commands between prompts
- Track execution times based on prompt timestamps

### Option 3: Shell History Integration

Monitor shell history files:
- `~/.bash_history`
- `~/.zsh_history`
- Correlate with Kitty window PIDs

## Configuration

Create a configuration file at `~/.config/sinex/kitty-ingestor.toml`:

```toml
[database]
url = "postgresql://localhost/sinex"
max_connections = 5

[logging]
level = "info"
format = "pretty"

[kitty]
socket_path = "/tmp/kitty-*"
polling_interval_secs = 5
command_timeout_secs = 30
heartbeat_interval_secs = 60
```

## Running

```bash
# Check database connection
kitty-ingestor check

# Run the ingestor
kitty-ingestor run

# Generate example config
kitty-ingestor generate-config
```

## Events Produced

### `terminal.kitty.command_executed`

```json
{
  "command_string": "ls -la",
  "cwd": "/home/user/projects",
  "exit_code": 0,
  "ts_start_orig": "2024-01-15T10:30:00Z",
  "ts_end_orig": "2024-01-15T10:30:01Z"
}
```

## Future Enhancements

1. **Real-time Command Detection**: Implement shell integration markers
2. **Command Output Capture**: Optionally capture command output
3. **Session Tracking**: Track terminal session lifecycle
4. **Multi-Shell Support**: Handle different shell configurations
5. **Performance Metrics**: Track command execution time and resource usage