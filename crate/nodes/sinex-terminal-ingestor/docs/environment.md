# Terminal Ingestor Environment Variables

Environment variables specific to `sinex-terminal-ingestor`.

## Configuration

```bash
# Session ID (auto-detected from TERM_SESSION_ID if not set)
SINEX_SESSION_ID="terminal-session-123"
```

## System Detection

The terminal ingestor also reads these system variables:

```bash
# Kitty terminal IPC socket
KITTY_LISTEN_ON="unix:/tmp/kitty-socket"

# Standard terminal session ID
TERM_SESSION_ID="..."
```

## See Also

- Global env vars: `docs/current/configuration/environment-variables.md`
- Node SDK: `crate/lib/sinex-node-sdk/docs/`
