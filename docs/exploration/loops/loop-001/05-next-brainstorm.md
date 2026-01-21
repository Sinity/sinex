# Loop 001 - Next Analysis Brainstorm

- Checkpoint cleanup wiring: config exists but is the cleanup task ever started?
- Env var contract audit: find env vars used in code that are not documented (or vice versa).
- Replay control lifecycle: ensure background tasks can be shut down cleanly without dropping requests.
- Node shutdown consistency: compare node-specific `shutdown()` implementations for flush, checkpoint, and watcher teardown.
