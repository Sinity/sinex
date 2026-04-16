## sinexctl

- `sinexctl context`: recover recent activity context when the task is "what was I just doing?" Use `sinexctl context --since 4h` for a wider window.
- `sinexctl report today`: produce daily summaries and heatmaps. Use `sinexctl report yesterday` for the previous day.
- `sinexctl telemetry ...`: inspect continuous aggregates such as `window-focus`, `command-frequency`, `file-activity`, `recent-activity`, and `system-state`.
- `sinexctl query` and `sinexctl trace`: search captured events and follow provenance chains. Use `sinexctl trace <event-id> --format dot` for graph output.
- `sinexctl gateway ingest`: send a provenance-valid smoke event through gateway -> NATS -> ingestd, for example `sinexctl gateway ingest --source test --event-type test.ping --payload '{}'`.
- `sinexctl watch`, `sinexctl tui`, `sinexctl status`, and `sinexctl recent`: use these for live observation, dashboard views, and quick health/recent-event checks.
