## sinexctl

- `sinexctl context`: recover recent activity context when the task is "what was I just doing?" Use `sinexctl context --since 4h` for a wider window.
- `sinexctl report today`: produce daily summaries and heatmaps. Use `sinexctl report yesterday` for the previous day.
- `sinexctl telemetry ...`: inspect continuous aggregates such as `window-focus`, `command-frequency`, `file-activity`, `recent-activity`, and `system-state`.
- `sinexctl query` and `sinexctl trace`: search captured events and follow provenance chains. Use `sinexctl trace <event-id> --format dot` for graph output.
- `sinexctl watch`, `sinexctl tui`, `sinexctl status`, and `sinexctl recent`: use these for live observation, dashboard views, and quick health/recent-event checks.
- `sinexctl automata`: list registered derived-node automata with run/checkpoint state and operator-facing runtime telemetry. The table surfaces `derived.event_lag_p50_ms` / `derived.event_lag_p99_ms` (sliding-reservoir percentiles of upstream-`ts_orig` to dispatch lag), `derived.tick_runtime_p99_ms` (per-tick runtime), and `derived.throughput_eps` (events/sec over the live throughput window) — emitted from `DerivedNodeAdapter::observe_processing_latency` and read back via `automata.status` against the latest matching `metric.gauge` events.
