# Loop 003 - Concrete Issues

1) Replay control requests are processed serially without an explicit concurrency or time budget
- Evidence: `ReplayControlServer::spawn()` in `crate/core/sinex-gateway/src/replay_control.rs` handles each message by awaiting `handle_message()` inside the subscription loop.
- Impact: long-running replay operations can block subsequent requests, causing head-of-line blocking under concurrency.

2) Replay control client timeout is hardcoded and not configurable
- Evidence: `ReplayControlClient::send()` uses `Duration::from_secs(30)` in `crate/core/sinex-gateway/src/replay_control.rs`.
- Impact: operations that legitimately take longer than 30s will be reported as failures; retry behavior may be ambiguous.
