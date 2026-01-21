Concrete issues to handle
- Stream processor event processor shutdown channel is never signaled and the JoinHandle is never joined/aborted; add explicit shutdown and join to avoid orphaned tasks (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:468-574`, `:1090-1102`).
- Automaton event bridge spawns a JetStream consumer without lifecycle management; consider cancellation or join when exiting the bridge (`crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs:864-884`).
- Gateway metrics emission and rate limiter cleanup tasks have no shutdown signal; either wire cancel channels or document that they are intentionally detached (`crate/core/sinex-gateway/src/rpc_server.rs:1305-1318`, `crate/core/sinex-gateway/src/rate_limit.rs:169-178`).
- Replay control subscription loop and telemetry sampler are detached and run indefinitely; add cancellation hooks or track handles in the service container (`crate/core/sinex-gateway/src/replay_control.rs:343-363`, `:567-589`).
- GatewayAuth token file watcher thread runs forever without shutdown; consider integrating with process shutdown signals (`crate/core/sinex-gateway/src/rpc_server.rs:178-255`).
