Next analysis brainstorm
- Error origin tracing for sinex-node-sdk and gateway: map where errors are created vs logged, and whether context is preserved.
- Channel topology mapping: list mpsc/broadcast/oneshot usage and identify backpressure strategies.
- Task lifetime audit: enumerate tokio::spawn sites and verify they are joined or shutdown cleanly.
