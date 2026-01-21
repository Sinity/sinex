Meta-reflection
- This survey focused on tokio mpsc/oneshot/watch; I did not map all NATS JetStream streams or SQLx connection pools, which are important flow controls outside Rust channels.
- Several channel sites are in tests or tooling; I excluded most of them, so this is not a full inventory.
- I did not validate runtime behavior under load; backpressure and drop behaviors should be confirmed with metrics/tracing.
