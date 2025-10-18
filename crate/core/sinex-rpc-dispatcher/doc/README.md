# sinex-rpc-dispatcher

`sinex-rpc-dispatcher` adapts the unified `StatefulStreamProcessor` interface to
serve RPC clients. The processor accepts scan requests, streams checkpoints, and
coordinates historical replays across satellites.

- Wraps the stream processor trait implementations from `sinex-satellite-sdk`.
- Exposes configuration for concurrency, timeouts, and historical scan windows.
- Provides a transport-friendly API that gateways can embed.

Related architecture context is captured in
`docs/architecture/UserInteraction_And_Query_Architecture.md`.
