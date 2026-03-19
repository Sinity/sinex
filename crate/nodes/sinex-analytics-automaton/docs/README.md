# sinex-analytics-automaton

The analytics automaton consumes event streams and emits synthesized insights.
It implements the `WindowedNode` interface from `sinex-node-sdk` via
`WindowedNodeAdapter` and is responsible for turning raw events into aggregated
analytics over time windows.

- Listens for events from filesystem, desktop, and other nodes.
- Aggregates events within time windows and produces summary metrics.
- Maintains checkpoint state for reliable replay.

Reference `OPERATIONS.md` for
consumer dashboards and `crate/lib/sinex-node-sdk/docs/overview.md` for the
shared node architecture.
