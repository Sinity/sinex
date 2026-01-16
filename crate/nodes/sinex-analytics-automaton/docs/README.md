# sinex-analytics-automaton

The analytics automaton consumes event streams and emits synthesized insights.
It implements the shared `StatefulStreamProcessor` traits from
`sinex-node-sdk` and is responsible for turning raw events into aggregated
analytics.

- Listens for events from filesystem, desktop, and other nodes.
- Produces derived events and metrics requested by gateways.
- Maintains checkpoint state for reliable replay.

Reference `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md` for
consumer dashboards and `crate/lib/sinex-node-sdk/docs/overview.md` for the
shared processor architecture.
