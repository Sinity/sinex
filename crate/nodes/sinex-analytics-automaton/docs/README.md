# sinex-analytics-automaton

The analytics automaton consumes trusted material activity signals and emits
bounded `activity.window.summary` rollups.
It implements the `WindowedNode` interface from `sinex-node-sdk` via
`WindowedNodeAdapter` and acts as the first aggregate layer for the activity
timeline.

- Subscribes to all material events but only accumulates trusted activity
  signals from window focus, browser activity, and terminal commands.
- Closes windows on real gaps, max-duration bounds, or parent-count budgets so
  synthesis provenance stays under the DB parent hard limit.
- Emits replay-stable window summaries that session-level rollups can consume
  instead of pointing directly at raw events.
- Maintains checkpoint state for reliable replay.

Reference `README.md#deployment--operations` for the operator path and
`crate/lib/sinex-node-sdk/docs/overview.md` for the shared node architecture.
