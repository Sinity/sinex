# sinex-session-detector

The session detector groups bounded `activity.window.summary` rollups into
completed activity sessions.
It no longer points session boundaries directly at raw events; it consumes the
bounded activity-window layer and emits exact synthesized provenance over those
window summaries.

It implements the `WindowedNode` interface from `sinex-node-sdk` via
`WindowedNodeAdapter` and emits `activity.session.boundary` events containing
session metadata (duration, dominant activity source, contributing sources,
window count).

- Subscribes specifically to `activity.window.summary` synthesized outputs.
- Tracks current session state as a rollup over bounded windows rather than raw
  event IDs, keeping provenance exact while capping fan-in.
- Emits a boundary event when a gap-closed window arrives.
- Uses `SyntheticTemporalPolicy::WindowBoundary` for replay-correct timestamps.

Reference `README.md#deployment--operations` for the operator path and
`crate/lib/sinex-node-sdk/docs/overview.md` for the shared node architecture.
