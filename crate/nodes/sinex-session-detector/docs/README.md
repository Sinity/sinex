# sinex-session-detector

The session detector groups trusted activity signals into activity sessions.
A gap of more than 5 minutes between consecutive activity events marks a session boundary.

It implements the `WindowedNode` interface from `sinex-node-sdk` via
`WindowedNodeAdapter` and emits `activity.session.boundary` events containing
session metadata (duration, dominant activity source, contributing sources).

- Subscribes to all event types (`*`) but only accumulates activity-bearing
  signals: Hyprland focus changes, terminal command execution, and browser
  activity events.
- Tracks current session state: start time, event count, unique raw sources, and
  logical activity-source counts (`window`, `terminal`, `browser`).
- Emits a boundary event when a gap exceeding the threshold is detected.
- Uses `SyntheticTemporalPolicy::WindowBoundary` for replay-correct timestamps.

Reference `README.md#deployment--operations` for the operator path and
`crate/lib/sinex-node-sdk/docs/overview.md` for the shared node architecture.
