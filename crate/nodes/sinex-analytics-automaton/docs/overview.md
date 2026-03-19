# Analytics Automaton Overview

This automaton consumes raw event feeds and derives analytic insights. It
implements the shared `AutomatonNode` lifecycle and maintains its own
checkpoint state so replays and continuous streaming share the same code paths.

Key responsibilities:

- Sampling and aggregating event activity across sources.
- Emitting derived events that power the operator-facing summaries surfaced through
  the deployment path in `README.md#deployment--operations`.
- Coordinating with `sinex-node-sdk` primitives for health checks,
  replays, and graceful shutdown.
