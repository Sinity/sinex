# Analytics Automaton Overview

This automaton consumes raw event feeds and derives analytic insights. It
implements the shared `StatefulStreamProcessor` lifecycle and maintains its own
checkpoint state so replays and continuous streaming share the same code paths.

Key responsibilities:

- Sampling and aggregating event activity across sources.
- Emitting derived events that power the operational dashboards described in
  `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`.
- Coordinating with `sinex-satellite-sdk` primitives for health checks,
  replays, and graceful shutdown.
