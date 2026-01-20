Self-prompt: Channel topology and backpressure audit

Goal
- Map the primary async channel topology (mpsc/oneshot/watch/broadcast) in production code.
- Identify backpressure behavior, drop strategies, and shutdown semantics per channel.

Process
1) Use ripgrep to locate channel creation sites (tokio::sync::mpsc, oneshot, watch, std::sync::mpsc).
2) Focus on production code paths (skip tests except when they document intended behavior).
3) For each channel, capture:
   - Location and purpose (producer/consumer roles).
   - Capacity and whether it is bounded.
   - Send strategy (send/try_send/blocking_send) and what happens on full or closed.
   - Shutdown semantics (watch/oneshot used to signal termination).
4) Summarize per subsystem (node runtime, ingestors, gateway, core utilities).
5) Identify any mismatches between comments and actual behavior or missing backpressure handling.

Output
- 02-analysis.md: topology map with file references and concrete notes.
- 03-meta-reflection.md: limitations and missing edges.
- 04-issues.md: actionable items (backpressure bugs, missing metrics, etc.).
- 05-next-brainstorm.md: next analysis idea.
