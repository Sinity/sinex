Self-prompt: Error origin tracing

Goal
- Map where SinexError variants are constructed, and how they propagate to user-facing surfaces (logs, CLI exit, RPC responses).
- Identify gaps where errors are created but never surfaced or lose context.

Process
1) Locate the SinexError definition and helper constructors to understand context/sources and formatting.
2) Find top-level entrypoints (ingestd, gateway) and identify the terminal error handlers (logging, exit).
3) Trace key error flows from origin to handler in ingestd and gateway (config, database, NATS, schema sync, runtime tasks).
4) Check conversion layers (NodeError <-> SinexError, sqlx -> SinexError) for loss of context.
5) Inventory unused SinexError variants by searching for constructors outside error.rs.
6) Summarize confirmed flows, and list actionable issues where context is dropped or errors are only logged without escalation.

Output
- 02-analysis.md: concrete flow map with file references and evidence.
- 03-meta-reflection.md: limits and missing edges.
- 04-issues.md: specific fixes.
- 05-next-brainstorm.md: next analysis idea.
