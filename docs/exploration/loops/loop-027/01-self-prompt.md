Self-prompt: Observability coverage audit

Goal
- Map current logging and self-observation telemetry coverage across ingestd, gateway, and node SDK.
- Identify which telemetry APIs are defined vs actually used, and where metrics are missing or low fidelity.

Process
1) Find self-observation emitters and call sites (SelfObserver, GatewayMetrics, ingestd stats, blob manager, heartbeat).
2) For each subsystem, capture what is logged vs emitted as events, and how data is transported (NATS telemetry vs journald log ingestion).
3) Cross-check self-observation aggregates/migrations vs actual event emitters.
4) Note missing metrics (latency, queue depth) or placeholders (0 values).
5) Produce actionable issues focused on coverage gaps or misleading metrics.

Output
- 02-analysis.md: concrete map with file references.
- 03-meta-reflection.md: limitations and missing edges.
- 04-issues.md: specific improvements.
- 05-next-brainstorm.md: next analysis idea.
