# Loop 011 - Self-Observation Emission Volume vs Rate Limiting

Goal
- Determine whether self-observation telemetry can starve or drop higher-level metrics due to a shared rate limiter.
- Identify high-frequency emission paths that might suppress periodic aggregates or overwhelm NATS.

Process
1) Use cclsp to locate `SelfObserver` and its call sites.
2) Read `self_observation.rs` to understand rate limiting and emission mechanics.
3) Trace hot-path emitters (gateway, ingestd) to see how often they emit and if they share the same observer instance.
4) Check for background emitters that rely on periodic intervals (10s/60s) and whether per-request emissions can suppress them.
5) Note any missing fields or placeholders in emitted payloads that reduce observability.

Deliverables
- Map of emission paths and cadence.
- Findings on shared rate limiter behavior.
- Concrete issues if fields are stubbed or data loss is likely.
- Next analysis candidates.
