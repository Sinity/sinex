# Loop 014 - Next Analysis Ideas

- Inspect `governor` decision types to see if wait/limit metadata can be captured.
- Add an internal counter for rate-limit denials and compare against emitted events.
- Trace all telemetry emission points and confirm per-component rate-limiting policies.
- Review `SelfObserver` rate limiter to see if per-event buckets should be added.
