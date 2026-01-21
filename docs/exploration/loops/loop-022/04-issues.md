# Loop 022 - Concrete Issues

1) Registry includes legacy schemas with no emitters.
- `journald/satellite.heartbeat` and `system/*_historical` schemas exist but no code emits them.
- This may indicate stale schemas that should be documented or pruned.
