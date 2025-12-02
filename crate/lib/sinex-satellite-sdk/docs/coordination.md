# Coordination Error Recovery

Coordination helpers for leader election, handoff, and failure recovery.

The module wraps advisory-lock arbitration, version-aware priority, optional
preflight gating, and the work-tracking needed to leave leadership cleanly.
Refer to `crate/lib/sinex-satellite-sdk/docs/overview.md` for the lifecycle
narrative and timing diagrams that inform this implementation.
- **Timeout Handling**: Force shutdown if graceful completion takes too long
- **Heartbeat Integration**: Report work status via heartbeat metrics

## Error Recovery Patterns

### Heartbeat Timeout Recovery
```rust
// Standby instances detect leader failure
if leader_heartbeat_age > 30_seconds {
attempt_leadership_takeover();
}
```

### Critical Failure Recovery
```rust
// Leader detects critical error
coordination.signal_critical_failure("Database connection lost").await?;
// Standby instances receive signal and attempt takeover
```

### Version Upgrade Handoff
```rust
// New version starts and requests handoff
let handoff_request = HandoffRequest {
from_instance: new_instance_id,
to_version: SatelliteVersion::current(),
timeout_seconds: 30,
};
send_handoff_request(handoff_request).await?;
```
