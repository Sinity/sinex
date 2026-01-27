# Distributed Coordination & Leadership

The `coordination` module implements high-level distributed patterns required for zero-downtime operations and singleton service enforcement.

## 🏆 Leadership Election

Sinex uses a **Single Leader** model for stateful automata to prevent conflicting state updates.
- **Lease Persistence**: Leadership is maintained via NATS KV keys with a 15-second TTL.
- **Heartbeat**: Leaders must refresh their lease every 5 seconds.
- **Standby Mode**: Non-leader instances run in standby mode, monitoring the lease for takeover opportunities if the leader's TTL expires.

## 🔄 Zero-Downtime Handoff

When a newer version of a service starts, it initiates a graceful handoff protocol:

1.  **Detection**: The new instance lists instances in `KV_sinex_instances` to find older versions.
2.  **Handoff Request**: New instance publishes to `sinex.coordination.<service>.handoff`.
3.  **Drain**: The old leader stops accepting new work and waits for in-flight operations to zero out (max 30s).
4.  **Signal Ready**: Old leader publishes to `handoff_ready` and releases its NATS KV lease.
5.  **Takeover**: New instance immediately acquires the lease and begins processing.

## 🔒 Lock Ordering & Deadlock Prevention

This module uses `RwLock` for work tracking. To prevent deadlocks, follow these rules:

1.  **Hierarchy**: Always acquire the `work_tracker` lock *before* accessing internal state.
2.  **No Upgrades**: Never hold a read lock while waiting for a write lock (classic upgrade deadlock).
3.  **No I/O**: Release all locks before performing NATS or Database I/O.
4.  **Instrumentation**: Lock acquisitions exceeding 10ms are logged as warnings.

## 🚨 Error Recovery

### Leader Crash
If a leader crashes without releasing its lease, the NATS KV TTL will expire after 15 seconds. A standby instance will then automatically acquire the lease.

### Critical Failure
Leaders can broadcast a **Critical Failure Signal** (`sinex.coordination.<service>.failure`) to trigger an immediate takeover by standby instances, bypassing the normal TTL wait.

### Clock Skew
Coordination primitives are sensitive to clock skew. Nodes should use `tokio::time::Instant` for internal timeouts and rely on NATS server-side timestamps for cross-node coordination where possible.
