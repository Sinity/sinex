# Distributed Coordination

## Overview

The `sinex-node-sdk::coordination` module implements high-level distributed patterns on top of the `sinex-core` primitives. It handles:
1.  **Leadership Election**: Ensuring only one "Leader" instance runs for a service.
2.  **Graceful Handoff**: Coordinating zero-downtime upgrades between old and new versions.
3.  **Work Tracking**: Ensuring critical operations complete before shutdown.

## Concurrency Model & Lock Ordering

This module uses a mix of `RwLock` and atomic primitives. To prevent deadlocks, a strict lock hierarchy is enforced:

1.  **`work_tracker: RwLock<WorkTracker>`**: Top-level lock. Acquire BEFORE accessing internal tracker state.
2.  **Internal Atomics**: `CoordinationPrimitive` uses atomics internally (lock-free).

**Deadlock Prevention Rules**:
*   Never hold a read lock while waiting for a write lock (upgrade deadlock).
*   Release locks before performing I/O.

## Handoff Protocol

When a new version of a service starts:
1.  **Detection**: It lists instances in NATS KV to find older versions.
2.  **Request**: It publishes a `HandoffRequest` to `sinex.coordination.<service>.handoff`.
3.  **Drain**: The old leader receives the request, stops accepting new work, and waits for in-flight ops to zero out.
4.  **Signal**: The old leader publishes to `handoff_ready`.
5.  **Release**: The old leader releases its NATS KV leadership lease.
6.  **Takeover**: The new leader acquires the lease and begins processing.

## Error Recovery

*   **Lease Expiry**: If a leader crashes, its NATS KV key TTL expires (15s), allowing a standby to take over.
*   **Critical Failure**: Leaders can broadcast a "critical failure" signal to trigger immediate takeover.