# Distributed Coordination

Sinex utilizes NATS Key-Value (KV) buckets to manage cluster-wide coordination without a central management service. This ensures high availability and decentralized decision-making.

## Core Primitives

The coordination layer provides three primary distributed primitives:

### 1. Instance Registration
Nodes register themselves in the `KV_sinex_instances` bucket upon startup.
- **Key Format**: `{service_name}.{instance_id}`
- **Metadata**: Includes hostname, PID, version, and capabilities.
- **Lifecycle**: Managed via heartbeats.

### 2. Distributed Heartbeats
Nodes must periodically update their registration key to signal health.
- **Mechanism**: Atomic `PUT` operations with revision checks.
- **Staleness**: Other nodes detect failures when an instance's heartbeat exceeds the configured threshold.

### 3. Leader Election
Critical singleton tasks (like schema synchronization or scheduled maintenance) use the `CoordinationKvClient` for leader election.
- **Strategy**: Optimistic locking via NATS KV CAS (Compare-And-Swap) semantics.
- **Ownership**: Leadership is acquired by creating/updating a key with a unique candidate ID and a short TTL.

## CAS Semantics & Safety

Coordination operations rely on NATS revision numbers to prevent race conditions:

- **Updates**: Use `update(key, value, current_revision)` to ensure no concurrent modifications occurred.
- **Initial Writes**: Use `update(key, value, 0)` to ensure the key does not already exist.
- **Graceful Handoff**: Leaders should release leadership by deleting their specific revision, allowing other candidates to contend immediately.

## Implementation Details

The underlying implementation is found in `sinex-core::coordination::kv_client`. It is designed to be:
- **Low Overhead**: Uses efficient JetStream binary protocols.
- **Fail-Safe**: Relies on NATS-enforced TTLs for automatic cleanup of crashed instances.
