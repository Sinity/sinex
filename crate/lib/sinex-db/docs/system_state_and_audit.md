# System State and Audit Trail

The `StateRepository` manages high-level system observability, including the audit trail of operations and the registry of active nodes.

## Operations Log

The operations log is the primary audit trail for all significant system activities, such as replays, entity merges, and administrative maintenance.

- **Operation Lifecycle**: Operations transition through `running`, `success`, `failure`, or `partial` states.
- **Scope Tracking**: Each log entry contains a JSON `scope` describing the target of the operation (e.g., a specific time range or event ID).
- **Preview Summary**: For complex operations like replays, the log stores metadata about intended changes, allowing for safe dry-runs and operator approval.

## Node Manifests

The node manifest serves as a registry for all nodes (ingestors and automata) that have participated in the cluster.

- **Identification**: Each entry records the `node_name`, its `version`, and its `node_type`.
- **Manifest vs. Status**: While the manifest records that a node *exists*, its current runtime health is tracked separately via heartbeats.

## Health Monitoring

The system determines node health by analyzing `process.heartbeat` events emitted by nodes.

- **Active Count**: Nodes that have emitted a heartbeat within the staleness threshold (default 120s).
- **Inactive Count**: Registered nodes that have either never sent a heartbeat or have exceeded the staleness threshold.
- **Diagnostics**: The repository provides comprehensive capability checks (UUID/ULID generation, extension presence) to verify the underlying database environment.

## Relationship to Checkpoints

**Important**: This repository does *not* manage individual processing checkpoints (e.g., "how far has this node read in the stream"). Checkpoint state is stored in NATS Key-Value buckets for better throughput and lower database contention.
