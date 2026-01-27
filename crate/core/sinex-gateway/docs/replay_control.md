# Replay Control

Distributed replay orchestration via NATS RPC.

## ⚠️ Current Status

**Note: This subsystem is currently in active development.**
- The **Execution** phase is currently a simulation stub (fast-forwards state without re-emitting events).
- There is an architectural disconnect between this gateway-driven control plane and the `sinex-node-sdk` replay service (see architectural issue INS-027).
- Authorization checks are currently disabled (TODOs in code).

## Architecture

The replay control system orchestrates distributed replay operations across the cluster.
It sits atop the core `ReplayStateMachine` and provides a NATS-based RPC interface for:

- **Planning**: Creating new replay operations with a specific scope.
- **Previewing**: Analyzing the impact of a replay (cascades, event counts).
- **Approving**: Moving an operation from `Previewed` to `Approved`.
- **Executing**: Triggering execution on a specific node.
- **Cancelling**: Aborting a running or planned operation.

## Execution Flow

1.  **Plan**: Client sends `plan(actor, scope)`. State machine creates a `Planning` operation.
2.  **Preview**: Client requests preview. System runs `CascadeAnalyzer` and updates state to `Previewed`.
3.  **Approve**: User (or automated policy) approves the operation. State moves to `Approved`.
4.  **Execute**: Execution node (or gateway) triggers execution. State moves to `Executing`.
    -   Acquires distributed lock (advisory lock) to prevent concurrent execution.
    -   Updates progress checkpoints.
    -   On completion, state moves to `Completed`.

## State Machine

The replay lifecycle follows a strict state machine:

- `Planning` → `Previewed` | `Cancelled`
- `Previewed` → `Approved` | `Cancelled` | `Planning` (re-plan)
- `Approved` → `Executing` | `Cancelled`
- `Executing` → `Committing` | `Failed` | `Cancelled`
- `Committing` → `Completed` | `Failed`

Transitions are enforced by the database-backed `ReplayStateMachine` in `sinex-core`.

## Security Considerations

- **Authorization**: Operations should be restricted to authorized actors.
- **Scope Injection**: Replay scopes can be broad; validation ensures time windows and filters are reasonable.
- **Locking**: Distributed execution locks prevent multiple nodes from running the same replay simultaneously.

## Telemetry

The system runs a background telemetry task that samples active operations and reports metrics (e.g., number of active replays, state distribution) for monitoring.
