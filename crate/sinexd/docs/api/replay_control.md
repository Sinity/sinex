# Replay Control

Replay orchestration via `sinexd` RPC plus source-runtime scan commands.

## Current Status

Replay control is operational:
- **Execution is real**: approved operations archive the affected cascade, then dispatch typed historical scans to the target source runtime.
- **Actor validation is enforced**: actor/approver/executor identities must use approved prefixes (`system:`, `service:`, `user:`, etc.).
- **State transitions are enforced** by the database-backed replay state machine.

## Architecture

The replay control system orchestrates replay operations inside the `sinexd` runtime plane.
It sits atop the core `ReplayStateMachine` and provides a NATS-based RPC interface for:

- **Planning**: Creating new replay operations with a specific scope.
- **Previewing**: Analyzing the impact of a replay (cascades, event counts).
- **Approving**: Moving an operation from `Previewed` to `Approved`.
- **Executing**: Triggering execution through a specific runtime identity.
- **Cancelling**: Aborting a running or planned operation.

## Execution Flow

1.  **Plan**: Client sends `plan(actor, scope)`. State machine creates a `Planning` operation.
2.  **Preview**: Client requests preview. System runs `CascadeAnalyzer` and updates state to `Previewed`.
3.  **Approve**: User (or automated policy) approves the operation. State moves to `Approved`.
4.  **Execute**: An authenticated runtime identity triggers execution. State moves to `Executing`.
    -   Acquires distributed lock (advisory lock) to prevent concurrent execution.
    -   Expands cascade from selected material-root events and archives affected live rows.
    -   Dispatches a `SourceScanCommand` to the running source runtime over NATS request/reply.
    -   The source runtime re-reads source material through its historical scan ingress and emits fresh events through the normal pipeline.
    -   Updates progress checkpoints.
    -   On completion, state moves to `Completed`.

## State Machine

The replay lifecycle follows a strict state machine:

- `Planning` → `Previewed` | `Cancelled`
- `Previewed` → `Approved` | `Cancelled` | `Planning` (re-plan)
- `Approved` → `Executing` | `Cancelled`
- `Executing` → `Committing` | `Failed` | `Cancelled`
- `Committing` → `Completed` | `Failed`

Transitions are enforced by the database-backed `ReplayStateMachine` in `sinex-db` (re-exported by gateway).

## Security Considerations

- **Authorization**: Replay RPC methods enforce RBAC via gateway registry; actor identifiers are validated by replay control.
- **Scope Injection**: Replay scopes can be broad; validation ensures time windows and filters are reasonable.
- **Locking**: Execution locks prevent multiple runtime identities from running the same replay simultaneously.

## Telemetry

The system runs a background telemetry task that samples active operations and reports metrics (e.g., number of active replays, state distribution) for monitoring.
