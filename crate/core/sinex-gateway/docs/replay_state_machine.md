# Replay State Machine

Distributed replay operation tracking that enables pause/resume, collaborative approval, and
failure recovery.

## State Machine Overview

Replay operations move through these states:

- **Planning** – gather scope and plan the operation.
- **Previewed** – preview computed, awaiting authorized approval.
- **Approved** – ready for execution.
- **Executing** – replay running with checkpoint tracking.
- **Committing** – finalising changes and cleanup.
- **Completed** – successful finish.
- **Failed** – execution error.
- **Cancelled** – user-aborted operation.

## State Transitions

Valid transitions keep operations safe:

```text
Planning → Previewed → Approved → Executing → Committing → Completed
    ↓          ↓         ↓          ↓            ↓
Cancelled  Cancelled  Cancelled   Failed      Failed
    ↓          ↓
Planning   Planning
```

## Distributed Coordination

- PostgreSQL advisory locks prevent concurrent execution conflicts.
- Checkpoints enable pause/resume functionality.
- Node tracking identifies which executor is running operations.
- Approval workflow ensures human oversight of destructive operations.

## Error Handling and Recovery

- Failed operations can restart from the Planning state.
- Checkpoints capture savepoint data for rollback.
- Detailed error logging supports troubleshooting.
- Operations can be cancelled at any non-terminal state.
