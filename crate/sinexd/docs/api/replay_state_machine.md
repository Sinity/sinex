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
- RuntimeModule tracking identifies which executor is running operations.
- Approval workflow ensures human oversight of destructive operations.

## Error Handling and Recovery

- Failed operations can restart from the Planning state.
- Checkpoints capture savepoint data for rollback.
- Detailed error logging supports troubleshooting.
- Operations can be cancelled at any non-terminal state.

### Archive-before-reemit recovery

Replay archives the affected event cascade before asking the source runtime to
re-emit current interpretations. The archive transaction records the cascade
IDs in the operation metadata, so recovery can restore the originals if the
daemon stops before re-emission completes.

On every daemon startup, `ServiceContainer` scans all replay operations still
in an executing, cancelling, or committing state; it does not wait for a
staleness window. This matches the deployed systemd restart path, where the
daemon may restart seconds after a crash. Recovery restores archived rows before
marking the operation failed and is safe to repeat.

Any clean failure after the archive commit runs the same compensation sequence:
link visible replacement events, restore archived rows whose occurrences have
no live replacement, and publish scope invalidations. If any compensation step
fails, the operation is marked failed with an explicit operator-recovery
requirement instead of silently reporting a normal execution failure.
